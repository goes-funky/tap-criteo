"""Logic to sync tap."""
import copy
import csv
import io

from dateutil.relativedelta import relativedelta
import singer
from singer import bookmarks
from singer import metadata
from singer import metrics
from singer import Schema
from singer import Transformer
from singer import utils
from tap_criteo.criteo import (
    create_sdk_client,
    get_audiences_endpoint,
    get_generic_endpoint,
    get_statistics_report,
    refresh_auth_token,
)
from tap_criteo.endpoints import (
    GENERIC_ENDPOINT_MAPPINGS,
    SELLER_STATS_REPORT_TYPES,
    STATISTICS_REPORT_TYPES,
)


CSV_DELIMITER = ";"
LOGGER = singer.get_logger()

REPORT_RUN_DATETIME = utils.strftime(utils.now())


def get_attribution_window_bookmark(state, advertiser_ids, stream_name):
    """Get attribution window for stream from Singer State."""
    mid_bk_value = bookmarks.get_bookmark(
        state,
        state_key_name(advertiser_ids, stream_name),
        "last_attribution_window_date",
    )
    return utils.strptime_with_tz(mid_bk_value) if mid_bk_value else None


def get_start_for_stream(config, state, advertiser_ids, stream_name):
    """Get start date for stream sync."""
    bk_value = bookmarks.get_bookmark(
        state, state_key_name(advertiser_ids, stream_name), "date"
    )
    bk_start_date = utils.strptime_with_tz(bk_value or config["start_date"])
    return bk_start_date


def apply_conversion_window(config, start_date):
    """Adjust start date based on conversion window."""
    conversion_window_days = int(config.get("conversion_window_days", "-30"))
    return start_date + relativedelta(days=conversion_window_days)


def get_end_date(config):
    """Get end date from config file."""
    if config.get("end_date"):
        return utils.strptime_with_tz(config.get("end_date"))

    return utils.now()


def state_key_name(advertiser_ids, stream_name):
    """Generate Singer State key for stream."""
    if advertiser_ids:
        return stream_name + "_" + advertiser_ids
    else:
        return stream_name


def should_sync(mdata, field):
    """Return True if field should be synced."""
    inclusion = metadata.get(mdata, field, "inclusion")
    selected = metadata.get(mdata, field, "selected")
    return utils.should_sync_field(inclusion, selected)


def get_fields_to_sync(stream):
    """Return fields of stream which should be synced."""
    fields = (
        stream.schema.properties
    )  # pylint: disable=unsubscriptable-object
    mdata = metadata.to_map(stream.metadata)
    return [
        field for field in fields if should_sync(mdata, ("properties", field))
    ]


def get_field_list(stream):
    """Return fields of stream which should be synced with synthetic keys."""
    stream = add_synthetic_keys_to_stream_metadata(stream)
    field_list = get_fields_to_sync(stream)
    LOGGER.info("Request fields: %s", field_list)
    return field_list


def add_synthetic_keys_to_stream_schema(stream):
    """Add synthetic keys to stream's schema."""
    stream.schema.properties["_sdc_report_datetime"] = Schema.from_dict(
        {
            "description": "DateTime of Report Run",
            "type": "string",
            "format": "date-time",
        }
    )
    if stream.tap_stream_id in STATISTICS_REPORT_TYPES:
        stream.schema.properties["_sdc_report_currency"] = Schema.from_dict(
            {
                "description": "Currency of all costs in report",
                "type": "string",
            }
        )
        stream.schema.properties[
            "_sdc_report_ignore_x_device"
        ] = Schema.from_dict(
            {
                "description": "Ignore cross-device data. Also can explicitly "
                + "set to null for TransactionID ReportType to get all data.",
                "type": "boolean",
            }
        )
    return stream


def add_synthetic_keys_to_stream_metadata(stream):
    """Add synthetic keys to stream's metadata."""
    stream.metadata.append(
        {
            "metadata": {"inclusion": "automatic"},
            "breadcrumb": ["properties", "_sdc_report_datetime"],
        }
    )
    if stream.tap_stream_id in STATISTICS_REPORT_TYPES:
        stream.metadata.append(
            {
                "metadata": {"inclusion": "automatic"},
                "breadcrumb": ["properties", "_sdc_report_currency"],
            }
        )
        stream.metadata.append(
            {
                "metadata": {"inclusion": "automatic"},
                "breadcrumb": ["properties", "_sdc_report_ignore_x_device"],
            }
        )
    return stream


def parse_csv_string(mdata, csv_string):
    """Parse CSV string into iterable of dictionaries."""
    # Remove BOM
    csv_string = csv_string.lstrip("\ufeff")
    # Read a single line into a String, and parse the headers as a CSV
    headers = csv.reader(io.StringIO(csv_string), delimiter=CSV_DELIMITER)
    # Convert headers to match Schema from metadata
    header_mapping = {v.get("tap-criteo.col-name"): k for k, v in mdata.items()}
    header_array = list(headers)[0]
    header_array = [header_mapping[header][1] for header in header_array]

    # Create another CSV reader for the rest of the data
    csv_reader = csv.DictReader(
        io.StringIO(csv_string),
        fieldnames=header_array,
        delimiter=CSV_DELIMITER,
    )
    next(csv_reader, None)  # Skip header row
    return csv_reader


def sync_statistics_report(config, state, stream, sdk_client, token):
    """Sync a stream which is backed by the Criteo Statistics endpoint."""
    advertiser_ids = config.get("advertiser_ids", "")
    mdata = metadata.to_map(stream.metadata)

    stream = add_synthetic_keys_to_stream_schema(stream)

    field_list = get_field_list(stream)

    primary_keys = []
    LOGGER.info("{} primary keys are {}".format(stream.stream, primary_keys))
    singer.write_schema(
        stream.stream,
        stream.schema.to_dict(),
        primary_keys,
        bookmark_properties=["Day"],
    )

    # If an attribution window sync is interrupted, start where it left off
    start_date = get_attribution_window_bookmark(
        state, advertiser_ids, stream.stream
    )
    if start_date is None:
        start_date = apply_conversion_window(
            config,
            get_start_for_stream(
                config, state, advertiser_ids, stream.stream
            ),
        )

    # According to Criteo's documentation the StatisticsApi only supports
    # between one and three dimensions and at least one metric.
    report_dimensions = [
        field
        for field in field_list
        if metadata.get(mdata, ("properties", field), "tap-criteo.behaviour")
        == "dimension"
    ]
    LOGGER.info("Selected dimensions: %s", report_dimensions)
    if not 0 <= len(report_dimensions) <= 3:
        raise ValueError(
            "%s stream only supports up to 3 selected dimensions"
            % stream.stream
        )
    report_metrics = [
        field
        for field in field_list
        if metadata.get(mdata, ("properties", field), "tap-criteo.behaviour") == "metric"
    ]
    LOGGER.info("Selected metrics: %s", report_metrics)
    if not len(report_metrics) >= 1:
        raise ValueError(
            "%s stream must have at least 1 selected metric" % stream.stream
        )

    while start_date <= get_end_date(config):
        token = refresh_auth_token(sdk_client, token)
        sync_statistics_for_day(
            config,
            state,
            stream,
            sdk_client,
            token,
            start_date,
            report_metrics,
            report_dimensions,
        )
        start_date = start_date + relativedelta(days=1)
        bookmarks.write_bookmark(
            state,
            state_key_name(advertiser_ids, stream.stream),
            "last_attribution_window_date",
            start_date.strftime(utils.DATETIME_FMT),
        )
        singer.write_state(state)
    bookmarks.clear_bookmark(
        state,
        state_key_name(advertiser_ids, stream.stream),
        "last_attribution_window_date",
    )
    singer.write_state(state)
    LOGGER.info(
        "Done syncing the %s report for advertiser_ids %s",
        stream.stream,
        advertiser_ids,
    )


def sync_statistics_for_day(
    config,
    state,
    stream,
    sdk_client,
    token,
    start,
    report_metrics,
    report_dimensions,
):  # pylint: disable=too-many-locals
    """Sync and output Criteo Statistics endpoint for one day."""
    mdata = metadata.to_map(stream.metadata)
    stats_query = {
        "report_type": stream.tap_stream_id,
        "dimensions": report_dimensions,
        "metrics": report_metrics,
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": start.strftime("%Y-%m-%d"),
        "currency": metadata.get(mdata, (), "currency"),
    }
    # Filter advertiser_ids if defined in config
    advertiser_ids = config.get("advertiser_ids")
    if advertiser_ids:
        stats_query["advertiserId"] = advertiser_ids
    # Add ignore_x_device if defined in metadata
    ignore_x_device = metadata.get(mdata, (), "tap-criteo.ignoreXDevice")
    if ignore_x_device:
        stats_query["tap-criteo.ignoreXDevice"] = ignore_x_device

    # Fetch the report as a csv string
    with metrics.http_request_timer(stream.tap_stream_id):
        # TODO implement custom solution
        result = get_statistics_report(sdk_client, stats_query, token=token)

    csv_reader = parse_csv_string(mdata, result)
    with metrics.record_counter(stream.tap_stream_id) as counter:
        time_extracted = utils.now()

        with Transformer() as bumble_bee:
            for row in csv_reader:
                row["_sdc_report_datetime"] = REPORT_RUN_DATETIME
                row["_sdc_report_currency"] = metadata.get(
                    mdata, (), "currency"
                )
                row = bumble_bee.transform(row, stream.schema.to_dict())

                singer.write_record(
                    stream.stream, row, time_extracted=time_extracted
                )
                counter.increment()

        if start > get_start_for_stream(
            config, state, advertiser_ids, stream.stream
        ):
            LOGGER.info(
                "updating bookmark: %s > %s",
                start,
                get_start_for_stream(
                    config, state, advertiser_ids, stream.stream
                ),
            )
            bookmarks.write_bookmark(
                state,
                state_key_name(advertiser_ids, stream.stream),
                "date",
                start.strftime(utils.DATETIME_FMT),
            )
            singer.write_state(state)
        else:
            LOGGER.info(
                "not updating bookmark: %s <= %s",
                start,
                get_start_for_stream(
                    config, state, advertiser_ids, stream.stream
                ),
            )

        LOGGER.info(
            "Done syncing %s records for the %s report for "
            + "advertiser_ids %s on %s",
            counter.value,
            stream.stream,
            advertiser_ids,
            start,
        )


def sync_seller_v2_stats_report(config, state, stream, sdk_client, token):
    """Sync a stream which is backed by the Criteo SellerV2Stats endpoint."""
    pass


def sync_seller_v2_stats_for_day(
    config,
    state,
    stream,
    sdk_client,
    token,
    start,
    metrics,
    report_dimensions,
):
    """Sync and output Criteo SellerV2Stats endpoint for one day."""
    pass


def convert_keys_snake_to_camel(result_array):
    """Convert keys of a dictionaries from snake_case to camelCase."""
    result_copy = copy.copy(result_array)
    result_copy = [
        {
            "".join(x.capitalize() or "_" for x in k.split("_")): v
            for k, v in each.items()
        }
        for each in result_copy
    ]
    return [
        {k[0].lower() + k[1:]: v for k, v in each.items()}
        for each in result_copy
    ]


def call_generic_endpoint(
    stream, sdk_client, module, method, advertiser_ids=None, token=None
):
    """Call a generic Criteo Marketing API endpoint with Singer Metrics."""
    with metrics.http_request_timer(stream.tap_stream_id):
        return get_generic_endpoint(
            sdk_client,
            module,
            method,
            advertiser_ids=advertiser_ids,
            token=token,
        )


def sync_generic_endpoint(config, state, stream, sdk_client, token):
    """Sync a stream which is backed by a generic Criteo endpoint."""
    stream = add_synthetic_keys_to_stream_schema(stream)
    stream = add_synthetic_keys_to_stream_metadata(stream)
    mdata = metadata.to_map(stream.metadata)
    primary_keys = metadata.get(mdata, (), "table-key-properties") or []
    LOGGER.info("{} primary keys are {}".format(stream.stream, primary_keys))
    singer.write_schema(stream.stream, stream.schema.to_dict(), primary_keys)

    advertiser_ids = config.get("advertiser_ids", None)
    if stream.tap_stream_id == "Audiences":
        if not advertiser_ids:
            LOGGER.warn(
                "%s stream needs at least one advertiser_id defined in config"
                % stream.stream
            )
        for advertiser_id in advertiser_ids.split(","):
            token = refresh_auth_token(sdk_client, token)
            with metrics.http_request_timer(stream.tap_stream_id):
                result = get_audiences_endpoint(
                    sdk_client, advertiser_id, token=token
                )
    else:
        module = GENERIC_ENDPOINT_MAPPINGS[stream.tap_stream_id]["module"]
        method = GENERIC_ENDPOINT_MAPPINGS[stream.tap_stream_id]["method"]
        if stream.tap_stream_id in (
            "Portfolio",
            "AdvertiserInfo",
            "Sellers",
            "SellerBudgets",
            "SellerCampaigns",
        ):
            result = call_generic_endpoint(
                stream, sdk_client, module, method, token=token
            )
        else:
            result = call_generic_endpoint(
                stream,
                sdk_client,
                module,
                method,
                advertiser_ids=advertiser_ids,
                token=token,
            )

    result = ([i for i in result])

    with metrics.record_counter(stream.tap_stream_id) as counter:
        time_extracted = utils.now()

        with Transformer() as bumble_bee:
            for row in result:
                row["_sdc_report_datetime"] = REPORT_RUN_DATETIME

                row = bumble_bee.transform(row, stream.schema.to_dict())
                singer.write_record(
                    stream.stream, row, time_extracted=time_extracted
                )
                counter.increment()

    LOGGER.info(
        "Done syncing %s records for the %s report for advertiser_ids %s",
        counter.value,
        stream.stream,
        advertiser_ids,
    )


def sync_stream(config, state, stream, sdk_client):
    """Sync a stream."""
    # This bifurcation is real. Generic Endpoints have entirely different
    # performance characteristics and constraints than the Report
    # Endpoints and thus should be kept separate.
    token = refresh_auth_token(sdk_client, None)
    if stream.tap_stream_id in SELLER_STATS_REPORT_TYPES:
        sync_seller_v2_stats_report(config, state, stream, sdk_client, token)
    elif stream.tap_stream_id in STATISTICS_REPORT_TYPES:
        sync_statistics_report(config, state, stream, sdk_client, token)
    elif stream.tap_stream_id in GENERIC_ENDPOINT_MAPPINGS:
        sync_generic_endpoint(config, state, stream, sdk_client, token)
    else:
        raise Exception(
            "Unrecognized tap_stream_id {}".format(stream.tap_stream_id)
        )


def do_sync(config, state, catalog):
    """Sync all streams in Catalog based on State and Config."""
    sdk_client = create_sdk_client(config)
    selected_streams = False
    # Loop over streams in catalog
    advertiser_ids = config.get("advertiser_ids", "").split(",")
    if advertiser_ids:
        LOGGER.info("Syncing advertiser IDs %s ...", advertiser_ids)
    else:
        LOGGER.info("Syncing all advertiser IDs ...")

    for stream in catalog.get_selected_streams(state):
        selected_streams = True
        LOGGER.info("Syncing stream: %s", stream.stream)

        sync_stream(config, state, stream, sdk_client)

    if not selected_streams:
        LOGGER.warn("No streams selected")

    if advertiser_ids:
        LOGGER.info("Done syncing advertiser IDs %s ...", advertiser_ids)
    else:
        LOGGER.info("Done syncing all advertiser IDs ...")

    return
