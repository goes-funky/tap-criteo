"""Logic to discover tap."""
import os

import singer
from singer import metadata
from singer import utils
from tap_criteo.endpoints import (
    GENERIC_ENDPOINT_MAPPINGS,
    STATISTICS_REPORT_TYPES,
)


LOGGER = singer.get_logger()


def get_abs_path(path):
    """Get absolute filepath from relative filepath."""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(entity):
    """Load Singer Schema from JSON file in schemas directory."""
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))


def load_metadata(entity):
    """Load Singer Metadata from JSON file in schemas directory."""
    return utils.load_json(get_abs_path("metadata/{}.json".format(entity)))


def do_discover():
    """Generate Singer Catalog for tap."""
    streams = []

    # Load generic endpoints
    for stream_name in GENERIC_ENDPOINT_MAPPINGS:
        LOGGER.info("Loading schema for %s", stream_name)
        schema = load_schema(stream_name)
        LOGGER.info("Loading metadata for %s", stream_name)
        mdata = load_metadata(stream_name)
        LOGGER.info("Adding stream for %s", stream_name)
        streams.append(
            {
                "stream": stream_name,
                "tap_stream_id": stream_name,
                "schema": schema,
                "metadata": mdata,
                "key_properties": [],
            }
        )

    for report_name in STATISTICS_REPORT_TYPES:
        LOGGER.info("Loading schema for %s", report_name)
        schema = load_schema("Statistics")
        LOGGER.info("Loading metdata for %s", report_name)
        mdata = load_metadata("Statistics")
        if report_name == "TransactionID":
            # Explicitly set to null for TransactionID ReportType to get
            # all data but no way to add null to metadata using Singer
            # helper function
            mdata = metadata.to_map(mdata)
            mdata.get(()).update({"tap-criteo.ignoreXDevice": None})
            mdata = metadata.to_list(mdata)
        LOGGER.info("Adding stream for %s", report_name)
        streams.append(
            {
                "stream": report_name,
                "tap_stream_id": report_name,
                "schema": schema,
                "metadata": mdata,
            }
        )

    return {"streams": streams}
