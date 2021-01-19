"""Functions to interact with Criteo Marketing API."""
import time

import criteo_marketing
from criteo_marketing.rest import ApiException
import singer
from tap_criteo.marketing_client import (
    get_oauth_token,
    mc_get_audiences,
    mc_sync,
    mc_sync_statistics
)

LOGGER = singer.get_logger()

GRANT_TYPE = "client_credentials"  # Other grant types are not available
TIMEZONE = (
    "GMT"  # Hardcoded to GMT since all values will be converted to UTC anyway
)
FORMAT = (
    "Csv"  # Hardcoded to Csv since all records converted to Singer spec JSON
)
TOKEN_EXPIRE = (
    0  # Unix timestamp at which the current auth token expires (seconds)
)
TOKEN_REFRESH_MARGIN = (
    60  # Maximum number of seconds before token expiry to get new token
)


# Return unix timestamp to the nearest second
def get_unixtime():
    """Get UNIX timestamp for time now."""
    return int(time.time())


def create_sdk_client(config):
    """Create an interface to Criteo Marketing API."""
    LOGGER.info("Creating Criteo Marketing API client with OAuth credentials")
    configuration = criteo_marketing.Configuration(
        username=config["client_id"], password=config["client_secret"]
    )

    # Enable/Disable debug httplib and criteo_marketing packages
    # logging.basicConfig(level=logging.DEBUG)
    # configuration.debug = True

    return criteo_marketing.ApiClient(configuration)


def get_auth_token(client):
    """Authenticate with Criteo Marketing API and return acccess token."""
    LOGGER.info("Getting OAuth token")

    with singer.metrics.http_request_timer("Authentication"):
        auth_response = get_oauth_token(
            client_id=client.configuration.username,
            client_secret=client.configuration.password,
        )
    global TOKEN_EXPIRE

    TOKEN_EXPIRE = get_unixtime() + auth_response['expires_in']
    # Token type is always "BEARER"

    return auth_response['token_type'] + " " + auth_response['access_token']


def refresh_auth_token(client, token):
    """Refresh Criteo Marketing API access token if it expires soon."""
    time_to_expire = TOKEN_EXPIRE - TOKEN_REFRESH_MARGIN
    if time_to_expire <= get_unixtime():
        # Don't log time to expire on if no token provided
        if token:
            LOGGER.info("Token expires in %d" % time_to_expire)
        return get_auth_token(client)

    return token


def exception_is_4xx(exception):
    """Return True if exception is in the 4xx range."""
    if not hasattr(exception, "status"):
        return False

    return 400 <= exception.status < 500


@singer.utils.backoff((ApiException,), exception_is_4xx)
def get_statistics_report(client, stats_query, token=None):
    """Get Statistics Report from Criteo Marketing API endpoint."""

    token = token or get_auth_token(client)
    result = mc_sync_statistics(token, stats_query)
    return result
    token = token or get_auth_token(client)
    defaults = {"format": FORMAT, "timezone": TIMEZONE}
    stats_query.update(defaults)
    stats_api = criteo_marketing.StatisticsApi(client)
    stats_query_message = criteo_marketing.StatsQueryMessageEx(**stats_query)

    return stats_api.get_stats(token, stats_query_message)


@singer.utils.backoff((ApiException,), exception_is_4xx)
def get_audiences_endpoint(client, advertiser_id, token=None):
    """Get Audiences for an Advertiser from Criteo Marketing API."""
    token = token or get_auth_token(client)
    return mc_get_audiences(token, advertiser_id)


@singer.utils.backoff((ApiException,), exception_is_4xx)
def get_generic_endpoint(
    client, module, method, advertiser_ids=None, token=None
):
    """Get objects from Criteo Marketing API for a selection of Avertisers."""
    return mc_sync(module + '.' + method, token, advertiser_ids)

    token = token or get_auth_token(client)
    api_instance = getattr(criteo_marketing, module)(client)
    if advertiser_ids:
        return getattr(api_instance, method)(
            token, advertiser_ids=advertiser_ids
        )
    return getattr(api_instance, method)(token)
