"""Singer Tap to pull data from Criteo Marketing API."""
import json

import singer
from singer import utils
from tap_criteo.discover import do_discover
from tap_criteo.sync import do_sync


REQUIRED_CONFIG_KEYS = ["start_date", "client_id", "client_secret"]

LOGGER = singer.get_logger()


@utils.handle_top_exception(LOGGER)
def main():
    """CLI for Singer Tap."""
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = do_discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    elif args.catalog:
        do_sync(args.config, args.state, args.catalog)


if __name__ == "__main__":
    main()
