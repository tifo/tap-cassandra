"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_tap_test_class

from tap_cassandra.tap import TapCassandra

SAMPLE_CONFIG = {
    "hosts": ["10.0.0.1", "10.0.0.2"],
    "port": 9042,
    "keyspace": "my_keyspace",
    "username": "test",
    "password": "test",
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "request_timeout": 60,
    "local_dc": "my_dc",
    "reconnect_delay": 60,
    "max_attempts": 5,
    "protocol_version": 65,
}

# TODO: Create additional tests
# Create a docker image with local cassandra so we can test TapCassandra.
# Run standard built-in tap tests from the SDK:
# TestTapCassandra = get_tap_test_class(
#     tap_class=TapCassandra,
#     config=SAMPLE_CONFIG,
# )
