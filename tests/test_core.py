"""Tests standard tap features using the built-in SDK tests library."""

import datetime

import pytest
from cassandra.cluster import Cluster
from singer_sdk.testing import get_tap_test_class

from tap_cassandra.tap import TapCassandra


SAMPLE_CONFIG = {
    "hosts": "127.0.0.1",
    "username": "cassandra",
    "password": "cassandra",
    "keyspace": "test_schema"
}

# Run standard built-in tap tests from the SDK:
PreTestTapCassandra = get_tap_test_class(
    tap_class=TapCassandra,
    config=SAMPLE_CONFIG,
    catalog="tests/resources/catalog.json",
)

class TestTapCassandra(PreTestTapCassandra):
    keyspace_name = 'test_schema'
    table_name = 'test_table'

    def _setup(self):
        cluster = Cluster()
        session = cluster.connect()
        
        create_keyspace_command = f"""
        CREATE KEYSPACE IF NOT EXISTS {self.keyspace_name}
        WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }};
        """
        session.execute(create_keyspace_command)
        session.set_keyspace(self.keyspace_name)

        create_table_command = f"""
        CREATE TABLE IF NOT EXISTS {self.keyspace_name}.{self.table_name}
        (id int, updated_at timestamp, name text, primary key (id));
        """
        session.execute(create_table_command)

        insert_command = f"""
        INSERT INTO {self.keyspace_name}.{self.table_name} (id, updated_at, name) values (1, dateof(now()), 'test1');
        """
        session.execute(insert_command)

    @pytest.fixture(scope="class")
    def resource(self):
        self._setup()