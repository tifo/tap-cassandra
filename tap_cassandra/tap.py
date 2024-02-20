"""Cassandra tap class."""

from singer_sdk import SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_cassandra.streams import CassandraStream
from tap_cassandra.client import CassandraConnector


class TapCassandra(SQLTap):
    """Cassandra tap class."""

    name = "tap-cassandra"
    _connector = None

    config_jsonschema = th.PropertiesList(
        th.Property(
            "hosts",
            th.StringType,
            required=True,
            description="The list of contact points to try connecting for cluster discovery.",
        ),
        th.Property(
            "port",
            th.IntegerType,
            required=False,
            default=9042,
            description="The server-side port to open connections to. Defaults to 9042.",
        ),
        th.Property(
            "keyspace",
            th.StringType,
            required=False,
            description="Keyspace will be the default keyspace for operations on the Session.",
        ),
        th.Property(
            "username",
            th.StringType,
            required=True,
            description="The username passed as a PlainTextAuthProvider username.",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The password passed as a PlainTextAuthProvider password.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=False,
            description="The earliest record date to sync.",
        ),
        th.Property(
            "request_timeout",
            th.IntegerType,
            required=False,
            description="Request timeout used when not overridden in Session.execute().",
        ),
        th.Property(
            "local_dc",
            th.StringType,
            required=False,
            description="The local_dc parameter should be the name of the datacenter.",
        ),
        th.Property(
            "reconnect_delay",
            th.IntegerType,
            required=False,
            default=60,
            description="Floating point number of seconds to wait inbetween each attempt.",
        ),
        th.Property(
            "max_attempts",
            th.IntegerType,
            required=False,
            default=5,
            description="Should be a total number of attempts to be made before giving up.",
        ),
        th.Property(
            "protocol_version",
            th.IntegerType,
            required=False,
            default=None,
            description="The maximum version of the native protocol to use.",
        ),
        th.Property(
            "fetch_size",
            th.IntegerType,
            required=False,
            default=10000,
            description="The fetch size when syncing data from Cassandra.",
        ),
        th.Property(
            "skip_hot_partitions",
            th.BooleanType,
            required=False,
            default=False,
            description="When set to `True` skipping partitions when faced ReadTimout or ReadFailure errors.",
        ),
        th.Property(
            "ssl_enabled",
            th.BooleanType,
            required=False,
            default=False,
            description="Enable SSL communication when connecting to Cassandra."
        ),
        th.Property(
            "ssl_ca_cert",
            th.StringType,
            required=False,
            default=None,
            description="The SSL CA Certificate to use when connecting to Cassandra (require `ssl_enabled`)."
        ),
        th.Property(
            "ssl_certfile",
            th.StringType,
            required=False,
            default=None,
            description="The client SSL Certificate to use when connecting to Cassandra (require `ssl_enabled`)."
        ),
        th.Property(
            "ssl_keyfile",
            th.StringType,
            required=False,
            default=None,
            description="The client SSL CA Certificate Key to use when connecting to Cassandra (require `ssl_enabled`)."
        ),
        th.Property(
            "ssl_no_verify",
            th.BooleanType,
            required=False,
            default=False,
            description="Skip SSL host verification when connecting to Cassandra (require `ssl_enabled`)."
        ),
    ).to_dict()

    @property
    def connector(self) -> CassandraConnector:
        """Get Cassandra Connector class instance."""

        if self._connector is None:
            self._connector = CassandraConnector(config=dict(self.config))
        return self._connector

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        if self._catalog_dict:
            return self._catalog_dict

        if self.input_catalog:
            return self.input_catalog.to_dict()

        result: dict[str, list[dict]] = {"streams": []}
        result["streams"].extend(self.connector.discover_catalog_entries())

        self._catalog_dict: dict = result
        return self._catalog_dict

    def discover_streams(self) -> list[CassandraStream]:
        """Initialize all available streams and return them as a list.

        Returns:
            List of discovered Stream objects.
        """
        return [
            CassandraStream(self, catalog_entry, connector=self.connector)
            for catalog_entry in self.catalog_dict["streams"]
        ]

if __name__ == "__main__":
    TapCassandra.cli()
