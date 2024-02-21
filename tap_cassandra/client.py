"""Custom client handling, including CassandraStream base class."""

import time
import logging

from singer_sdk import typing as th
from singer_sdk._singerlib import CatalogEntry, MetadataMapping, Schema

from cassandra import ReadFailure, ReadTimeout
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import (
    ConsistencyLevel,
    ConstantReconnectionPolicy,
    RetryPolicy,
    DCAwareRoundRobinPolicy,
)
from cassandra.connection import SniEndPointFactory
from cassandra.query import dict_factory, SimpleStatement


class CassandraConnector:
    """Connects to the Cassandra source."""

    # TODO: Implement as a method rather hardcoded dict.
    # Use native cassandra driver to get all available types
    # https://cassandra.apache.org/doc/latest/cassandra/cql/types.html
    # Properly map maps, sets and list, but not to StringType
    CASSANDRA_TO_SINGER_MAP = {
        'ascii': th.StringType, #
        'bigint': th.IntegerType,
        'blob': th.StringType, #
        'boolean': th.BooleanType,
        'counter': th.IntegerType,
        'date': th.DateType,
        'decimal': th.NumberType,
        'double': th.NumberType,
        'duration': th.StringType, #
        'float': th.NumberType,
        'inet': th.StringType, # An IP address, either IPv4 or IPv6
        'int': th.IntegerType,
        'smallint': th.IntegerType,
        'text': th.StringType, #
        'time': th.TimeType,
        'timestamp': th.DateTimeType,
        'timeuuid': th.UUIDType,
        'tinyint': th.IntegerType,
        'uuid': th.UUIDType,
        'varchar': th.StringType, #
        'varint': th.IntegerType,
        'map<': th.StringType, #
        'set<': th.StringType, #
        'list<': th.StringType, #
    }

    def __init__(self, config):
        """Initialize the connector.

        Args:
            config: The connector configuration.
        """

        self.config = config

        self._auth_provider = None
        self._ssl_options = None
        self._profile = None
        self._cluster = None
        self._session = None

    @property
    def auth_provider(self):
        """An AuthProvider that works with Cassandra’s PasswordAuthenticator.

        https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/auth/#cassandra.auth.AuthProvider
        """

        # TODO: Implement other authentication methods.
        if self._auth_provider is None:
            self._auth_provider = PlainTextAuthProvider(
                username=self.config.get('username'), password=self.config.get('password')
            )
        return self._auth_provider

    @property
    def ssl_options(self):
        if self._ssl_options is None and self.config.get('ssl_enabled') == True:
            self._ssl_options = {
                'ssl_no_verify': self.config.get('ssl_no_verify'),
            }
            if self.config.get('ssl_ca_cert') is not None:
                self._ssl_options['ca_certs'] = self.config.get('ssl_ca_cert')
            if self.config.get('ssl_certfile') is not None:
                self._ssl_options['certfile'] = self.config.get('ssl_certfile')
            if self.config.get('ssl_keyfile') is not None:
                self._ssl_options['keyfile'] = self.config.get('ssl_keyfile')
        return self._ssl_options

    @property
    def profile(self):
        """Execution profile property.

        https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/cluster/#cassandra.cluster.ExecutionProfile.
        """

        if self._profile is None:
            self._profile = ExecutionProfile(
                retry_policy=RetryPolicy(),
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
                request_timeout=self.config.get('request_timeout'),
                row_factory=dict_factory,
                load_balancing_policy=DCAwareRoundRobinPolicy(self.config.get('local_dc')),
            )
        return self._profile

    @property
    def cluster(self):
        """The main class to use when interacting with a Cassandra cluster.

        https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/cluster/#module-cassandra.cluster
        """
        if self._cluster is None:
            self._cluster = Cluster(
                endpoint_factory=SniEndPointFactory(proxy_address=self.config.get('host'), port=self.config.get('port')),
                contact_points=[self.config.get('host')],
                port=self.config.get('port'),
                auth_provider=self.auth_provider,
                execution_profiles={EXEC_PROFILE_DEFAULT: self.profile},
                reconnection_policy=ConstantReconnectionPolicy(
                    delay=self.config.get('reconnect_delay'),
                    max_attempts=self.config.get('max_attempts')
                ),
                ssl_options=self.ssl_options,
            )
            if self.config.get('protocol_version'):
                self._cluster.protocol_version = self.config.get('protocol_version')
        return self._cluster

    @property
    def session(self):
        """Session object used to execute the queries."""

        if self._session is None:
            self._session = self.cluster.connect(self.config.get('keyspace'))
        return self._session

    @property
    def logger(self) -> logging.Logger:
        """Get logger.

        Returns:
            Plugin logger.
        """
        return logging.getLogger("cassandra.connector")

    @staticmethod
    def get_fully_qualified_name(table_name=None, schema_name=None, delimiter="."):
        """Concatenates a fully qualified name from the parts.

        Args:
            table_name: The name of the table.
            schema_name: The name of the schema. Defaults to None.
            delimiter: Generally: '.' for SQL names and '-' for Singer names.

        Raises:
            ValueError: If all 3 name parts not supplied.

        Returns:
            The fully qualified name as a string.
        """
        parts = []

        if schema_name:
            parts.append(schema_name)
        if table_name:
            parts.append(table_name)

        if not parts:
            raise ValueError(
                "Could not generate fully qualified name: "
                + ":".join(
                    [
                        schema_name or "(unknown-schema)",
                        table_name or "(unknown-table-name)",
                    ],
                ),
            )

        return delimiter.join(parts)

    @staticmethod
    def query_statement(cql, fetch_size):
        """Create a simple query statement with batch size defined."""

        return SimpleStatement(cql, fetch_size=fetch_size)

    def _is_connected(self):
        """Method to check if connection to Cassandra cluster."""

        return self._cluster is not None and self._session is not None

    def disconnect(self):
        """Method to disconnect from a cluster."""

        if self._is_connected():
            self.cluster.shutdown()

    def execute(self, query):
        """Method to execute the query and return the output.

        Args:
            query: Cassandra CQL query to execute
        """

        try:
            res = self.session.execute(self.query_statement(query, self.config.get('fetch_size')))
            while res.has_more_pages or res.current_rows:
                batch = res.current_rows
                self.logger.info(f'{len(batch)} row(s) fetched.')
                for row in batch:
                    yield row
                res.fetch_next_page()
        except Exception as e:
            self.disconnect()
            raise(e)

    def execute_with_skip(self, query, key_col):
        """Method to execute the query and return the output.

        Handles ReadTimeout and ReadFailure to skip hot partitions.

        Args:
            query: Cassandra CQL query to execute
            key_col: first partition_key of a table
        """

        # Retry for ReadTimeout and ReadFailure
        sleep_time_seconds = 30
        retry = 0
        max_retries = 3
        while retry < max_retries:
            try:
                batch = None
                res = self.session.execute(self.query_statement(query, self.config.get('fetch_size')))
                while res.has_more_pages or res.current_rows:
                    batch = res.current_rows
                    self.logger.info(f'{len(batch)} row(s) fetched.')
                    for row in batch:
                        yield row
                    res.fetch_next_page()
                break
            except (ReadTimeout, ReadFailure) as re:
                retry += 1
                if not batch:
                    res = self.session.execute(self.query_statement(query, 1))
                    batch = res.current_rows
                    self.logger.info(f'{len(batch)} row(s) fetched.')
                last_key = batch[-1][key_col]
                self.logger.info(f'Skipping {key_col} = {last_key}')
                # Remove any filters done for a query
                base_query = query.lower().split('where')[0].rstrip()
                query = base_query + f" where token({key_col}) > token({last_key})"
                print(f'Sleeping for {sleep_time_seconds} before retry')
                self.logger.info(f'Sleeping for {sleep_time_seconds} before retry {retry} out of {max_retries}.')
                time.sleep(sleep_time_seconds)
            except Exception as e:
                self.disconnect()
                raise(e)

    def discover_catalog_entry(
        self,
        table_name: str
    ) -> CatalogEntry:
        """Create `CatalogEntry` object for the given table

        Args:
            table_name: Name of the table

        Returns:
            `CatalogEntry` object for the given table
        """
        self.logger.info('discover_catalog_entry called.')
        table_schema = th.PropertiesList()
        partition_keys = list()
        clustering_keys = list()

        schema_query = f'''
        select *
        from system_schema.columns
        where keyspace_name = '{self.config.get('keyspace')}'
        and table_name = '{table_name}'
        '''

        # Initialize columns list
        for row in self.session.execute(self.query_statement(schema_query, self.config.get('fetch_size'))):
            row_column_name = row['column_name']

            dtype = row['type']
            if dtype not in self.CASSANDRA_TO_SINGER_MAP.keys():
                d2type = [v for k, v in self.CASSANDRA_TO_SINGER_MAP.items() if dtype.startswith(k)]
                if len(d2type) > 0:
                    row_data_type = d2type[0]
                else:
                    row_data_type = th.ObjectType()
            else:
                row_data_type = self.CASSANDRA_TO_SINGER_MAP[dtype]

            if row['kind'] == 'partition_key':
                is_nullable = False
                partition_keys.append(row_column_name)
            elif row['kind'] == 'clustering':
                is_nullable = False
                clustering_keys.append((row_column_name, row['position']))
            else:
                is_nullable = True

            table_schema.append(
                th.Property(
                    name=row_column_name,
                    wrapped=th.CustomType(row_data_type.type_dict),
                    required=not is_nullable,
                ),
            )

        schema = table_schema.to_dict()

        # Initialize unique stream name
        unique_stream_id = f"{self.config.get('keyspace')}-{table_name}"

        # Detect key properties
        clustering_keys.sort(key=lambda tup: tup[1])
        key_properties = partition_keys + [tup[0] for tup in clustering_keys]

        # Initialize available replication methods
        addl_replication_methods: list[str] = [""]  # By default an empty list.
        # Notes regarding replication methods:
        # - 'INCREMENTAL' replication must be enabled by the user by specifying
        #   a replication_key value.
        # - 'LOG_BASED' replication must be enabled by the developer, according
        #   to source-specific implementation capabilities.
        replication_method = next(reversed(["FULL_TABLE", *addl_replication_methods]))

        # Create the catalog entry object
        return CatalogEntry(
            tap_stream_id=unique_stream_id,
            stream=unique_stream_id,
            table=table_name,
            key_properties=key_properties,
            schema=Schema.from_dict(schema),
            replication_method=replication_method,
            metadata=MetadataMapping.get_standard_metadata(
                schema_name=self.config.get('keyspace'),
                schema=schema,
                replication_method=replication_method,
                key_properties=key_properties,
                valid_replication_keys=None,  # Must be defined by user
            ),
            database=None,  # Expects single-database context
            row_count=None,
            stream_alias=None,
            replication_key=None,  # Must be defined by user
        )

    def discover_catalog_entries(self) -> list[dict]:
        """Return a list of catalog entries from discovery.

        Returns:
            The discovered catalog entries as a list.
        """
        result: list[dict] = []

        table_query = f'''
        select table_name
        from system_schema.tables
        where keyspace_name = '{self.config.get('keyspace')}'
        '''
        for table in self.session.execute(self.query_statement(table_query, self.config.get('fetch_size'))):
        # for table in self.session.execute(self.query_statement(table_query, self.config.get('fetch_size'))):
            catalog_entry = self.discover_catalog_entry(table['table_name'])
            result.append(catalog_entry.to_dict())

        return result
