"""Stream type classes for tap-cassandra."""

from typing import Any, Iterable

from singer_sdk.streams import SQLStream


class CassandraStream(SQLStream):
    """Stream class for Cassandra streams."""

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        If the stream has a replication_key value defined, records will be sorted by the
        incremental key. If the stream also has an available starting bookmark, the
        records will be filtered for values greater than or equal to the bookmark value.

        Args:
            context: If partition context is provided, will read specifically from this
                data slice.

        Yields:
            One dict per record.

        Raises:
            NotImplementedError: If partition is passed in context and the stream does
                not support partitioning.
        """
        if context:
            msg = f"Stream '{self.name}' does not support partitioning."
            raise NotImplementedError(msg)

        selected_column_names = self.get_selected_schema()["properties"].keys()
        selected_column_string = ','.join(selected_column_names)

        cql = f"select {selected_column_string} from {self.name.split('-')[1]}"
        for record in self.connector.execute(cql):
            yield record
