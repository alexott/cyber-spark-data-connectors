from datetime import date, datetime
from typing import Any, Dict, Iterator

from pyspark.sql.datasource import DataSource, DataSourceStreamWriter, DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import Row, StructType

from cyber_connectors.common import DateTimeJsonEncoder, SimpleCommitMessage, get_http_session


class RestApiDataSource(DataSource):
    """Data source for REST APIs. Right now supports writing to a REST API.

    Write options:
    - url: REST API URL
    - http_format: (optional) format of the payload - json or form-data (default: json)
    - http_method: (optional) HTTP method to use - get, post or put (default: post)
    - url_query_params: (optional) comma-separated list of column names to send as URL query parameters.
                        Use `*` to send all columns as query parameters.
    - http_header_*: (optional) custom HTTP headers. Use prefix http_header_ followed by header name.
                     Example: http_header_Authorization, http_header_X-API-Key

    """

    @classmethod
    def name(cls):
        return "rest"

    # needed only for reads without schema
    # def schema(self):
    #     return "name string, date string, zipcode string, state string"

    def streamWriter(self, schema: StructType, overwrite: bool) -> DataSourceStreamWriter:
        return RestApiStreamWriter(self.options)

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        return RestApiBatchWriter(self.options)


class RestApiWriter:
    def __init__(self, options: Dict[str, Any]):
        self.options = options
        self.url = self.options.get("url")
        self.payload_format: str = self.options.get("http_format", "json").lower()
        self.http_method: str = self.options.get("http_method", "post").lower()
        self.query_param_columns = self._parse_query_param_columns(self.options.get("url_query_params"))
        assert self.url is not None
        assert self.payload_format in ["json", "form-data"]
        assert self.http_method in ["get", "post", "put"]

        # Extract custom headers with http_header_ prefix
        self.custom_headers = {}
        header_prefix = "http_header_"
        for key, value in self.options.items():
            if key.startswith(header_prefix):
                header_name = key[len(header_prefix):]
                self.custom_headers[header_name] = value

    @staticmethod
    def _parse_query_param_columns(query_param_columns: str | None) -> list[str] | str | None:
        """Parse the url_query_params option."""
        if query_param_columns is None:
            return None

        parsed_value = query_param_columns.strip()
        if not parsed_value:
            raise ValueError("url_query_params cannot be empty")
        if parsed_value == "*":
            return "*"

        columns = [column.strip() for column in parsed_value.split(",") if column.strip()]
        if not columns:
            raise ValueError("url_query_params must contain at least one column name or '*'")
        return columns

    @staticmethod
    def _convert_query_param_value(value: Any) -> str:
        """Convert values to strings suitable for URL query parameters."""
        if value is None:
            return ""
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        return str(value)

    def _split_row_data(self, row_dict: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, str]]:
        """Split a row into request body fields and URL query parameters."""
        if self.query_param_columns is None:
            return row_dict, {}

        if self.query_param_columns == "*":
            query_param_columns = list(row_dict.keys())
        else:
            query_param_columns = self.query_param_columns
            missing_columns = [column for column in query_param_columns if column not in row_dict]
            if missing_columns:
                missing_columns_str = ", ".join(missing_columns)
                raise ValueError(f"Columns specified in url_query_params were not found in row: {missing_columns_str}")

        query_params = {
            column: self._convert_query_param_value(row_dict[column])
            for column in query_param_columns
        }
        payload = {column: value for column, value in row_dict.items() if column not in query_params}
        return payload, query_params

    def write(self, iterator: Iterator[Row]):
        """Write rows to the REST API and return the partition commit message."""
        import json

        from pyspark import TaskContext

        # Start with custom headers (they take precedence)
        additional_headers = self.custom_headers.copy()

        # Add Content-Type only if not already specified by user
        if self.http_method != "get" and self.payload_format == "json" and "Content-Type" not in additional_headers:
            additional_headers["Content-Type"] = "application/json"

        # make retry_on_post configurable
        s = get_http_session(additional_headers=additional_headers, retry_on_post=True)
        context = TaskContext.get()
        partition_id = context.partitionId()
        cnt = 0
        for row in iterator:
            cnt += 1
            data = None
            row_dict = row.asDict()
            payload_dict, query_params = self._split_row_data(row_dict)
            if self.http_method == "get" and payload_dict:
                raise ValueError("HTTP GET requires all row columns to be sent as URL query parameters")

            if self.payload_format == "json":
                data = json.dumps(payload_dict, cls=DateTimeJsonEncoder)
            elif self.payload_format == "form-data":
                # Convert all values to strings for form data
                data = {k: str(v) if v is not None else "" for k, v in payload_dict.items()}

            if self.http_method == "get":
                response = s.get(self.url, params=query_params)
            elif self.http_method == "post":
                response = s.post(self.url, data=data, params=query_params)
            elif self.http_method == "put":
                response = s.put(self.url, data=data, params=query_params)
            else:
                raise ValueError(f"Unsupported http method: {self.http_method}")
            print(response.status_code, response.text)

        return SimpleCommitMessage(partition_id=partition_id, count=cnt)


class RestApiBatchWriter(RestApiWriter, DataSourceWriter):
    def __init__(self, options):
        super().__init__(options)


class RestApiStreamWriter(RestApiWriter, DataSourceStreamWriter):
    def __init__(self, options):
        super().__init__(options)

    def commit(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        """No-op on successful streaming batch commit."""
        pass

    def abort(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        """No-op on failed streaming batch commit."""
        pass
