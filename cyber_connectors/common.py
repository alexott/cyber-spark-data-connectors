import json
from dataclasses import dataclass
from datetime import date, datetime

from pyspark.sql.datasource import WriterCommitMessage


@dataclass
class SimpleCommitMessage(WriterCommitMessage):
    partition_id: int
    count: int


class DateTimeJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime) or isinstance(o, date):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)


def get_http_session(retry: int = 5, additional_headers: dict = None, retry_on_post: bool = False):
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util import Retry

    session = requests.Session()
    if additional_headers:
        session.headers.update(additional_headers)

    if retry > 0:
        allowed_methods = ["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE"]
        if retry_on_post:
            allowed_methods.append("POST")
        retry_strategy = Retry(
            total=retry,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=allowed_methods,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

    return session


def _driver_dbutils():
    """Return the driver-side ``dbutils`` object on a Databricks runtime.

    Tries several lookup strategies because where ``dbutils`` lives depends
    on the runtime:

    1. **DBR classic notebook** — the kernel injects ``dbutils`` into the
       notebook's IPython namespace and ``__main__`` globals.
    2. **DBR Serverless / Databricks Connect** — the data source callback
       runs in an isolated Python process with no notebook kernel attached.
       The active ``SparkSession`` carries the workspace auth context, so
       ``pyspark.dbutils.DBUtils(spark)`` produces a working ``dbutils``.
    3. **Bare SDK runtime** — last-resort proxy that requires the SDK to be
       able to construct a workspace ``Config``.
    """
    try:
        import IPython

        ipy = IPython.get_ipython()
        if ipy is not None:
            db = ipy.user_ns.get("dbutils")
            if db is not None:
                return db
    except Exception:
        pass

    import sys

    main = sys.modules.get("__main__")
    if main is not None:
        db = getattr(main, "dbutils", None)
        if db is not None:
            return db

    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            return DBUtils(spark)
    except Exception:
        pass

    from databricks.sdk.runtime import dbutils

    return dbutils


def get_service_credentials_provider(credential_name):
    """Return a Unity Catalog service credential provider, on driver or executor.

    ``databricks.service_credentials.getServiceCredentialsProvider`` only
    works on Spark executors (where a ``TaskContext`` is present). On the
    driver — for example during schema inference or other ``DataSource``
    callbacks invoked before tasks are scheduled — we fall back to the
    notebook-injected ``dbutils.credentials.getServiceCredentialsProvider``.

    Args:
        credential_name: Name of the Unity Catalog service credential.

    Returns:
        Credential provider object suitable for the underlying client SDK.

    """
    from pyspark import TaskContext

    if TaskContext.get() is not None:
        from databricks.service_credentials import getServiceCredentialsProvider

        return getServiceCredentialsProvider(credential_name)

    return _driver_dbutils().credentials.getServiceCredentialsProvider(credential_name)
