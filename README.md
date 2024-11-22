# Custom data sources/sinks for Cybersecurity-related work


Based on [PySpark DataSource API](https://docs.databricks.com/en/pyspark/datasources.html) available with Spark 4 & DBR 15.4.



```python
from cyber_connectors import *

spark.dataSource.register(RestApiDataSource)

df = spark.range(10)
df.write.format("rest").mode("overwrite").option("uri", "http://localhost:8001/").save()



from cyber_connectors import *
spark.dataSource.register(SplunkDataSource)

df = spark.range(10)
df.write.format("splunk").mode("overwrite").option("uri", "http://192.168.0.10:8088/services/collector").option("token", "...").save()


# Streaming usage

sdf = spark.readStream.format("rate").load()

stream_options = {
  "uri": "http://192.168.0.10:8088/services/collector",
  "token": "....",
  "source": "spark-stream",
  "host": "my_host",
  "time_column": "timestamp",
  "checkpointLocation": "/tmp/splunk-checkpoint/"
}
stream = sdf.writeStream.format("splunk").options(**stream_options).start()


```


# TODOs

- \[ \] add tests - need to mock REST API
