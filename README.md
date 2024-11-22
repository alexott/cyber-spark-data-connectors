# Custom data sources/sinks for Cybersecurity-related work


Based on [PySpark DataSource API](https://docs.databricks.com/en/pyspark/datasources.html) available with Spark 4 & DBR 15.4.



```py
from cyber_connectors import *

spark.dataSource.register(RestApiDataSource)

df = spark.range(10)
df.write.format("rest").mode("overwrite").option("uri", "http://localhost:8001/").save()


```
