# Write Model

<Warning>
This feature is experimental and the API may change in future releases.
</Warning>

Spark Connector also has experimental support for writing Pinot segments from Spark DataFrames.
Currently, only append mode is supported and the schema of the DataFrame should match the schema of the Pinot table.

```scala
// create sample data
val data = Seq(
  ("ORD", "Florida", 1000, true, 1722025994),
  ("ORD", "Florida", 1000, false, 1722025994),
  ("ORD", "Florida", 1000, false, 1722025994),
  ("NYC", "New York", 20, true, 1722025994),
)

val airports = spark.createDataFrame(data)
  .toDF("airport", "state", "distance", "active", "ts")
  .repartition(2)

airports.write.format("pinot")
  .mode("append")
  .option("table", "airlineStats")
  .option("tableType", "OFFLINE")
  .option("segmentNameFormat", "{table}_{partitionId:03}")
  .option("invertedIndexColumns", "airport")
  .option("noDictionaryColumns", "airport,state")
  .option("bloomFilterColumns", "airport")
  .option("timeColumnName", "ts")
  .save("myPath")
```

For more details, refer to the implementation at `org.apache.pinot.connector.spark.v3.datasource.PinotDataWriter`.