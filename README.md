# Pinot

Pinot is a realtime distributed OLAP datastore, which is used at LinkedIn to deliver scalable real time analytics with low latency. It can ingest data from offline data sources (such as Hadoop and flat files) as well as online sources (such as Kafka). At LinkedIn, it powers dozens of both internal and customer-facing analytical applications, such as profile and page views, with interactive-level response times.

## Introduction

In many analytical applications, having a low latency between events occurring and them being queryable opens new possibilities for data analysis. Pinot allows near real time ingestion of events through Kafka as well as batch processing through Hadoop. Because data sizes and query rates can vary a lot between applications, Pinot is designed to scale horizontally and query data sets with billions of rows with sub second latency.

Pinot is queried through the Pinot Query Language (PQL), which is a query language similar to SQL. For example, the following query behaves as one would normally expect:

```sql
SELECT SUM(saleValue) FROM sales
  WHERE year BETWEEN 2012 AND 2015
    AND quarter = 'Q4'
  GROUP BY region, department
```

### What is it for (and not)?

Pinot is well suited for analytical use cases on immutable append-only data that require low latency between an event being ingested and it being available to be queried. Because of the design choices to achieve these goals, there are certain limitations present in Pinot:

#### What is supported

- Fact tables
- Selection and aggregation on fact tables
- Near real time fact ingestion and querying
- Multivalue fields

#### What is not supported

- Joins with other tables (joins with small dimension tables are part of the future roadmap)
- User defined functions
- Nested data types
- Being a system of reference (Pinot can contain important data, but it should not be relied upon as a source of truth; for example, it does not have any kind of backup mechanism and it is assumed that data can simply be regenerated from another system, such as Hadoop, and uploaded again in Pinot)
- Full text search
- Transactions
- Individual row update and deletion

Some of the use cases where Pinot worked well for us is makings events queryable (profile views, ad campaign performance, etc.) in an analytical fashion (who viewed this profile in the last weeks, how many ads were clicked per campaign). It would not be appropriate for storing data that is expected to be updated (user profiles, messages) or can be stored more efficiently in other systems (such as key-value stores like Voldemort or Redis).

## Getting Started

### Requirements

- Java 7 or later
- Zookeeper

Steps to launch the Pinot cluster, upload and query data are outlined below. For detailed description refer to documentation:
- Start the cluster: This includes starting Zookeeper, Pinot-Controller, Pinot-Broker, PinotServer:
```bash
pinot-admin.sh StartZookeeper &

pinot-admin.sh StartController -clusterName "myCluster" \
  -zkAddress "localhost:2181" -controllerPort 9000 \
  -dataDir "/tmp/PinotController" &

pinot-admin.sh StartBroker -clusterName "myCluster" \
  -zkAddress "localhost:2181" &

pinot-admin.sh StartServer -clusterName "myCluster" \
  -zkAddress "localhost:2181" -dataDir /tmp/data \
  -segmentDir /tmp/segment &
```

- Create Pinot segments from JSON/AVRO/CSV

```bash
pinot-admin.sh CreateSegment -dataDir ./data -format CSV \
  -tableName myTable -segmentName mySegment \
  -schemaFile ./data/schema.json \
  -outDir ./data/pinotSegments -overwrite
```

- Add table, upload and query data

```bash
pinot-admin.sh AddTable -filePath ./data/table.json \
  -controllerPort 9000

pinot-admin.sh UploadData -controllerPort 9000 \
  -segmentDir ./data/pinotSegments

pinot-admin.sh PostQuery -brokerUrl http://localhost:8099 \
  -query "select count(*) from 'myTable'"
```
- Stop all processes:
```
pinot-admin.sh StopProcess -server -broker -controller -zooKeeper
```
## Documentation

### Pinot Documentation
- [Getting Started](https://github.com/linkedin/pinot/wiki/Quick-Start-Guide)
- [Client API](https://github.com/linkedin/pinot/wiki/Pinot-Client-API)
- [PQL](https://github.com/linkedin/pinot/wiki/Pinot-Query-Language-Examples)
- [Hadoop](https://github.com/linkedin/pinot/wiki/Pinot-Hadoop-Workflow)
- [Converting Data](https://github.com/linkedin/pinot/wiki/Creating-Pinot-Segments-from-CSV-JSON)
- [Realtime](https://github.com/linkedin/pinot/wiki/Pinot-Realtime-Workflow)

### Pinot Administration
- [Pinot Ops](https://github.com/linkedin/pinot/wiki/Pinot-Ops)

### Contributor

- [Coding Guidelines](https://github.com/linkedin/pinot/wiki/Coding-guidelines)
- [Local instance setup](https://github.com/linkedin/pinot/wiki/Local-Instance-Setup)
- [Testing](https://github.com/linkedin/pinot/wiki/Testing)

### Design Docs

- [Multitenancy](https://github.com/linkedin/pinot/wiki/Multitenancy)
- [Architecture](https://github.com/linkedin/pinot/wiki/Architecture)
- [Query Execution](https://github.com/linkedin/pinot/wiki/Query-Execution)

## License

[Apache License version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
