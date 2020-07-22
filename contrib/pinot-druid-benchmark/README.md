# Running the benchmark

For instructions on how to run the Pinot/Druid benchmark please refer to the
```run_benchmark.sh``` file. 

In order to run the Apache Pinot benchmark you'll need to create the appropriate
data segments, which are too large to be included in this github repository and
they may need to be recreated with new Apache Pinot versions.

To create the neccessary segment data for the benchmark please follow the
instructions below.

# Creating Apache Pinot benchmark segments from TPC-H data

To run the Pinot/Druid benchmark with Apache Pinot you'll need to download and run 
the TPC-H tools to generate the benchmark data sets.

## Downloading and building the TPC-H tools

The TPC-H tools can be downloaded from the [TPC-H Website](http://www.tpc.org/tpch/default5.asp). 
Registration is required.

**Note:**: The instructions below for dbgen assume a Linux OS.

After downloading and extracing the TPC-H tools, you'll need to build the
db generator tool: ```dbgen```. To do so, extract the package that you have 
downloaded from TPC-H's website and inside the dbgen sub directory edit the 
```makefile``` file.

Set the following variables in the makefile to:

```
CC      = gcc
...
DATABASE= SQLSERVER
MACHINE = LINUX
WORKLOAD = TPCH
```

Next, build the dbgen tool as per the README instructions in the dbgen directory.

## Generating the TPC-H data and converting them for use in Apache Pinot

After building ```dbgen``` run the following command line in the ```dbgen``` directory:

```
./dbgen -TL -s8
```

The command above will generate a single large file called ```lineitem.tbl```.
This is the data file for the TPC-H benchmark, which we'll need to post-process 
a bit to be imported into Apache Pinot.

Next, build the Pinot/Druid Benchmark code if you haven't done so already.

**Note:** Apache Pinot has JDK11 support, however for now it's
best to use JDK8 for all build and run operations in this manual.

Inside ```pinot_directory/contrib/pinot-druid-benchmark``` run:

```
mvn clean install
```

Next, inside the same directory split the ```lineitem``` table:

```
./target/appassembler/bin/data-separator.sh <Path to lineitem.tbl> <Output Directory> 
```

Use the output directory from the split as the input directory for the merge
command below:

```
./target/appassembler/bin/data-merger.sh <Input Directory> <Output Directory> YEAR
```

If all ran well you should see a few CSV files produced, 1992.csv through 1998.csv.

These files are the starting point for creating our Apache Pinot segments.

## Create the Apache Pinot segments

The first step in the process is to launch a standalone Apache Pinot Cluster on one
single server. This cluster will serve as a host to hold the initial segments, 
which we'll extract and copy for later re-use in the benchmark.

Follow the steps outlined in the Apache Pinot Manual Cluster setup to launch the
cluster:

https://docs.pinot.apache.org/basics/getting-started/advanced-pinot-setup

You don't need the Kafka service as we won't be using it.

Next, we need to follow the instructions similar to the ones described in
the [Batch Import Example](https://docs.pinot.apache.org/basics/getting-started/pushing-your-data-to-pinot)
in the Apache Pinot documentation.

### Create the Apache Pinot tables

Run:

```
pinot-admin.sh AddTable \
  -tableConfigFile /absolute/path/to/table-config.json \
  -schemaFile /absolute/path/to/schema.json -exec
```

For this command above you'll need the following configuration files:

```table_config.json```
```
{
  "tableName": "tpch_lineitem",
  "segmentsConfig" : {
    "replication" : "1",
    "schemaName" : "tpch_lineitem",
    "segmentAssignmentStrategy" : "BalanceNumSegmentAssignmentStrategy"
  },
  "tenants" : {
    "broker":"DefaultTenant",
    "server":"DefaultTenant"
  },
  "tableIndexConfig" : {
    "starTreeIndexConfigs":[{
      "maxLeafRecords": 100,
      "functionColumnPairs": ["SUM__l_extendedprice", "SUM__l_discount", "SUM__l_quantity"],
      "dimensionsSplitOrder": ["l_receiptdate", "l_shipdate", "l_shipmode", "l_returnflag"],
      "skipStarNodeCreationForDimensions": [],
      "skipMaterializationForDimensions": ["l_partkey", "l_commitdate", "l_linestatus", "l_comment", "l_orderkey", "l_shipinstruct", "l_linenumber", "l_suppkey"]
    }]
  },
  "tableType":"OFFLINE",
  "metadata": {}
}
```

```schema.json```
```
{
  "schemaName": "tpch_lineitem",
  "dimensionFieldSpecs": [
    {
      "name": "l_orderkey",
      "dataType": "INT"
    },
    {
      "name": "l_partkey",
      "dataType": "INT"
    },
    {
      "name": "l_suppkey",
      "dataType": "INT"
    },
    {
      "name": "l_linenumber",
      "dataType": "INT"
    },
    {
      "name": "l_returnflag",
      "dataType": "STRING"
    },
    {
      "name": "l_linestatus",
      "dataType": "STRING"
    },
    {
      "name": "l_shipdate",
      "dataType": "STRING"
    },
    {
      "name": "l_commitdate",
      "dataType": "STRING"
    },
    {
      "name": "l_receiptdate",
      "dataType": "STRING"
    },
    {
      "name": "l_shipinstruct",
      "dataType": "STRING"
    },
    {
      "name": "l_shipmode",
      "dataType": "STRING"
    },
    {
      "name": "l_comment",
      "dataType": "STRING"
    }
  ],
  "metricFieldSpecs": [
    {
      "name": "l_quantity",
      "dataType": "LONG"
    },
    {
      "name": "l_extendedprice",
      "dataType": "DOUBLE"
    },
    {
      "name": "l_discount",
      "dataType": "DOUBLE"
    },
    {
      "name": "l_tax",
      "dataType": "DOUBLE"
    }
  ]
}
```

**Note:** The configuration as specified above will give you
the data with the **optimal star tree index**. The index configuration is
specified in the ```tableIndexConfig``` section in the ```table_config.json``` file. If
you want to generate a different type of indexed segment, then you
should modify the tableIndexConfig section to reflect the correct index
type as described in the [Indexing Section](https://docs.pinot.apache.org/basics/features/indexing) 
of the Apache Pinot Documentation.

### Create the Apache Pinot segments

Next, we'll create the segments for this Apache Pinot table using the optimal
star tree index configuration. 

For this purpose you'll need a job specification YAML file. Here's an example
that does the TPC-H data import:

```job-spec.yml```
```
executionFrameworkSpec:
  name: 'standalone'
  segmentGenerationJobRunnerClassName: 'org.apache.pinot.plugin.ingestion.batch.standalone.SegmentGenerationJobRunner'
  segmentTarPushJobRunnerClassName: 'org.apache.pinot.plugin.ingestion.batch.standalone.SegmentTarPushJobRunner'
  segmentUriPushJobRunnerClassName: 'org.apache.pinot.plugin.ingestion.batch.standalone.SegmentUriPushJobRunner'
jobType: SegmentCreationAndTarPush
inputDirURI: '/absolute/path/to/incubator-pinot/contrib/pinot-druid-benchmark/data_out/raw_data/'
includeFileNamePattern: 'glob:**/*.csv'
outputDirURI: '/absolute/path/to/incubator-pinot/contrib/pinot-druid-benchmark/data_out/segments/'
overwriteOutput: true
pinotFSSpecs:
  - scheme: file
    className: org.apache.pinot.spi.filesystem.LocalPinotFS
recordReaderSpec:
  dataFormat: 'csv'
  className: 'org.apache.pinot.plugin.inputformat.csv.CSVRecordReader'
  configClassName: 'org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig'
  configs:
    delimiter: '|'
    header: 'l_orderkey|l_partkey|l_suppkey|l_linenumber|l_quantity|l_extendedprice|l_discount|l_tax|l_returnflag|l_linestatus|l_shipdate|l_commitdate|l_receiptdate|l_shipinstruct|l_shipmode|l_comment|'
tableSpec:
  tableName: 'tpch_lineitem'
  schemaURI: 'http://localhost:9000/tables/tpch_lineitem/schema'
  tableConfigURI: 'http://localhost:9000/tables/tpch_lineitem'
pinotClusterSpecs:
  - controllerURI: 'http://localhost:9000'
```

**Note:** Make sure you modify the absolute path for **inputDirURI** and **outputDirURI**
above. The inputDirURI should be pointing to the directory where you have
generated the 7 YEAR CSV files, 1992.csv through 1998.csv.

After you have modified the input and output dir, run the job as described in the 
[Batch Import Example](https://docs.pinot.apache.org/basics/getting-started/pushing-your-data-to-pinot) document:


```
pinot-admin.sh LaunchDataIngestionJob \
    -jobSpecFile /absolute/path/to/job-spec.yml
```

The segment creation output on the console will tell you where Apache Pinot will
store the created segments (it should be your output dir). You should see a 
line appear in the output as:

```
...
outputDirURI: /absolute/path/to/incubator-pinot/contrib/pinot-druid-benchmark/data_out/segments/
...
```

Inside there you'll find the tpch_lineitem_OFFLINE directory with 7 separate 
segments, 0 through 6. Tar/GZip the whole directory and this will be your
optimal_startree_small_yearly temp segment that the benchmark requires. However,
wait first for the segment creation to finish.

Try few queries to ensure that the segments are working. You can find some
sample queries under the benchmark directory ```src/main/resources/pinot_queries```.
Watch the console output from the Apache Pinot cluster as you run the queries, and make sure 
there are no complaints in there that the queries were slow since index wasn't found. 
If you see a message saying the query was slow, it means that the indexes weren't 
created properly. With the optimal star tree index your total query time should be
few milliseconds at most.

You can now shutdown the Apache Pinot cluster which you started manually and when you
launch the benchmark server cluster it will pick up your new segments. 

