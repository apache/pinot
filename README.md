# Introduction to Pinot

[![Build Status](https://travis-ci.org/linkedin/pinot.svg?branch=master)](https://travis-ci.org/linkedin/pinot) [![codecov.io](https://codecov.io/github/linkedin/pinot/branch/master/graph/badge.svg)](https://codecov.io/github/linkedin/pinot) [![Join the chat at https://gitter.im/linkedin/pinot](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/linkedin/pinot?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![license](https://img.shields.io/github/license/linkedin/pinot.svg)](LICENSE)

Pinot is a realtime distributed OLAP datastore, which is used at LinkedIn to deliver scalable real time analytics with low latency. It can ingest data from offline data sources (such as Hadoop and flat files) as well as online sources (such as Kafka). Pinot is designed to scale horizontally.

These three presentations on Pinot give an overview of Pinot and how it is used at LinkedIn:

* [Pinot: Realtime Distributed OLAP Datastore (Aug 2015)](http://www.slideshare.net/KishoreGopalakrishna/pinot-realtime-distributed-olap-datastore)
* [Introduction to Pinot (Jan 2016)](http://www.slideshare.net/jeanfrancoisim/intro-to-pinot-20160104) 
* [Open Source Analytics Pipeline at LinkedIn (Sep 2016, covers Gobblin and Pinot)](http://www.slideshare.net/IssacBuenrostro/open-source-linkedin-analytics-pipeline-vldb-2016)

## What is it for (and not)?

Pinot is well suited for analytical use cases on immutable append-only data that require low latency between an event being ingested and it being available to be queried. 

### Key Features

- A column-oriented database with various compression schemes such as Run Length, Fixed Bit Length
- Pluggable indexing technologies - Sorted Index, Bitmap Index, Inverted Index
- Ability to optimize query/execution plan based on query and segment metadata . 
- Near real time ingestion from Kafka and batch ingestion from Hadoop
- SQL like language that supports _selection, aggregation, filtering, group by, order by, distinct_ queries on fact data.
- Support for multivalued fields
- Horizontally scalable and fault tolerant 

Because of the design choices we made to achieve these goals, there are certain limitations present in Pinot:

- Pinot is not a replacement for database i.e it cannot be used as source of truth store, cannot mutate data 
- Not a replacement for search engine i.e Full text search, relevance not supported
- Query cannot span across multiple tables. 

Pinot works very well for querying time series data with lots of Dimensions and Metrics. Example - Query (profile views, ad campaign performance, etc.) in an analytical fashion (who viewed this profile in the last weeks, how many ads were clicked per campaign). 

## Terminology

Before we get to quick start, lets go over the terminology. 
- Table: A table is a logical abstraction to refer to a collection of related data. It consists of columns and rows (Document). Table Schema defines column names and their metadata.
- Segment: A logical table is divided into multiple physical units referred to as segments.

Pinot has following Roles/Components:

- Pinot Controller: Manages nodes in the cluster. Responsibilities :
  * Handles all Create, Update, Delete operations on Tables and Segments.
  * Computes assignment of Table and its segments to Pinot Servers.  
- Pinot Server: Hosts one or more physical segments. Responsibilities: -
  * When assigned a pre created segment, download it and load it. If assigned a Kafka topic, start consuming from a sub set of partitions in Kafka.
  * Executes queries and returns the response to Pinot Broker.
- Pinot Broker: Accepts queries from clients and routes them to multiple servers (based of routing strategy). All responses are merged and sent back to client.

Pinot leverages [Apache Helix](http://helix.apache.org) for cluster management. 

For more information on Pinot Design and Architecture can be found [here](https://github.com/linkedin/pinot/wiki/Architecture)

***

## Quick Start 

You can either build Pinot manually or use Docker to run Pinot.

### 1: Build and install Pinot (optional if you have Docker installed)

```
git clone https://github.com/linkedin/pinot.git
cd pinot
mvn install package  -DskipTests
cd pinot-distribution/target/pinot-0.016-pkg
chmod +x bin/*.sh
```

### Run

We will load BaseBall stats from 1878 to 2013 into Pinot and run queries against it. There are 100000 records and 15 columns ([schema](https://github.com/linkedin/pinot/blob/master/pinot-tools/src/main/resources/sample_data/baseballStats_schema.json)) in this dataset.

Execute the quick-start-offline.sh script in bin folder which performs the following:
- Converts Baseball data in CSV format into Pinot Index Segments.
- Starts Pinot components, Zookeeper, Controller, Broker, Server.
- Uploads segment to Pinot

If you have Docker, run `docker run -it -p 9000:9000 linkedin/pinot-quickstart-offline`. If you have built Pinot, run `bin/quick-start-offline.sh`.

We should see the following output.

```
Deployed Zookeeper
Deployed controller, broker and server
Added baseballStats schema
Creating baseballStats table
Built index segment for baseballStats
Pushing segments to the controller
```

At this point we can post queries. Here are some of the sample queries. 
Sample queries:

```sql
/*Total number of documents in the table*/
select count(*) from baseballStats

/*Top 5 run scorers of all time*/ 
select sum(runs) from baseballStats group by playerName top 5

/*Top 5 run scorers of the year 2000*/
select sum(runs) from baseballStats where yearID = 2000 group by playerName top 5

/*Top 10 run scorers after 2000*/
select sum(runs) from baseballStats where yearID >= 2000 group by playerName

/*Select playerName,runs,homeRuns for 10 records from the table and order them by yearID*/
select playerName, runs, homeRuns from baseballStats order by yearID limit 10

```

### Step 3: Pinot Data Explorer

There are 3 ways to [interact](https://github.com/linkedin/pinot/wiki/Pinot-Client-API) with Pinot - simple web interface, REST api and java client. Open your browser and go to http://localhost:9000/query/ and run any of the queries provided above. See [Pinot Query Syntax](https://github.com/linkedin/pinot/wiki/Pinot-Query-Language-Examples) for more info.

*** 
## Realtime quick start

There are two ways to ingest data into Pinot - batch and realtime. Previous baseball stats demonstrated ingestion in batch. Typically these batch jobs are run on Hadoop periodically (e.g every hour/day/week/month). Data freshness depends on job granularity. 

Lets look at an example where we ingest data in realtime. We will subscribe to meetup.com rsvp feed and index the rsvp events in real time. 
Execute quick-start-realtime.sh script in bin folder which performs the following:
- start a kafka broker 
- setup a meetup event listener that subscribes to meetup.com stream and publishes it to local kafka broker
- start zookeeper, pinot controller, pinot broker, pinot-server.
- configure the realtime source 

If you have Docker, run `docker run -it -p 9000:9000 linkedin/pinot-quickstart-realtime`. If you have built Pinot, run `bin/quick-start-realtime.sh`.

```
Starting Kafka
Created topic "meetupRSVPEvents".
Starting controller, server and broker
Added schema and table
Realtime quick start setup complete
Starting meetup data stream and publishing to kafka
```

Open Pinot Query Console at http://localhost:9000/query and run queries. Here are some sample queries

```sql
/*Total number of documents in the table*/
select count(*) from meetupRsvp

/*Top 10 cities with the most rsvp*/	
select sum(rsvp_count) from meetupRsvp group by group_city top 10

/*Show 10 most recent rsvps*/
select * from meetupRsvp order by mtime limit 10 

/*Show top 10 rsvp'ed events*/
select sum(rsvp_count) from meetupRsvp group by event_name top 10

```

## Pinot usage

At LinkedIn, it powers more than 50+ applications such as  Who Viewed My Profile, Who Viewed My Jobs and many more, with interactive-level response times. Pinot ingests close to a Billion per day in real time and processes 100 million queries per day.

## Discussion Group
Please join or post questions to this group. 
https://groups.google.com/forum/#!forum/pinot_users

## Documentation
- [Home](https://github.com/linkedin/pinot/wiki/Home)
- [How to use Pinot](https://github.com/linkedin/pinot/wiki/How-To-Use-Pinot)
- [Design and Architecture](https://github.com/linkedin/pinot/wiki/Architecture)
- [Pinot Administration](https://github.com/linkedin/pinot/wiki/Pinot-Administration)
