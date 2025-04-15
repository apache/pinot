<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
<div align="center">
    
<img src="https://imgur.com/wBkyzNm.png" align="center" alt="Apache Pinot"/>

---------------------------------------
[![Build Status](https://github.com/apache/pinot/actions/workflows/pinot_tests.yml/badge.svg?event=push)](https://github.com/apache/pinot/actions/workflows/pinot_tests.yml)
[![Release](https://img.shields.io/github/release/apache/pinot/all.svg)](https://pinot.apache.org/download/)
[![codecov.io](https://codecov.io/github/apache/pinot/branch/master/graph/badge.svg)](https://codecov.io/github/apache/pinot)
[![Join the chat at https://communityinviter.com/apps/apache-pinot/apache-pinot](https://img.shields.io/badge/slack-apache--pinot-brightgreen?logo=slack)](https://communityinviter.com/apps/apache-pinot/apache-pinot)
[![Twitter Follow](https://img.shields.io/twitter/follow/apachepinot.svg?label=Follow&style=social)](https://twitter.com/intent/follow?screen_name=apachepinot)
[![License](https://img.shields.io/github/license/apache/pinot.svg)](LICENSE)

</div>

- [What is Apache Pinot?](#what-is-apache-pinot)
  - [Features](#features)
  - [When should I use Pinot?](#when-should-i-use-pinot)
  - [Contributing to Pinot](#contributing-to-pinot)
  - [Apache Pinot YouTube Channel](#apache-pinot-youtube-channel)
  - [Building Pinot](#building-pinot)
  - [Deploying Pinot to Kubernetes](#deploying-pinot-to-kubernetes)
  - [Join the Community](#join-the-community)
  - [Documentation](#documentation)
  - [License](#license)

# What is Apache Pinot?

[Apache Pinot](https://pinot.apache.org) is a real-time distributed OLAP datastore, built to deliver scalable real-time analytics with low latency. It can ingest from batch data sources (such as Hadoop HDFS, Amazon S3, Azure ADLS, Google Cloud Storage) as well as stream data sources (such as Apache Kafka).

Pinot was built by engineers at LinkedIn and Uber and is designed to scale up and out with no upper bound. Performance always remains constant based on the size of your cluster and an expected query per second (QPS) threshold.

For getting started guides, deployment recipes, tutorials, and more, please visit our project documentation at [https://docs.pinot.apache.org](https://docs.pinot.apache.org).

<img src="https://pinot.apache.org/static/images/hero_diagram.svg" align="center" alt="Apache Pinot"/>

## Features

Pinot was originally built at LinkedIn to power rich interactive real-time analytic applications such as [Who Viewed Profile](https://www.linkedin.com/me/profile-views/urn:li:wvmp:summary/),  [Company Analytics](https://www.linkedin.com/company/linkedin/insights/),  [Talent Insights](https://business.linkedin.com/talent-solutions/talent-insights), and many more. [UberEats Restaurant Manager](https://eng.uber.com/restaurant-manager/) is another example of a customer facing Analytics App. At LinkedIn, Pinot powers 50+ user-facing products, ingesting millions of events per second and serving 100k+ queries per second at millisecond latency.

* **Fast Queries**: Filter and aggregate petabyte data sets with P90 latencies in the tens of milliseconds—fast enough to return live results interactively in the UI.

* **High Concurrency**: With user-facing applications querying Pinot directly, it can serve hundreds of thousands of concurrent queries per second.

* **SQL Query Interface**: The highly standard SQL query interface is accessible through a built-in query editor and a REST API.

* **Versatile Joins**: Perform arbitrary fact/dimension and fact/fact joins on petabyte data sets.

* **Column-oriented**: a column-oriented database with various compression schemes such as Run Length, Fixed Bit Length.

* [**Pluggable indexing**](https://docs.pinot.apache.org/basics/indexing): pluggable indexing technologies including [timestamp](https://docs.pinot.apache.org/basics/indexing/timestamp-index), [inverted](https://docs.pinot.apache.org/basics/indexing/inverted-index), [StarTree](https://docs.pinot.apache.org/basics/indexing/star-tree-index), [Bloom filter](https://docs.pinot.apache.org/basics/indexing/bloom-filter), [range](https://docs.pinot.apache.org/basics/indexing/range-index), [text](https://docs.pinot.apache.org/basics/indexing/text-search-support), [JSON](https://docs.pinot.apache.org/basics/indexing/json-index), and [geospatial](https://docs.pinot.apache.org/basics/indexing/geospatial-support) options.

* **Stream and batch ingest**: Ingest from [Apache Kafka](https://kafka.apache.org/), [Apache Pulsar](https://pulsar.apache.org/), and [AWS Kinesis](https://aws.amazon.com/kinesis/) in real time. Batch ingest from Hadoop, Spark, AWS S3, and more. Combine batch and streaming sources into a single table for querying.

* **Upsert during real-time ingestion**: update the data at-scale with consistency

* **Built-in Multitenancy**: Manage and secure data in isolated logical namespaces for cloud-friendly resource management.

* **Built for Scale**: Pinot is horizontally scalable and fault-tolerant, adaptable to workloads across the storage and throughput spectrum.

* **Cloud-native on Kubernetes**: Helm chart provides a horizontally scalable and fault-tolerant clustered deployment that is easy to manage using Kubernetes.

<a href="https://docs.pinot.apache.org/basics/getting-started"><img src="https://gblobscdn.gitbook.com/assets%2F-LtH6nl58DdnZnelPdTc%2F-MKaPf2qveUt5cg0dMbM%2F-MKaPmS1fuBs2CHnx9-Z%2Fpinot-ui-width-1000.gif?alt=media&token=53e4c5a8-a9cd-4610-a338-d54ea036c090" align="center" alt="Apache Pinot query console"/></a>

## When should I use Pinot?

Pinot is designed to execute real-time OLAP queries with low latency on massive amounts of data and events. In addition to real-time stream ingestion, Pinot also supports batch use cases with the same low latency guarantees. It is suited in contexts where fast analytics, such as aggregations, are needed on immutable data, possibly, with real-time data ingestion. Pinot works very well for querying time series data with lots of dimensions and metrics.

Example query:
```SQL
SELECT sum(clicks), sum(impressions) FROM AdAnalyticsTable
  WHERE
       ((daysSinceEpoch >= 17849 AND daysSinceEpoch <= 17856)) AND
       accountId IN (123456789)
  GROUP BY
       daysSinceEpoch TOP 100
```

## Contributing to Pinot

Want to contribute to apache/pinot? 👋🍷

Want to join the ranks of open source committers to Apache Pinot? Then check out the [Contribution Guide](https://docs.pinot.apache.org/developers/developers-and-contributors/contribution-guidelines) for how you can get involved in the code.

If you have a bug or an idea for a new feature, browse the [open issues](https://github.com/apache/pinot/issues) to see what we’re already working on before opening a new one.

We also tagged some [beginner issues](https://github.com/apache/pinot/issues?q=is%3Aopen+is%3Aissue+label%3Abeginner-task) new contributors can tackle.

## Apache Pinot YouTube Channel

Share Your Pinot Videos with the Community!

Have a Pinot use case, tutorial, or conference/meetup recording to share? We’d love to feature it on the [Pinot OSS YouTube channel](https://www.youtube.com/@Apache_Pinot/videos)!
Drop your video or a link to your session in the [#pinot-youtube-channel](https://apache-pinot.slack.com/archives/C08GH2MAVT4) on Pinot Slack, and we’ll showcase it for the community!

## Building Pinot

```
# Clone a repo
$ git clone https://github.com/apache/pinot.git
$ cd pinot

# Build Pinot
# -Pbin-dist is required to build the binary distribution
# -Pbuild-shaded-jar is required to build the shaded jar, which is necessary for some features like spark connectors
$ ./mvnw clean install -DskipTests -Pbin-dist -Pbuild-shaded-jar

# Run the Quick Demo
$ cd build/
$ bin/quick-start-batch.sh
```

For UI development setup refer this [doc](https://github.com/apache/pinot/blob/master/pinot-controller/src/main/resources/Readme.md).

Normal Pinot builds are done using the `./mvnw clean install` command.

However this command can take a long time to run.

For faster builds it is recommended to use `./mvnw verify -Ppinot-fastdev`, which disables some plugins that are not actually needed for development.

More detailed instructions can be found at [Quick Demo](https://docs.pinot.apache.org/basics/getting-started/quick-start) section in the documentation.

## Deploying Pinot to Kubernetes
Please refer to [Running Pinot on Kubernetes](https://docs.pinot.apache.org/basics/getting-started/kubernetes-quickstart) in our project documentation. Pinot also provides Kubernetes integrations with the interactive query engine, [Trino](https://docs.pinot.apache.org/integrations/trino) [Presto](https://docs.pinot.apache.org/integrations/presto), and the data visualization tool, [Apache Superset](helm/superset.yaml).

## Join the Community
 - Ask questions on [Apache Pinot Slack](https://join.slack.com/t/apache-pinot/shared_invite/zt-5z7pav2f-yYtjZdVA~EDmrGkho87Vzw)
 - Please join Apache Pinot mailing lists  
   dev-subscribe@pinot.apache.org (subscribe to pinot-dev mailing list)  
   dev@pinot.apache.org (posting to pinot-dev mailing list)  
   users-subscribe@pinot.apache.org (subscribe to pinot-user mailing list)  
   users@pinot.apache.org (posting to pinot-user mailing list)
 - Apache Pinot Meetup Group: https://www.meetup.com/apache-pinot/

## Documentation
Check out [Pinot documentation](https://docs.pinot.apache.org/) for a complete description of Pinot's features.
- [Quick Demo](https://docs.pinot.apache.org/getting-started/running-pinot-locally)
- [Pinot Architecture](https://docs.pinot.apache.org/basics/architecture)
- [Pinot Query Language](https://docs.pinot.apache.org/users/user-guide-query/pinot-query-language)

## License
Apache Pinot is under [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
