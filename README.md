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
# Apache Pinot (incubating)

[![Build Status](https://api.travis-ci.org/apache/incubator-pinot.svg?branch=master)](https://travis-ci.org/apache/incubator-pinot) [![codecov.io](https://codecov.io/github/apache/incubator-pinot/branch/master/graph/badge.svg)](https://codecov.io/github/apache/incubator-pinot) [![Join the chat at https://gitter.im/linkedin/pinot](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/linkedin/pinot?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![license](https://img.shields.io/github/license/apache/pinot.svg)](LICENSE)

Apache Pinot is a realtime distributed OLAP datastore, which is used to deliver scalable real time analytics with low latency. It can ingest data from offline data sources (such as Hadoop and flat files) as well as online sources (such as Kafka). Pinot is designed to scale horizontally.

These presentations on Pinot give an overview of Pinot:

* [Pinot: Enabling Real-time Analytics Applications @ LinkedIn's Scale  - ApacheCon 2019 (Sep 2019)](https://www.slideshare.net/seunghyunlee1460/pinot-enabling-realtime-analytics-applications-linkedins-scale)
* [Pinot: Realtime OLAP for 530 Million Users - Sigmod 2018 (Jun 2018)](http://www.slideshare.net/seunghyunlee1460/pinot-realtime-olap-for-530-million-users-sigmod-2018-107394584)
* [Open Source Analytics Pipeline at LinkedIn (Sep 2016, covers Gobblin and Pinot)](http://www.slideshare.net/IssacBuenrostro/open-source-linkedin-analytics-pipeline-vldb-2016)
* [Introduction to Pinot (Jan 2016)](http://www.slideshare.net/jeanfrancoisim/intro-to-pinot-20160104)
* [Pinot: Realtime Distributed OLAP Datastore (Aug 2015)](http://www.slideshare.net/KishoreGopalakrishna/pinot-realtime-distributed-olap-datastore)

Looking for the ThirdEye anomaly detection and root-cause analysis platform? Check out the [Pinot/ThirdEye project](https://github.com/apache/incubator-pinot/tree/master/thirdeye)

## Key Features

- A column-oriented database with various compression schemes such as Run Length, Fixed Bit Length
- Pluggable indexing technologies - Sorted Index, Bitmap Index, Inverted Index, Star-Tree Index
- Ability to optimize query/execution plan based on query and segment metadata
- Near real time ingestion from Kafka and batch ingestion from Hadoop
- SQL like language that supports _selection, aggregation, filtering, group by, order by, distinct_ queries on fact data
- Support for multivalued fields
- Horizontally scalable and fault tolerant 

Because of the design choices we made to achieve these goals, there are certain limitations present in Pinot:

- Pinot is not a replacement for database i.e it cannot be used as source of truth store, cannot mutate data 
- Not a replacement for search engine i.e full text search, relevance not supported
- Query cannot span across multiple tables

Pinot works very well for querying time series data with lots of Dimensions and Metrics. Example - Query (profile views, ad campaign performance, etc.) in an analytical fashion (who viewed this profile in the last weeks, how many ads were clicked per campaign). 

## Instructions to build Pinot
More detailed instructions can be found at [Quick Demo](https://apache-pinot.gitbook.io/apache-pinot-cookbook/getting-started) section in the documentation.
```
# Clone a repo
$ git clone https://github.com/apache/incubator-pinot.git
$ cd incubator-pinot

# Build Pinot
$ mvn clean install -DskipTests -Pbin-dist

# Run the Quick Demo
$ cd pinot-distribution/target/apache-pinot-incubating-<version>-SNAPSHOT-bin
$ bin/quick-start-batch.sh
```

## Deploy Pinot on Kubernetes
Please refer to [Kubernetes Readme](kubernetes/helm/README.md) to deploy Pinot using [Helm](https://helm.sh/docs/using_helm/#installing-helm) and load demo data set.

Pinot also provides k8s integration with interactive query engine [Presto](kubernetes/helm/presto-coordinator.yaml) and data visualization tool [Apache Superset](kubernetes/helm/superset.yaml).

## Getting Involved
 - Ask questions on [Apache Pinot Slack](https://communityinviter.com/apps/apache-pinot/apache-pinot)
 - Please join Apache Pinot mailing lists  
   dev-subscribe@pinot.apache.org (subscribe to pinot-dev mailing list)  
   dev@pinot.apache.org (posting to pinot-dev mailing list)  
   users-subscribe@pinot.apache.org (subscribe to pinot-user mailing list)  
   users@pinot.apache.org (posting to pinot-user mailing list)

## Documentation
Check out [Pinot documentation](https://apache-pinot.gitbook.io/apache-pinot-cookbook/) for a complete description of Pinot's features.
- [Quick Demo](https://apache-pinot.gitbook.io/apache-pinot-cookbook/getting-started/running-pinot-locally)
- [Pinot Architecture](https://apache-pinot.gitbook.io/apache-pinot-cookbook/concepts/architecture)
- [Pinot Query Language](https://apache-pinot.gitbook.io/apache-pinot-cookbook/pinot-user-guide/pinot-query-language)

## License
Apache Pinot is under [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
