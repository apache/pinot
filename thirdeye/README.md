# Introduction to ThirdEye
[![Build Status](https://travis-ci.org/linkedin/pinot.svg?branch=master)](https://travis-ci.org/linkedin/pinot) [![license](https://img.shields.io/github/license/linkedin/pinot.svg)](LICENSE)

ThirdEye is an integrated tool for realtime monitoring of time series and interactive root-cause analysis. It enables anyone inside an organization to collaborate on effective identification and analysis of deviations in business and system metrics. ThirdEye supports the entire workflow from anomaly detection, over root-cause analysis, to issue resolution and post-mortem reporting.

## What is it for? (key features)

Online monitoring and analysis of business and system metrics from multiple data sources. ThirdEye comes batteries included for both detection and analysis use cases. It aims to minimize the Mean-Time-To-Detection (MTTD) and Mean-Time-To-Recovery (MTTR) of production issues. ThirdEye improves its detection and analysis performance over time from incremental user feedback.

**Detection**
* Detection toolkit based on business rules and exponential smoothing
* Realtime monitoring of high-dimensional time series
* Native support for seasonality and permanent change points in time series
* Email alerts with 1-click feedback for automated tuning of detection algorithms

**Root-Cause Analysis**
* Collaborative root-cause analysis dashboards
* Interactive slice-and-dice of data, correlation analysis, and event identification
* Reporting and archiving tools for anomalies and analyses
* Knowledge graph construction over time from user feedback

**Integration**
* Connectors for continuous time series data from Pinot and CSV 
* Connectors for discrete event data sources, such as holidays from Google calendar
* Plugin support for detection and analysis components

## What isn't it? (limitations)

ThirdEye maintains a dedicated meta-data store to capture data sources, anomalies, and relationships between entities but does not store raw time series data. It relies on systems such as Pinot, RocksDB, and Kafka to obtain both realtime and historic time series data.

ThirdEye does not replace your issue tracker - it integrates with it. ThirdEye supports collaboration but focuses on the data-integration aspect of anomaly detection and root-cause analysis. After all, your organization probably already has a well-oiled issue resolution process that we don't want to disrupt.

ThirdEye is not a generic dashboard builder toolkit. ThirdEye attempts to bring overview data from different sources into one single place on-demand. In-depth data about events, such as A/B experiments and deployments, should be kept in their respective systems. ThirdEye can link to these directly.

## Quick start

ThirdEye supports an interactive demo mode for the analysis dashboard. These steps will guide you to get started.

### 1: Prerequisites

You'll need Java 8+, Maven 3+, and NPM 3.10+


### 2: Build ThirdEye

```
git clone https://github.com/linkedin/pinot.git
cd pinot/thirdeye
chmod +x ./install.sh
./install.sh
```

Note: The build of thirdeye-frontend may take several minutes


### 3: Run ThirdEye

```
chmod +x ./run.sh
./run.sh
```


### 4: Start an analysis

Point your favorite browser to

```
http://localhost:1426/app/#/rootcause?metricId=1
```

Note: ThirdEye in demo mode will accept any credentials


### 5: Have fun

Available metrics in demo mode are:
* business::puchases
* business::revenue
* tracking::adImpressions
* tracking::pageViews

Note: These metrics are regenerated randomly every time you launch ThirdEye in demo mode


### 6: Shutdown

You can stop the ThirdEye dashboard server anytime by pressing **Ctrl + C** in the terminal


## More information

More information coming. In the meantime, use your favorite web search engine to search for 'Pinot ThirdEye' articles and blog posts.

