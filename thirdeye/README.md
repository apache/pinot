# Introduction to ThirdEye
[![Build Status](https://travis-ci.org/linkedin/pinot.svg?branch=master)](https://travis-ci.org/linkedin/pinot)

ThirdEye is an integrated tool for realtime monitoring of time series and user-interactive root-cause analysis. It enables anyone inside an organization to collaborate on effective identification and analysis of deviations in business and system metrics. ThirdEye coveres supports the entire workflow from anomaly detection, over root-cause analysis, to resolution and reporting.

## What is it for? (key features)

Online monitoring of business and system metrics (time series) from multiple data sources. ThirdEye comes batteries included for both detection and analysis use cases and aims to minimize the Mean-Time-To-Detection (MTTD) and Mean-To-To-Recovery (MTTR) of production issues. ThirdEye improves automated detection and analysis performance via incremeental user feedback over time.

Detection
* Detection toolkit based on business rules and exponential smoothing
* Real-time, fine-granular monitoring of time series
* Native support for seasonality and permanent change points
* 1-click feedback for automated tuning of detection algorithms

Root-Cause Analysis
* Collaborative analysis dashboard
* Interactive slice-and-dice of data, correlation analysis, and event identification
* Reporting and archiving tools for anomalies and analysesi
* Knowledge graph construction over time from user analyses

Connectivity
* Connectors for continuous time series data from Pinot and CSV 
* Connectors for discrete event data sources, such as holidays from Google calendar
* Plugin support for detection and analysis components

## What isn't it? (limitations)

ThirdEye maintains a dedicated meta-data store to capture data sources, anomalies, and relationships between entities but does not store raw time series data. It relies on systems such as Pinot, Rocks DB, or Kafka to obtain both offline and online time series data.

ThirdEye does not replace your issue tracker - it integrates with it. ThirdEye supports collaboration but focuses on the data-serving aspect of anomaly detection and root-cause analysis. After all, your organization probably has a well-oiled issue resolution process already that we do not want to (nor need to) disrupt.

ThirdEye is not a generic dashboard builder toolkit. ThirdEye attempts to bring overview data from different sources into one single place on-demand. In-depth data about events, such as A/B experiments and deployments, should be kept in their respective systems (ThirdEye can link to these).

## Quick start

ThirdEye supports an interactive demo mode for the analysis dashboard.

### 1: Prerequisites

You'll need Java 8+, Maven 3+, and NPM 3.10+

### 2: Build ThirdEye

```
git clone https://github.com/linkedin/pinot.git
cd pinot/thirdeye
./install.sh
```

Note: The build of thirdeye-frontend may take several minutes

### 3: Run ThirdEye

```
./run.sh
```

### 4: Start an analysis

Point your favorite browser to

```
http://localhost:1426/app/#/rootcause?metricId=1
```

Note: ThirdEye in demo mode will accept any credentials

### 5: Have fun

Available mock metrics are:
* business::puchases
* business::revenue
* tracking::adImpressions
* tracking::pageViews

In demo mode, these metrics are regenerated randomly every time you launch ThirdEye

## More information

More information coming. In the meantime, use your favorite web search engine to search for ThirdEye articles and blog posts.

