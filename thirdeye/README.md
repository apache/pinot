# Introduction to ThirdEye
[![Build Status](https://api.travis-ci.org/apache/incubator-pinot.svg?branch=master)](https://travis-ci.org/apache/incubator-pinot) [![license](https://img.shields.io/github/license/linkedin/pinot.svg)](LICENSE)

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
* Connectors for continuous time series data from Pinot, Presto, MySQL and CSV
* Connectors for discrete event data sources, such as holidays from Google calendar
* Plugin support for detection and analysis components

## What isn't it? (limitations)

ThirdEye maintains a dedicated meta-data store to capture data sources, anomalies, and relationships between entities but does not store raw time series data. It relies on systems such as Pinot, Presto, MySQL, RocksDB, and Kafka to obtain both realtime and historic time series data.

ThirdEye does not replace your issue tracker - it integrates with it. ThirdEye supports collaboration but focuses on the data-integration aspect of anomaly detection and root-cause analysis. After all, your organization probably already has a well-oiled issue resolution process that we don't want to disrupt.

ThirdEye is not a generic dashboard builder toolkit. ThirdEye attempts to bring overview data from different sources into one single place on-demand. In-depth data about events, such as A/B experiments and deployments, should be kept in their respective systems. ThirdEye can link to these directly.

## Getting Involved
 - Ask questions on [Apache ThirdEye Slack](https://communityinviter.com/apps/apache-thirdeye/apache-thirdeye)
 - Please join Apache Pinot mailing lists  
   dev-subscribe@pinot.apache.org (subscribe to pinot-dev mailing list)  
   dev@pinot.apache.org (posting to pinot-dev mailing list)  
   users-subscribe@pinot.apache.org (subscribe to pinot-user mailing list)  
   users@pinot.apache.org (positng to pinot-user mailing list)

## Documentation

Detailed documentation can be found at [ThirdEye documentation](https://thirdeye.readthedocs.io) for a complete description of ThirdEye's features.

- [Quick Start](https://thirdeye.readthedocs.io/en/latest/quick_start.html)
- [Data Sources Setup](https://thirdeye.readthedocs.io/en/latest/datasources.html)
- [Production Settings](https://thirdeye.readthedocs.io/en/latest/production.html)
- [Alert Setup](https://thirdeye.readthedocs.io/en/latest/alert_setup.html)
- [Chat](https://communityinviter.com/apps/apache-thirdeye/apache-thirdeye)