---
id: features_of_pinot
title: Features of Pinot
sidebar_label: Features of Pinot
---

# Features of Pinot

- A column-oriented database with various compression schemes such as Run Length, Fixed Bit Length
- Pluggable indexing technologies - Sorted Index, Bitmap Index, Inverted Index
- Ability to optimize query/execution plan based on query and segment metadata .
- Near real time ingestion from streams and batch ingestion from Hadoop
- SQL like language that supports selection, aggregation, filtering, group by, order by, distinct queries on data.
- Support for multivalued fields
- Horizontally scalable and fault tolerant

Because of the design choices we made to achieve these goals, there are certain limitations in Pinot:

Pinot is not a replacement for database i.e it cannot be used as source of truth store, cannot mutate data
Not a replacement for search engine i.e Full text search, relevance not supported
Query cannot span across multiple tables. 
Pinot works very well for querying time series data with lots of Dimensions and Metrics. <br />

For example:

```SQL
SELECT sum(clicks), sum(impressions) FROM AdAnalyticsTable
  WHERE ((daysSinceEpoch >= 17849 AND daysSinceEpoch <= 17856)) AND accountId IN (123456789)
  GROUP BY daysSinceEpoch TOP 100
```

```SQL  
SELECT sum(impressions) FROM AdAnalyticsTable
  WHERE (daysSinceEpoch >= 17824 and daysSinceEpoch <= 17854) AND adveriserId = '1234356789'
  GROUP BY daysSinceEpoch,advertiserId TOP 100
```

```SQL
SELECT sum(cost) FROM AdAnalyticsTable GROUP BY advertiserId TOP 50
```