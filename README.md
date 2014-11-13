ThirdEye
========

ThirdEye is a system for efficient monitoring of and drill-down into business
metrics.

Build
-----

To build the project

```
mvn clean install package
```

Configuration
-------------

A configuration minimally consists of the following values:

* collection
* dimensionNames
* metricNames
* timeColumnName

For example,

```
{
    "collection": "abook",
    "dimensionNames": [
        "browserName",
        "locale",
        "countryCode",
        "emailDomain",
        "isSuccess",
        "errorStatus",
        "environment",
        "source",
        "deviceName"
    ],
    "metricNames": [
        "numberOfMemberConnectionsSent",
        "numberOfGuestInvitationsSent",
        "numberOfSuggestedMemberConnections",
        "numberOfSuggestedGuestInvitations",
        "numberOfImportedContacts"
    ],
    "timeColumnName": "hoursSinceEpoch"
}
```

### Record Store

Each `(dimensions, time, metrics)` tuple is referred to as a "record" here.

There are two record store implementations for different use cases:

`com.linkedin.thirdeye.impl.StarTreeRecordStoreByteBufferImpl`, which is a dynamically growing buffer for records, on which periodic compaction is performed. This should be used when the dimension combinations are unknown, such as during tree bootstrap, or for ad hoc use cases, and is the default implementation. It accepts the following config parameters:

* `bufferSize` - the default buffer size (buffer grows by this amount each time)
* `useDirect` - if true, use direct byte buffers (otherwise, heap buffers)
* `targetLoadFactor` - when the buffer is this full, try compaction, then if still this full, resize

`com.linkedin.thirdeye.impl.StarTreeRecordStoreCircularBufferImpl`, which is a fixed circular buffer (i.e. has a fixed set of dimension combinations and time buckets). This should be used when the star-tree index structure is built offline then loaded. It accepts the following config parameters:

* `rootDir` - the directory under which node buffers / indexes exist
* `numTimeBuckets` - the number of time buckets for each dimension combination

Each implementation has the following record store factory class:

* `com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryByteBufferImpl`
* `com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryCircularBufferImpl`

The record store can be specified via the following config parameters:

```
{
    ...,
    "recordStoreFactoryClass": "{className}",
    "recordStoreFactoryConfig": {
        "{name}": "{value}",
        ...
    }
}
```

### Threshold Function

If there is significant skew among the data with respect to one or more dimensions, it may be useful to define a threshold function to roll up dimension values that individually contribute little, but as a whole contribute a significant part of aggregates.

What this means exactly is that periodically, a sample of the data with respect to one dimension, grouped by dimension value, will be taken and provided to the threshold function. At that point, it is the threshold function's responsibility to decide what groups of records pass the threshold, and what don't.

The interface looks something like this

```
public interface StarTreeRecordThresholdFunction {
  /** @return the set of dimension values (i.e. keys of sample) that pass threshold */
  Set<String> apply(Map<String, List<StarTreeRecord>> sample);
}
```

Any dimension value not in the return value of this function is classified as "other" (i.e. `?`).

The threshold function can be specified via the following config parameters:

```
{
    ...,
    "thresholdFunctionClass": "{className}",
    "thresholdFunctionConfig": {
        "{name}": "{value}",
        ...
    }
}
```

N.b. this process can also be done offline to avoid bias introduced by sampling the data.

Bootstrap
---------

TODO

Run
---

TODO

API
---

The ThirdEye server exposes the following resources:

* `/collections`
* `/dimensions`
* `/metrics`

### Resources

| Method | Route | Description |
|--------|-------|-------------|
| GET | `/collections` | Show all collections loaded in server |
| GET | `/dimensions/{collection}` | Show all values for each dimension |
| GET | `/metrics/{collection}` | Aggregate across entire retention of server |
| GET | `/metrics/{collection}/{timeBuckets}` | Aggregate in specific time buckets (timeBuckets is CSV list) |
| GET | `/metrics/{collection}/{start}/{end}` | Aggregate across a specific time range (inclusive) |
| POST | `/metrics/{collection}` | Add a new value to collection, which will be reflected in aggregates |

### Tasks

| Method | Route | Description |
|--------|-------|-------------|
| POST | /tasks/gc | Run GC on the server |
| POST | /tasks/restore?collection={collection} | Restore a collection (must be located in `rootDir`) |

### Usage

In order to see all explicitly represented values in the tree, specify the
`rollup=true` query parameter, e.g.

```
GET /dimensions/myCollection?rollup=true
```

Each `GET` method  on `/metrics` allows specific dimension values to be fixed in the query string. E.g.

```
GET /metrics/myCollection/1000/2000?browserName=firefox&countryCode=us
```

A special `!` value means generate queries for all known dimension values, and
a special `?` value means the aggregate value of all dimension values that did
not pass a certain user-defined threshold.

The lack of a value means `*` (i.e. aggregate across all dimension values),
though `*` may be specified explicitly.

The `POST` body should be schemaed in the following way

```
{
    "name": "StarTreeRecord",
    "type": "record",
    "fields": [
        {
            "name": "dimensionValues",
            "type": {
                "type": "map",
                "values": "string"
            }
        },
        {
            "name": "metricValues",
            "type": {
                "type": "map",
                "values": "int"
            }
        },
        {
            "name": "time",
            "type": "long"
        }
    ]
}
```
