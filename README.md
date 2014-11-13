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

Config
------

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

### Record store

Each `(dimensions, time, metrics)` tuple is referred to as a "record" here.

There are two record store implementations for different use cases:

`com.linkedin.thirdeye.impl.StarTreeRecordStoreByteBufferImpl`, which is a dynamically growing buffer for records, on which periodic compaction is performed. This should be used when the dimension combinations are unknown, such as during tree bootstrap, or for ad hoc use cases.

`com.linkedin.thirdeye.impl.StarTreeRecordStoreCircularBufferImpl`, which is a fixed circular buffer (i.e. has a fixed set of dimension combinations and time buckets). This should be used when the star-tree index structure is built offline then loaded.

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
