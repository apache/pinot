ThirdEye
========

ThirdEye is a system for efficient monitoring of and drill-down into business
metrics.

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

### Collections

| Method | Route | Description |
|--------|-------|-------------|
| GET | `/collections` | Show all collections loaded in server |

### Dimensions

| Method | Route | Description |
|--------|-------|-------------|
| GET | `/dimensions/{collection}` | Show all values for each dimension |

In order to see all explicitly represented values in the tree, specify the
`rollup=true` query parameter, e.g.

```
GET /dimensions/myCollection?rollup=true
```

### Metrics

| Method | Route | Description |
|--------|-------|-------------|
| GET | `/metrics/{collection}` | Aggregate across entire retention of server |
| GET | `/metrics/{collection}/{timeBuckets}` | Aggregate in specific time buckets (timeBuckets is CSV list) |
| GET | `/metrics/{collection}/{start}/{end}` | Aggregate across a specific time range (inclusive) |
| POST | `/metrics/{collection}` | Add a new value to collection, which will be reflected in aggregates |

Each `GET` method allows specific dimension values to be fixed in the query string. E.g.

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
