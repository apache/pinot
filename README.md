thirdeye
========

Thirdeye is a system for efficient monitoring of and drill-down into business metrics.

API
---

| Method | Route | Description |
|--------|-------|-------------|
| GET    | `/collections` | Show all collections in server |
| GET    | `/metrics/{collection}` | Aggregate metrics for collection |
| GET    | `/dimensions/{collection}` | Show all explicit values for each dimension |
| GET    | `/configs/{collection}` | Get star-tree configuration for a collection |
| POST   | `/metrics` | Add new record to collection |
| POST   | `/configs` | Add a new star-tree configuration for a collection |
| POST   | `/bootstrap` | Load data from some source into server |

### Metrics

The `/metrics` resource accepts URI query parameters to narrow the scope of the query.

The following are special query parameters reserved for the time dimension:

| Name | Description |
|------|-------------|
| `__BETWEEN__` | A string in the format `{begin},{end}` defining an time range (inclusive) |
| `__IN__` | A CSV list of discrete time buckets |

The rest of query parameters are interpreted as dimension names.

For example, consider a data set `myCollection` that has dimensions `A`, `B`, `C`. One might execute the following query:

```
GET /metrics/myCollection?A=someValue&__BETWEEN__=1000,2000
```

Dimension values can take three special values as well:

| Name | Description |
|------|-------------|
| `*`  | Any value   |
| `?`  | Other value (determined via roll-up algorithm) |
| `!`  | All explicit values (includes `?` but not constitutent values of `?` |

If a dimension is left unspecified in the query string, `*` is assumed. If neither time constraint is specified, the aggregate is performed across all time.

A more concrete example:

```
>> curl -X GET -s 'http://localhost:8080/metrics/abook?isSuccess=!&browserName=chrome'
[
    {
        "dimensionValues": {
            "browserName": "chrome", 
            "countryCode": "*", 
            "deviceName": "*", 
            "emailDomain": "*", 
            "environment": "*", 
            "errorStatus": "*", 
            "isSuccess": "true", 
            "locale": "*", 
            "source": "*"
        }, 
        "metricValues": {
            "numberOfGuestInvitationsSent": 84129, 
            "numberOfImportedContacts": 219584, 
            "numberOfMemberConnectionsSent": 99804, 
            "numberOfSuggestedGuestInvitations": 433124, 
            "numberOfSuggestedMemberConnections": 217685
        }
    }, 
    {
        "dimensionValues": {
            "browserName": "chrome", 
            "countryCode": "*", 
            "deviceName": "*", 
            "emailDomain": "*", 
            "environment": "*", 
            "errorStatus": "*", 
            "isSuccess": "false", 
            "locale": "*", 
            "source": "*"
        }, 
        "metricValues": {
            "numberOfGuestInvitationsSent": 0, 
            "numberOfImportedContacts": 0, 
            "numberOfMemberConnectionsSent": 0, 
            "numberOfSuggestedGuestInvitations": 0, 
            "numberOfSuggestedMemberConnections": 0
        }
    }
]
```

### Configs

In order to use the system, one must specify a minimal config that contains

* Collection name;
* Dimension names; and
* Metrics names

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
    ]
}
```

The optional `maxRecordStoreEntries` controls how many records are allowed in a leaf node before a split is required. The default is 100000.

In addition to this, one can specify a threshold function and it's configuration. The following example shows use of an absolute threshold function that requires `numberOfMemberConnectionsSent` to be greater than or equal to 250 for a record not to be eligible for roll-up:

```
{
    ...,
    "thresholdFunctionClass": 
        "com.linkedin.thirdeye.impl.StarTreeRecordThresholdFunctionAbsImpl",
    "thresholdFunctionConfig": {
        "numberOfMemberConnectionsSent": "250"
    }
}
```

One can similarly specify the record store implementation on a per-collection basis:

```
{
    ...,
    "recordStoreFactoryClass": 
        "com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryByteBufferImpl",
    "recordStoreFactoryConfig": {
        "useDirect": "false",
        "targetCompressionRatio": "0.5"
    }
}
```

If a `recordStoreFactoryClass` is not specified, the `ByteBuffer`-based implementation is used with direct buffers.

### Bootstrap

In order to bootstrap data for a collection, one can specify a URI pointing to a TSV file in the following format:

```
{dimensionValues} {metricValues} {time}
```

For example, consider a collection `myCollection` with dimensions `A`, `B`, `C` and metric `M`. We could have a TSV file that looked something like this:

```
A1  B1  C1  10  1234
A2  B2  C2  11  1234
A1  B3  C3  12  1235
A2  B4  C1  13  1235
...
```

To load that data, execute a `POST` request to the `/bootstrap` resource with the following payload (this assumes data file is on same machine as server, but adjust URI appropriately):

```
{
    "collection": "myCollection",
    "uri": "file:///tmp/myData.tsv"
}
```
