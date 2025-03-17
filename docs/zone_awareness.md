# Zone Awareness in Apache Pinot

This document describes how to use zone awareness features in Apache Pinot for improved reliability and performance.

## Overview

Zone awareness in Pinot refers to the ability of the system to understand and utilize the physical deployment topology (e.g., AWS availability zones) when making decisions about:

1. **Segment Assignment** - How segment replicas are distributed across servers
2. **Query Routing** - How queries are routed from brokers to servers

These features are particularly useful in cloud environments where infrastructure is distributed across multiple availability zones for high availability.

## Benefits

* **Improved Reliability**: By distributing segment replicas across zones, your Pinot cluster can survive the loss of an entire availability zone.
* **Better Performance**: By routing queries to servers in the same zone as the broker, network latency is reduced.
* **Cost Efficiency**: Cross-zone traffic is often billed at a higher rate than intra-zone traffic in cloud environments.

## Configuration

### 1. Instance Configuration

For zone-aware features to work, Pinot instances need to be configured with their zone information:

```
# In server/broker config
environment.identifier.failureDomain=us-west-2a
```

The environment identifier map is stored in the Helix InstanceConfig for each instance.

### 2. Zone-Aware Segment Assignment

To use zone-aware segment assignment, you need to configure your table with the custom segment assignment strategy:

```json
{
  "tableName": "myTable",
  "segmentsConfig": {
    "replication": "3"
  },
  "tableIndexConfig": {
    ...
  },
  "tenants": {
    ...
  },
  "metadata": {
    ...
  },
  "routing": {
    "segmentAssignmentStrategy": "zoneAware"
  }
}
```

This strategy ensures that segment replicas are distributed across different availability zones whenever possible, providing fault tolerance against zone failures.

### 3. Zone-Aware Query Routing

To use zone-aware query routing, configure your table with the zone-aware instance selector:

```json
{
  "tableName": "myTable",
  ...
  "routing": {
    "instanceSelectorType": "zoneAware"
  }
}
```

#### Broker Configuration Properties

You can further customize zone-aware routing with the following broker properties:

* `pinot.broker.routing.sameZonePreference`: A value between 0.0 and 1.0 controlling the preference for same-zone servers (default: 0.5)
* `pinot.broker.routing.strictZoneMatch`: If true, queries will fail if the same-zone preference can't be met (default: false)
* `pinot.broker.zone`: Override for the broker's zone (by default, the broker uses the environment.identifier.failureDomain)

Example broker config:

```properties
pinot.broker.routing.sameZonePreference=0.8
pinot.broker.routing.strictZoneMatch=false
```

With a sameZonePreference of 0.8, the broker will try to route 80% of segment queries to same-zone servers.

## Monitoring

Pinot exposes the following metrics for monitoring zone-aware routing:

* `sameZoneServerPercentage`: A gauge showing the percentage of queries routed to same-zone servers (0-100)
* `zoneAwareRoutingFailures`: Counter of queries that failed due to strict zone matching requirements

These metrics can be monitored through the broker metrics endpoint or through your metrics collection system.

## Limitations

* Zone-aware segment assignment requires a sufficient number of servers per zone to satisfy the replication factor.
* If `strictZoneMatch` is enabled and no same-zone servers are available, queries will fail.
* In multi-tenant clusters, ensure each tenant has servers in multiple zones.

## Example Deployment

A typical deployment with zone awareness might look like:

```
Zone 1 (us-west-2a):
- 3 Pinot Servers
- 1 Pinot Broker

Zone 2 (us-west-2b):
- 3 Pinot Servers 
- 1 Pinot Broker

Zone 3 (us-west-2c):
- 3 Pinot Servers
- 1 Pinot Broker
```

With this setup and a replication factor of 3, each segment would have one replica in each availability zone, providing both fault tolerance and local query performance.