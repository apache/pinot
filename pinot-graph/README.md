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

# pinot-graph -- Experimental openCypher Support for Apache Pinot

This module provides **experimental** graph query support for Apache Pinot by
translating a minimal openCypher subset into multi-stage SQL (JOINs) against
existing Pinot tables. No native graph operator is introduced; the feature is
implemented entirely as a Cypher-to-SQL transpiler.

## Status

**Experimental.** Gated behind `pinot.graph.enabled=true` in the broker
configuration. Disabled by default.

## Enabling

Add to broker configuration:

```properties
pinot.graph.enabled=true
```

## Supported Query Syntax

Only single-hop MATCH patterns are supported:

```
MATCH (source:Label {prop: value})-[:EDGE_TYPE]->(target:Label)
RETURN target.property [, ...]
[LIMIT n]
```

Supported features:
- **Outgoing edges:** `(a)-[:TYPE]->(b)`
- **Incoming edges:** `(a)<-[:TYPE]-(b)`
- **Undirected edges:** `(a)-[:TYPE]-(b)`
- **Inline property filters:** `(a:User {id: '123'})`
- **Multiple return items:** `RETURN b.id, b.name`
- **LIMIT clause**
- **Named edge aliases:** `-[r:TYPE]->`
- **Case-insensitive keywords**
- **String and integer property values**

## Request Format

`POST /query/graph` with JSON body:

```json
{
  "query": "MATCH (a:User {id: '123'})-[:FOLLOWS]->(b:User) RETURN b.name LIMIT 10",
  "graphSchema": {
    "vertexLabels": {
      "User": {
        "tableName": "users_table",
        "primaryKey": "user_id",
        "properties": {
          "id": "user_id",
          "name": "user_name"
        }
      }
    },
    "edgeLabels": {
      "FOLLOWS": {
        "tableName": "follows_table",
        "sourceVertexLabel": "User",
        "sourceKey": "follower_id",
        "targetVertexLabel": "User",
        "targetKey": "followee_id",
        "properties": {}
      }
    }
  }
}
```

The `graphSchema` maps graph labels to Pinot tables and columns. It is passed
inline with each request (no server-side schema registration).

## Example

Given the request above, the transpiler generates:

```sql
SELECT b.user_name FROM follows_table AS e
  JOIN users_table AS a ON e.follower_id = a.user_id
  JOIN users_table AS b ON e.followee_id = b.user_id
  WHERE a.user_id = '123'
  LIMIT 10
```

This SQL is executed via the multi-stage query engine (`useMultistageEngine=true`).

## Limitations

- Only **1-hop** traversals are supported. Multi-hop and variable-length paths
  are rejected.
- No `WHERE` clause -- use inline property filters `{key: value}` instead.
- No `ORDER BY`, `SKIP`, `DISTINCT`, `WITH`, `UNION`, or `OPTIONAL MATCH`.
- No write operations (`CREATE`, `MERGE`, `DELETE`, `SET`, `REMOVE`).
- No `RETURN *` -- return items must be explicit.
- No aggregation functions in `RETURN`.
- The graph schema must be supplied inline with each request.
- Requires the multi-stage query engine to be available (JOIN support).

## Module Structure

| Module                  | Purpose                                         |
|-------------------------|-------------------------------------------------|
| `pinot-graph-spi`       | Schema config POJOs (`GraphSchemaConfig`)        |
| `pinot-graph-planner`   | Parser, validator, SQL generator, translator     |
