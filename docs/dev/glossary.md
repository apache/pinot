---
title: Glossary
summary: One name and one meaning for each term that the developer documentation uses.
verified_at: 5b53f30d80
---

# Glossary

Each term has one meaning in these pages.
If you need a new term, add it here before you use it.

| Term | Meaning |
|---|---|
| SSE | Single-stage query engine. The broker sends one request to each server and merges the results. Older code calls it the "v1 engine". |
| MSE | Multi-stage query engine. The broker splits a query into stages that exchange data through mailboxes. Older docs call it MSQ, MSQE, or the "v2 engine". |
| Stage | A fragment of an MSE plan. The workers of a stage execute the same operators on different data. Code: `PlanFragment`. |
| Root stage | Stage 0 of an MSE plan. It always runs on the broker, with one worker. It receives and merges the final rows. |
| Leaf stage | An MSE stage that scans a table. It runs on servers, and it delegates the scan to the SSE operators. |
| Intermediate stage | An MSE stage that is not the root stage and not a leaf stage. It only reads data from other stages. |
| Worker | One instance of a stage on one Pinot instance. A server can run many workers of the same stage. Code: `QueryServerInstance`. |
| Routing table | The result of routing for one query and one table type: the segments to query on each server. Code: `RoutingTable`. |
| Routing entry | The cached routing state of one table type on the broker. Code: `RoutingEntry` in `BaseBrokerRoutingManager`. |
| Table type | `OFFLINE` or `REALTIME`. A physical table name with its type is `myTable_OFFLINE` or `myTable_REALTIME`. |
| Hybrid table | A raw table name that has both an `OFFLINE` and a `REALTIME` table. |
| Logical table | A table name that maps to a list of physical tables. |
| Time boundary | The time value that splits a hybrid table. The `OFFLINE` table serves rows at or before it. The `REALTIME` table serves rows after it. |
| Enabled server | A server whose instance config does not disable it in Helix, does not mark a shutdown in progress, and does not disable queries. |
| Routable server | An enabled server that the failure detector did not exclude. |
| Replica group | A set of servers that together hold one full copy of a table. |
| Pruning | Removal of segments that cannot match the query filter, before the broker sends the query. |
| Physical optimizer | The opt-in MSE planner path that the `usePhysicalOptimizer` query option enables. It is in beta. Its code is in the `planner.physical.v2` package. Do not confuse it with the "v2 engine" (the MSE). |
