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
# TPC-H queries

This folder has the 22 TPC-H queries in two forms:

- `<n>.sql`: the query text from the TPC-H specification (version 3.0.1), with the default substitution parameters
  (`qgen -d`).
- `rewrites/<n>.sql`: the same query, rewritten for the multi-stage engine. A rewrite returns the same result as the
  specification text. Only the join order and the join distribution change.

`TPCHQueryIntegrationTest` runs both forms and compares the results with H2. The TPC-H quickstart runs some of the
rewrites as examples.

These queries are not a TPC-H benchmark. The specification allows only the changes listed in its clause 2.2.3.3, and
join hints and a different join order are not in that list. Do not publish results of these queries as TPC-H results.

## Changes to the specification text

Pinot stores the dates as strings, and it does not support some of the syntax in the specification. The query text
changes as follows. The result does not change.

- `date 'yyyy-mm-dd'` is the string `'yyyy-mm-dd'`.
- `date 'd' + interval 'n' month` (or `day`, `year`) is the result date as a string. For example, Q1 uses
  `'1998-09-02'` for `date '1998-12-01' - interval '90' day`.
- `extract(year from d)` is `substring(d, 1, 4)`.
- The view of Q15 is a common table expression.
- The row limit of Q2, Q3, Q10, Q18 and Q21 (clause 2.1.2.9) is a `limit` clause.

## Why the rewrites

Pinot has no cost-based optimizer, so the text of the query decides the plan:

- Comma joins are planned left-deep in `FROM` order, `((A ⋈ B) ⋈ C)`. When two adjacent tables have no join
  predicate, you get a cross join. The specification text of Q2 and Q8 does this.
- A hash join always builds on the **right** input and probes with the **left** input.
- A `joinOptions` hint applies to every join in its `SELECT` block. It also flows into nested subqueries that have no
  hint of their own. The nearest hint wins. For this reason, every block in the rewrites has an explicit hint.

## Required table layout

The rewrites mark `part`, `customer`, `supplier`, `nation` and `region` with `tableOptions(is_replicated='true')` and
join them with `left_distribution_type='local', right_distribution_type='local'`. This join runs where the left rows
already are and sends nothing over the network. **It is correct only if every server that hosts
`lineitem`/`orders`/`partsupp` segments has a full copy of each of those tables.** Use
`replication = number of servers`, or `isDimTable: true`, which assigns the table to every server. If a table is not
fully replicated, the results are wrong, and Pinot does not detect it.

`orders` and `lineitem` must be **partitioned and colocated by order key**: segment `p` of both tables holds exactly
the rows whose order key maps to partition `p`, and partition `p` of both tables is on the same server. Q3, Q4, Q5,
Q7, Q8, Q9, Q10, Q12, Q18 and Q21 join them with `joinOptions(is_colocated_by_join_keys='true')`, and Q4, Q18 and Q21
aggregate by order key with `aggOptions(is_partitioned_by_group_by_keys='true')`. Both hints trust the user.
**Without this layout they return wrong results, and Pinot does not detect it.** Without partitioning, use
`left_distribution_type='local', right_distribution_type='broadcast'` for those joins instead, and remove the
`aggOptions` hints.

The hint is needed even with the `inferPartitionHint=true` query option. The planner proves colocation only when both
join inputs are table scans. Here one input is usually the output of a local join with a replicated table (for example
`orders ⋈ customer`), which keeps the partitioning but loses the trait.

`partsupp` stays distributed (replication 1 is fine).

`TPCHQueryIntegrationTest#applyTableLayout` sets up this layout:
- replication = number of servers for the five small tables;
- `segmentPartitionConfig` on `o_orderkey` and `l_orderkey`, and one segment per partition;
- replica-group instance assignment by partition, with an empty `constraintConfig`. Without that config, the
  controller rotates the instance list by a hash of the table name, so the two tables would map a partition to
  different servers.

On a single server (the quickstart), every table is complete on that server, so the layout is always correct.

Do not use `right_distribution_type='broadcast'` on a replicated table. Each replica already scans every row, so a
broadcast multiplies the rows.

## Techniques used

| Technique | Queries |
|-----------|---------|
| Join orders and lineitem partition by partition (colocated by order key) | 3, 4, 5, 7, 8, 9, 10, 12, 18, 21 |
| Aggregate by order key inside each partition (no shuffle) | 4, 18, 21 |
| Replace EXISTS / NOT EXISTS on the same order with one per-order aggregation | 21 |
| Write the conditions implied by every OR branch outside the OR, so they filter the scans | 19 |
| Local join against replicated small tables (no exchange) | 2, 3, 5, 7, 8, 9, 10, 11, 15, 16, 17, 18, 19, 21 |
| Reduce with the fact join first, then join the small tables | 5, 7, 10, 18 |
| Change a correlated MIN/AVG/SUM subquery into a window over the rows that are already joined | 2, 11, 15, 17 |
| Aggregate before the join (the grouped columns depend only on the join key) | 10, 13, 18 |
| Put an `IN` semi join directly over the scan it filters. It keeps the default dynamic broadcast, which pushes the key set into the leaf filter. Do not give that block a join hint. | 20 |

Q1, Q6, Q14 and Q22 are the same as the specification text.

The comment at the top of each rewrite explains the change. The row counts in those comments are for scale factor 1,
on 2 servers.

The rewrites use `join_strategy='lookup'` (dimension tables) nowhere:
1. Each lookup join adds a stage and a local mailbox hop. Over a large scan, this was 4–6× slower than reducing first
   and then joining the replicated table locally.
2. When the left input is an intermediate stage (for example, the output of another join), the planner fails with an
   NPE in `WorkerManager`. Workers are assigned to leaf fragments before intermediate ones, and a lookup-join fragment
   counts as a leaf.
3. Filters on the dimension side are not pushed below the lookup.

## Query options

Run the rewrites with `SET inferPartitionHint=true`. On large data sets, also set a high `numGroupsLimit` (for example
`SET numGroupsLimit=1000000000`). With the default limit of 100k, the specification text of Q2, Q17, Q18 and Q20
reaches the limit at scale factor 1 and returns truncated results.
