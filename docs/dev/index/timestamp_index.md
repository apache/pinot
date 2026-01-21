# Timestamp indexes

Timestamp indexes are a key feature in Pinot, but they are very different from other indexes.
Although they are called indexes in the user documentation, some committers call them "syntactic sugar" because they
are not indexes in the codebase.
Instead, when the user configures a timestamp _index_ in their TableConfig:

1. A new column is created for each cardinality of the timestamp _index_ (one for days, one for months, etc).
2. A range index is created for each of these columns.
3. Whenever a query is received, _the broker_ rewrites the query to use these columns instead of the original
   timestamp column if the query has a filter using one of the cardinalities.

Some of these steps are described in the [timestamp page of the user documentation][timestamp-index], but not all of
them.

[timestamp-index]:https://docs.pinot.apache.org/basics/indexing/timestamp-index

Specifically, there is one key point on timestamp _indexes_.
All other column indexes optimize queries at the segment level (in the servers) by changing the way `FilterPlanNode` are
transformed into different Operators (in `FilterPlanNode.constructPhysicalOperator`).
Meanwhile, timestamp _indexes_ optimize queries at the broker level (as explained above).
The broker analyzes the query to look for all usages of the original column that can be optimized.
For example if there is a timestamp index on `event_time` that includes the `YEAR` granularity,
the broker marks in the meta-information that any call to `dateTrunc('YEAR', event_time)` can be rewritten as
`$event_time$YEAR`.
Brokers do this `BaseSingleStageBrokerRequestHandler.handleExpressionOverride()`, setting the `expressionOverrideMap`
attribute of `QueryConfig`.

Then the server that receives the query verifies that the column `$event_time$YEAR` exists in the segment
(remember that the segment may not be updated to the latest table config!) and if it does, it rewrites the query
before `FilterPlanNode.constructPhysicalOperator` is called.
Servers do this when building `TableCache.TableConfigInfo`, which obtains the information from
`QueryConfig.getExpressionOverrideMap()`

At least this is how it works in Single-stage query engine (SSQ).
In Multi-stage query engine (MSQ) the broker doesn't rewrite the query and therefore timestamp indexes are not used
(ie https://github.com/apache/pinot/pull/11409 tried to add support for it).

How do we add these new cardinality columns?
They are added in `TimestampIndexUtils.applyTimestampIndex`, which modifies the schema and the table config of the
table.

Do we store the modified schema and table config somewhere?
No, it is not stored in Zookeeper nor in the segment metadata.
Therefore it is very important for developers to know there are two kinds of Schema and TableConfig objects:

* The ones that are not enriched. They are the ones that are persisted and shown to the users.
* The ones that are used at runtime. They are the ones that have been enriched with the timestamp indexes.

And given the typesystem doesn't help (we don't have EnrichedSchema and EnrichedTableConfig classes), developers
need to know when they are working with one or the other.