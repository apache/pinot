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
# Polymorphic aggregation binding

A polymorphic aggregate resolves its logical result type from its input expressions before execution.
SSE and MSE use the `SqlReturnTypeInference` registered in `AggregationFunctionType`; the runtime receives
an immutable `AggregateCallBinding` rather than synthetic SQL arguments.

## SQL behavior

```sql
SELECT MODE(name), MODE(eventTime), MODE(enabled) FROM events;
SELECT FIRST_WITH_TIME(name, eventTime), LAST_WITH_TIME(enabled, eventTime) FROM events;
SELECT MODE(CAST(value AS STRING)) FROM events;
SELECT ANY_VALUE(enabled), ARRAY_AGG(eventTime), ARRAY_AGG(name, true) FROM events;
```

- MODE preserves STRING, TIMESTAMP and BOOLEAN input types. MIN is the default tie reducer; MAX is also supported.
- Numeric MODE retains the existing DOUBLE result, including the AVG tie reducer. AVG is rejected for nonnumeric
  inputs. DECIMAL and multi-value MODE inputs are unsupported.
- Two-argument FIRST_WITH_TIME and LAST_WITH_TIME preserve the first argument's type: INT, LONG, FLOAT, DOUBLE,
  STRING, TIMESTAMP or BOOLEAN. Their ordering argument must be INT, LONG or TIMESTAMP.
- Existing three-argument FIRST_WITH_TIME/LAST_WITH_TIME calls retain their explicit type behavior.
- Bound nonnumeric MODE returns NULL when no value is aggregated. Numeric MODE retains its existing null-handling
  and empty-input behavior.
- ANY_VALUE preserves supported scalar input types, including BOOLEAN, TIMESTAMP, exact LONG, DECIMAL, BYTES and
  UUID. JSON inputs use STRING results, as in the SQL type adapter. Its result schema is available on empty input.
- ARRAY_AGG infers its array element type from a scalar or multi-value expression. The optional second argument is
  a boolean literal selecting distinct values. Existing `ARRAY_AGG(expr, 'TYPE'[, distinct])` forms remain supported.
- EXPR_MIN/EXPR_MAX's existing SSE parent/child rewrite preserves logical measuring and projection schemas,
  including BOOLEAN, TIMESTAMP and multi-value projections, through merges and empty responses. This does not add
  public EXPR_MIN/EXPR_MAX execution to MSE; its existing SSE query rewriter is still required.
- Aggregates with fixed numeric contracts, such as SUM and numeric MIN/MAX, retain their existing behavior.

## Adding another polymorphic aggregate

1. Register its argument validation and `SqlReturnTypeInference` in `AggregationFunctionType`, and opt its applicable
   overloads into `isTypeBindingRequired`. The rule may depend on argument types and literal options; it need not
   return the first argument's type. Array result types are supported by the common type adapter. Use the overload
   accepting `IntPredicate isStringLiteral` when an existing explicit-type form must remain unbound.
2. Implement `AggregationFunctionProvider` and register its class in
   `META-INF/services/org.apache.pinot.core.query.aggregation.function.AggregationFunctionProvider` on the runtime
   classpath. The provider receives the original `FunctionContext`, including its optional binding, and creates the
   appropriate kernel. Choose and validate its final logical type at construction; do not discover or mutate types
   while scanning blocks. Providers must be stateless and thread-safe. Duplicate registrations are rejected.
3. Keep the intermediate accumulator type and serialization separate. MODE uses a frequency map and FIRST/LAST
   use value/time pairs; their intermediate OBJECT column does not determine the final SQL type.
4. Test input expressions, null/empty inputs, dictionary/raw data, intermediate merges, server-final responses and
   logical BOOLEAN/TIMESTAMP rendering. A type rule does not itself add kernel or serialization support for a type.

The factory discovers registered providers once and verifies that each created aggregate preserves its bound final
type. Adding another enum-registered polymorphic aggregate requires no new factory switch case, planner stage
branch, or reducer branch. The registry does not introduce arbitrary SQL function names or replace existing UDAF
registration. Legacy unbound Java constructors and requests can retain their previous type-discovery behavior.
Aggregates that already supported unbound calls opt into `supportsLegacyUnboundCalls` to keep that path for native
inputs without schema-only metadata, such as LOOKUP or custom transforms. Only missing type metadata enables this
fallback; invalid expressions and unsupported aggregate types still fail. New inferred FIRST/LAST and ARRAY_AGG
overloads require resolved types.
Bound implementations read physical blocks through conversion getters for their bound stored type, so a table's
LONG schema can read older INT segments without publishing Integer values under a LONG transport schema.

ExprMin/Max serialization also preserves null projections and all tied rows from merged servers. Previously the
first serialized input's tied rows could be dropped when converting its accumulator back to mutable form.

`AggregationFunctionBinder` attaches metadata to the executable SSE request after schema acquisition and query
rewrites. Bindings preserve the original logical contract when an expression override uses an INT/LONG storage
column for BOOLEAN/TIMESTAMP. Override matching ignores binding metadata, and new aggregate calls introduced by
an override are bound against the schema. `ExpressionTypeResolver` follows native SSE transform precedence, then typed scalar overload lookup.
Native transforms must supply a schema-only inference rule for new inferred overloads. Existing MODE, ANY_VALUE,
and ExprMin/Max calls retain their legacy unbound execution when that metadata is unavailable. This resolver never
constructs segment transforms on a broker.

MSE preserves the binding from the original aggregate input through stage splitting, Rex/protobuf serialization,
and SSE leaf conversion. A FINAL-stage input may be OBJECT; it must use the retained original binding rather than
infer a scalar result from that accumulator. Broker reduction reconstructs the same binding from the request.
Gapfill retains inner bindings before expression overrides and resolves outer aliases from the actual inner result
schema, including empty results. Direct server SQL defers aggregate construction until the executor obtains the table schema, before pruning.

SQL names, aliases and operands remain unchanged. FunctionContext equality intentionally ignores execution binding
so aggregation indexing and post-aggregation expression matching retain the original expression identity.

## Version compatibility

Binding metadata is optional in both request Thrift and MSE plan protobuf. Old requests and existing explicitly
typed FIRST/LAST and ARRAY_AGG calls remain supported; numeric MODE retains its response contract. The new inferred
overloads and logical type preservation require upgraded brokers and execution workers. Older nodes ignore the metadata and do
not implement these semantics, so new functionality must not be used during a mixed-version rollout. Adding optional
metadata does not make older aggregate implementations understand new types.
