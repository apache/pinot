/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
namespace java org.apache.pinot.common.request

struct PinotQuery {
  1: optional i32 version;
  2: optional DataSource dataSource;
  3: optional list<Expression> selectList;
  4: optional Expression filterExpression;
  5: optional list<Expression> groupByList;
  6: optional list<Expression> orderByList;
  7: optional Expression havingExpression;
  8: optional i32 limit = 10;
  9: optional i32 offset = 0;
//10: optional map<string, string> debugOptions;
  11: optional map<string, string> queryOptions;
  12: optional bool explain;
  13: optional map<Expression, Expression> expressionOverrideHints;
//14: retired before release (was list<i32> groupingSetMasks, a bitmask encoding capped at 31 grouping columns);
//    do not reuse the id.
  // GROUP BY GROUPING SETS / ROLLUP / CUBE: one entry per grouping set, in query order; each entry lists the
  // indexes into groupByList (the ordered union of all grouping columns) that participate in (are grouped by)
  // that set. An empty inner list is the grand-total set (). A set's position in this list is its ordinal, which
  // the engine carries as the synthetic $groupingId discriminator key, so the number of grouping columns is
  // unlimited (mirroring Calcite's per-set column bitset). Unset for a plain GROUP BY query.
  // Rolling upgrade: a server that predates this field ignores it and runs a plain GROUP BY, returning results
  // without the $groupingId column the reducer expects. The reduce path reconciles positionally (by column type,
  // not name), so this is NOT rejected cleanly — it surfaces as an opaque error or as silently collapsed/wrong
  // results (missing subtotal/grand-total rows). Upgrade all servers before issuing grouping-set queries.
  15: optional list<list<i32>> groupingSets;
}

struct DataSource {
  1: optional string tableName;
  2: optional PinotQuery subquery;
  3: optional Join join;
}

struct Join {
  1: required JoinType type;
  2: required DataSource left;
  3: required DataSource right;
  4: optional Expression condition;
}

enum JoinType {
  INNER,
  LEFT,
  RIGHT,
  FULL
}

struct Expression {
  1: required ExpressionType type;
  2: optional Function functionCall;
  3: optional Literal literal;
  4: optional Identifier identifier;
}

enum ExpressionType {
  LITERAL,
  IDENTIFIER,
  FUNCTION
}

union Literal {
  1: optional bool boolValue;
  2: optional i8 byteValue;
  3: optional i16 shortValue;
  4: optional i32 intValue;
  5: optional i64 longValue;
  6: optional double doubleValue;
  7: optional string stringValue;
  8: optional binary binaryValue;
  // Set to true when the literal value is a null.
  9: optional bool nullValue;
  10: optional binary bigDecimalValue;
  // Use i32 to represent float since there is no native float type
  11: optional i32 floatValue;
  12: optional list<i32> intArrayValue;
  13: optional list<i64> longArrayValue;
  14: optional list<i32> floatArrayValue;
  15: optional list<double> doubleArrayValue;
  16: optional list<string> stringArrayValue;
}

struct Identifier {
  1: required string name;
}

struct Function {
  1: required string operator;
  2: optional list<Expression> operands;
}
