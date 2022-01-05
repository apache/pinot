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

struct DataSource {
  1: optional string tableName;
}

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
  10: optional map<string, string> debugOptions;
  11: optional map<string, string> queryOptions;
  12: optional bool explain;
}

enum ExpressionType {
  LITERAL,
  IDENTIFIER,
  FUNCTION
}

struct Expression {
  1: required ExpressionType type;
  2: optional Function functionCall;
  3: optional Literal literal;
  4: optional Identifier identifier;
}

struct Identifier {
  1: required string name;
}

union Literal {
  1: optional bool boolValue;
  2: optional byte byteValue;
  3: optional i16 shortValue;
  4: optional i32 intValue;
  5: optional i64 longValue;
  6: optional double doubleValue;
  7: optional string stringValue;
  8: optional binary binaryValue;
}

struct Function {
  1: required string operator;
  2: optional list<Expression> operands;
}
