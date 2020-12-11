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
include "query.thrift"

/**
 * AUTO GENERATED: DO NOT EDIT
 * Filter Operator
 **/
enum FilterOperator {
  AND,
  OR,
  EQUALITY,
  NOT,
  RANGE,
  REGEXP_LIKE,
  NOT_IN,
  IN,
  IS_NULL,
  IS_NOT_NULL,
  TEXT_MATCH,
  JSON_MATCH
}

/**
 * AUTO GENERATED: DO NOT EDIT
 *  Query type
 **/
struct QueryType {
  1: optional bool hasSelection;
  2: optional bool hasFilter;
  3: optional bool hasAggregation;
  4: optional bool hasGroup_by;
  5: optional bool hasHaving;
}

/**
 * AUTO GENERATED: DO NOT EDIT
 * Query source
 **/
struct QuerySource {
  1: optional string tableName;
}

/**
 * AUTO GENERATED: DO NOT EDIT
 * Filter query
 **/
struct FilterQuery {
  1: required i32 id; /** This should be unique within a single request **/
  2: optional string column;
  3: list<string> value;
  4: optional FilterOperator operator;
  5: list<i32> nestedFilterQueryIds;
}

/**
 * AUTO GENERATED: DO NOT EDIT
 * Filter Query is nested but thrift stable version does not support yet (The support is there in top of the trunk but no released jars. Two concerns : stability and onus of maintaining a stable point. Also, its pretty difficult to compile thrift in Linkedin software development environment which is not geared towards c++ dev. Hence, the )
 **/
struct FilterQueryMap {
 1: optional map<i32,FilterQuery> filterQueryMap;
}

/**
 * AUTO GENERATED: DO NOT EDIT
 * Having Filter query
 **/
struct HavingFilterQuery {
  1: required i32 id; /** This should be unique within a single request **/
  2: optional AggregationInfo aggregationInfo;
  3: list<string> value;
  4: optional FilterOperator operator;
  5: list<i32> nestedFilterQueryIds;
}

struct HavingFilterQueryMap {
 1: optional map<i32,HavingFilterQuery> filterQueryMap;
}

/**
 * AUTO GENERATED: DO NOT EDIT
 *  Aggregation
 **/
struct AggregationInfo {
  1: optional string aggregationType;
  2: optional map<string,string> aggregationParams;
  3: optional bool isInSelectList;

  // Backward compatible change to allow aggregation functions to take multiple arguments.
  // We could not reuse aggregationParams, as it requires argument name (as key), which may not be
  // available for aggregation functions with variable arguments. Each argument can be an expression.
  4: optional list<string> expressions;

}

/**
 * AUTO GENERATED: DO NOT EDIT
 * GroupBy
 **/
struct GroupBy {
  1: optional list<string> columns;
  2: optional i64 topN;
  3: optional list<string> expressions;
}

/**
 * AUTO GENERATED: DO NOT EDIT
 * selection-sort : specifies how the search results should be sorted.
 * The results can be sorted based on one or multiple columns
 **/
struct SelectionSort {
  1: optional string column;
  2: optional bool isAsc;
}

/**
 * AUTO GENERATED: DO NOT EDIT
 * Selection
 **/
struct Selection {
  1: optional list<string> selectionColumns;
  2: optional list<SelectionSort> selectionSortSequence;
  3: optional i32 offset = 0;
  4: optional i32 size = 10;
}

/**
 * AUTO GENERATED: DO NOT EDIT
 * Broker Query
 **/
struct BrokerRequest {
  1: optional QueryType queryType;
  2: optional QuerySource querySource;
  3: optional string timeInterval;
  4: optional string duration;
  5: optional FilterQuery filterQuery;
  6: optional list<AggregationInfo> aggregationsInfo;
  7: optional GroupBy groupBy;
  8: optional Selection selections;
  9: optional FilterQueryMap filterSubQueryMap;
 10: optional string bucketHashKey;
 11: optional bool enableTrace;
 12: optional string responseFormat;
 13: optional map<string, string> debugOptions;
 14: optional map<string, string> queryOptions;
 15: optional HavingFilterQuery havingFilterQuery;
 16: optional HavingFilterQueryMap havingFilterSubQueryMap;
 17: optional query.PinotQuery pinotQuery;
 18: optional list<SelectionSort> orderBy;
 19: optional i32 limit = 0;
}

/**
 * AUTO GENERATED: DO NOT EDIT
 * Instance Request
 **/
struct InstanceRequest {
  1: required i64 requestId;
  2: required BrokerRequest query;
  3: optional list<string> searchSegments;
  4: optional bool enableTrace;
  5: optional string brokerId;
}
