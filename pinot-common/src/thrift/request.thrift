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

struct QuerySource {
  1: optional string tableName;
}

struct BrokerRequest {
//  1: optional QueryType queryType;
  2: optional QuerySource querySource;
//  3: optional string timeInterval;
//  4: optional string duration;
//  5: optional FilterQuery filterQuery;
//  6: optional list<AggregationInfo> aggregationsInfo;
//  7: optional GroupBy groupBy;
//  8: optional Selection selections;
//  9: optional FilterQueryMap filterSubQueryMap;
// 10: optional string bucketHashKey;
// 11: optional bool enableTrace;
// 12: optional string responseFormat;
// 13: optional map<string, string> debugOptions;
// 14: optional map<string, string> queryOptions;
// 15: optional HavingFilterQuery havingFilterQuery;
// 16: optional HavingFilterQueryMap havingFilterSubQueryMap;
 17: optional query.PinotQuery pinotQuery;
// 18: optional list<SelectionSort> orderBy;
// 19: optional i32 limit = 0;
}

struct InstanceRequest {
  1: required i64 requestId;
  2: required BrokerRequest query;
  3: optional list<string> searchSegments;
  4: optional bool enableTrace;
  5: optional string brokerId;
  6: optional list<string> optionalSegments;
  7: optional string cid;
}
