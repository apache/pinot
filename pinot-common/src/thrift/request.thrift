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

struct TableSegmentsInfo {
  1: required string tableName;
  2: required list<string> segments;
  3: optional list<string> optionalSegments;
}

struct InstanceRequest {
  1: required i64 requestId;
  2: required BrokerRequest query;
  3: optional list<string> searchSegments;
  4: optional bool enableTrace;
  5: optional string brokerId;
  6: optional list<string> optionalSegments;
  7: optional string cid;
  8: optional list<TableSegmentsInfo> tableSegmentsInfoList;
}
