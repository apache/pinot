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
package org.apache.pinot.broker.requesthandler;

public class LogicalTableRequests extends BaseSingleStageBrokerRequestHandler.Requests {
  public final LogicalTableUtils.HybridTableName _hybridTableName;

  public LogicalTableRequests(BaseSingleStageBrokerRequestHandler.Requests requests,
      LogicalTableUtils.HybridTableName hybridTableName) {
    super(requests._offlineRoutingTable, requests._realtimeRoutingTable, requests._offlineBrokerRequest,
        requests._realtimeBrokerRequest, requests._numPrunedSegmentsTotal, requests._exceptions);
    _hybridTableName = hybridTableName;
  }
}
