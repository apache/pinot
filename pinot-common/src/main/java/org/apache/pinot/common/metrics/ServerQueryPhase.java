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
package org.apache.pinot.common.metrics;

import org.apache.pinot.common.Utils;


/**
 * Enumeration containing all the query phases executed by the server.
 *
 */
public enum ServerQueryPhase implements AbstractMetrics.QueryPhase {
  REQUEST_DESERIALIZATION,
  TOTAL_QUERY_TIME,
  SEGMENT_PRUNING,
  BUILD_QUERY_PLAN,
  QUERY_PLAN_EXECUTION,
  RESPONSE_SERIALIZATION,
  QUERY_PROCESSING,
  SCHEDULER_WAIT;

  // NOTE: update query.context.TimerContext toString() method if you
  // time more phases of query execution
  private final String _queryPhaseName;

  ServerQueryPhase() {
    _queryPhaseName = Utils.toCamelCase(name().toLowerCase());
  }

  @Override
  public String getQueryPhaseName() {
    return _queryPhaseName;
  }
}
