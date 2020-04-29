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
package org.apache.pinot.core.query.request;

import java.util.List;
import java.util.Set;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.core.query.request.context.utils.BrokerRequestToQueryContextConverter;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;


/**
 * The <id>ServerQueryRequest</id> class encapsulates the query related information as well as the query processing
 * context.
 * <p>All segment independent information should be pre-computed and stored in this class to avoid repetitive work on a
 * per segment basis.
 */
public class ServerQueryRequest {
  private final long _requestId;
  private final String _tableNameWithType;
  private final List<String> _segmentsToQuery;
  private final boolean _enableTrace;
  private final String _brokerId;

  // Timing information for different phases of query execution
  private final TimerContext _timerContext;

  // Pre-computed segment independent information
  private final QueryContext _queryContext;
  private final Set<String> _allColumns;

  public ServerQueryRequest(InstanceRequest instanceRequest, ServerMetrics serverMetrics, long queryArrivalTimeMs) {
    _requestId = instanceRequest.getRequestId();
    BrokerRequest brokerRequest = instanceRequest.getQuery();
    _tableNameWithType = brokerRequest.getQuerySource().getTableName();
    _segmentsToQuery = instanceRequest.getSearchSegments();
    _enableTrace = instanceRequest.isEnableTrace();
    _brokerId = instanceRequest.getBrokerId() != null ? instanceRequest.getBrokerId() : "unknown";
    _timerContext = new TimerContext(_tableNameWithType, serverMetrics, queryArrivalTimeMs);

    // Pre-compute segment independent information
    _queryContext = BrokerRequestToQueryContextConverter.convert(brokerRequest);
    _allColumns = QueryContextUtils.getAllColumns(_queryContext);
  }

  public long getRequestId() {
    return _requestId;
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public List<String> getSegmentsToQuery() {
    return _segmentsToQuery;
  }

  public boolean isEnableTrace() {
    return _enableTrace;
  }

  public String getBrokerId() {
    return _brokerId;
  }

  public TimerContext getTimerContext() {
    return _timerContext;
  }

  public QueryContext getQueryContext() {
    return _queryContext;
  }

  public Set<String> getAllColumns() {
    return _allColumns;
  }
}
