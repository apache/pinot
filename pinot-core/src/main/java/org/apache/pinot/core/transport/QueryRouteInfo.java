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
package org.apache.pinot.core.transport;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;


public class QueryRouteInfo {
  public final static QueryRouteInfo EMPTY =
      new QueryRouteInfo(null, -1, null, null, null, null, null, null, null, null, null, 0, -1, null);

  protected final String _brokerId;
  protected final long _requestId;
  private final String _rawTableName;
  private BrokerRequest _originalBrokerRequest;
  protected final BrokerRequest _serverBrokerRequest;
  private final String _offlineTableName;
  private final BrokerRequest _offlineBrokerRequest;
  private final Map<ServerInstance, ServerRouteInfo> _offlineRoutingTable;
  private final String _realtimeTableName;
  private final BrokerRequest _realtimeBrokerRequest;
  private final Map<ServerInstance, ServerRouteInfo> _realtimeRoutingTable;
  protected int _numPrunedSegments;
  private final long _timeoutMs;
  private final RequestContext _requestContext;

  public QueryRouteInfo(String brokerId, long requestId, String rawTableName, BrokerRequest originalBrokerRequest,
      BrokerRequest serverBrokerRequest, String offlineTableName, BrokerRequest offlineBrokerRequest,
      Map<ServerInstance, ServerRouteInfo> offlineRoutingTable, String realtimeTableName,
      BrokerRequest realtimeBrokerRequest, Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable,
      int numPrunedSegments, long timeoutMs, RequestContext requestContext) {

    _brokerId = brokerId;
    _requestId = requestId;
    _rawTableName = rawTableName;
    _originalBrokerRequest = originalBrokerRequest;
    _serverBrokerRequest = serverBrokerRequest;
    _offlineTableName = offlineTableName;
    _offlineBrokerRequest = offlineBrokerRequest;
    _offlineRoutingTable = offlineRoutingTable;
    _realtimeTableName = realtimeTableName;
    _realtimeBrokerRequest = realtimeBrokerRequest;
    _realtimeRoutingTable = realtimeRoutingTable;
    _numPrunedSegments = numPrunedSegments;
    _timeoutMs = timeoutMs;
    _requestContext = requestContext;
  }

  public QueryRouteInfo(long requestId, String rawTableName,
      BrokerRequest offlineBrokerRequest,
      Map<ServerInstance, ServerRouteInfo> offlineRoutingTable,
      BrokerRequest realtimeBrokerRequest,
      Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable, long timeoutMs) {
    this(null, requestId, rawTableName, null, null, null, offlineBrokerRequest, offlineRoutingTable, null,
        realtimeBrokerRequest, realtimeRoutingTable, 0, timeoutMs, null);
  }

  // Getters and setters for each member variable
  public long getRequestId() {
    return _requestId;
  }

  public String getRawTableName() {
    return _rawTableName;
  }

  public BrokerRequest getOriginalBrokerRequest() {
    return _originalBrokerRequest;
  }

  public void setOriginalBrokerRequest(BrokerRequest originalBrokerRequest) {
    _originalBrokerRequest = originalBrokerRequest;
  }

  public BrokerRequest getServerBrokerRequest() {
    return _serverBrokerRequest;
  }

  public String getOfflineTableName() {
    return _offlineTableName;
  }

  public BrokerRequest getOfflineBrokerRequest() {
    return _offlineBrokerRequest;
  }

  public Map<ServerInstance, ServerRouteInfo> getOfflineRoutingTable() {
    return _offlineRoutingTable;
  }

  public String getRealtimeTableName() {
    return _realtimeTableName;
  }

  public BrokerRequest getRealtimeBrokerRequest() {
    return _realtimeBrokerRequest;
  }

  public Map<ServerInstance, ServerRouteInfo> getRealtimeRoutingTable() {
    return _realtimeRoutingTable;
  }

  public int getNumPrunedSegments() {
    return _numPrunedSegments;
  }

  public long getTimeoutMs() {
    return _timeoutMs;
  }

  public RequestContext getRequestContext() {
    return _requestContext;
  }

  public Map<ServerRoutingInstance, InstanceRequest> getRequestMap(boolean preferTls) {
    Map<ServerRoutingInstance, InstanceRequest> requestMap = new HashMap<>();
    if (_offlineBrokerRequest != null) {
      assert _offlineRoutingTable != null;
      for (Map.Entry<ServerInstance, ServerRouteInfo> entry : _offlineRoutingTable.entrySet()) {
        ServerRoutingInstance serverRoutingInstance =
            entry.getKey().toServerRoutingInstance(TableType.OFFLINE, preferTls);
        InstanceRequest instanceRequest = getInstanceRequest(_offlineBrokerRequest, entry.getValue());
        requestMap.put(serverRoutingInstance, instanceRequest);
      }
    }
    if (_realtimeBrokerRequest != null) {
      assert _realtimeRoutingTable != null;
      for (Map.Entry<ServerInstance, ServerRouteInfo> entry : _realtimeRoutingTable.entrySet()) {
        ServerRoutingInstance serverRoutingInstance =
            entry.getKey().toServerRoutingInstance(TableType.REALTIME, preferTls);
        InstanceRequest instanceRequest = getInstanceRequest(_realtimeBrokerRequest, entry.getValue());
        requestMap.put(serverRoutingInstance, instanceRequest);
      }
    }

    return requestMap;
  }

  private InstanceRequest getInstanceRequest(BrokerRequest brokerRequest, ServerRouteInfo segments) {
    InstanceRequest instanceRequest = new InstanceRequest();
    instanceRequest.setRequestId(_requestId);
    instanceRequest.setCid(QueryThreadContext.getCid());
    instanceRequest.setQuery(brokerRequest);
    Map<String, String> queryOptions = brokerRequest.getPinotQuery().getQueryOptions();
    if (queryOptions != null) {
      instanceRequest.setEnableTrace(Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.TRACE)));
    }
    instanceRequest.setSearchSegments(segments.getSegments());
    instanceRequest.setBrokerId(_brokerId);
    if (CollectionUtils.isNotEmpty(segments.getOptionalSegments())) {
      // Don't set this field, i.e. leave it as null, if there is no optional segment at all, to be more backward
      // compatible, as there are places like in multi-stage query engine where this field is not set today when
      // creating the InstanceRequest.
      instanceRequest.setOptionalSegments(segments.getOptionalSegments());
    }
    return instanceRequest;
  }
}
