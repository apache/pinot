package org.apache.pinot.broker.requesthandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.TableRouteInfo;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.QueryRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;


public class LogicalQueryRouteInfo extends QueryRouteInfo {
  private final List<QueryRouteInfo> _routeInfos;

  public LogicalQueryRouteInfo(String brokerId, long requestId, String rawTableName,
      BrokerRequest originalBrokerRequest, BrokerRequest serverBrokerRequest, List<QueryRouteInfo> routeInfos,
      int numPrunedSegments, long timeoutMs, RequestContext requestContext) {
    super(brokerId, requestId, rawTableName, originalBrokerRequest, serverBrokerRequest, rawTableName,
        serverBrokerRequest, null, rawTableName, serverBrokerRequest, null, numPrunedSegments, timeoutMs,
        requestContext);
    _routeInfos = routeInfos;
    _numPrunedSegments = 0;
  }

  @Override
  public Map<ServerRoutingInstance, InstanceRequest> getRequestMap(boolean preferTls) {
    Map<ServerInstance, List<TableRouteInfo>> offlineTableRouteInfo = new HashMap<>();
    Map<ServerInstance, List<TableRouteInfo>> realtimeTableRouteInfo = new HashMap<>();

    for (QueryRouteInfo routeInfo : _routeInfos) {
      if (routeInfo.getOfflineRoutingTable() != null) {
        for (Map.Entry<ServerInstance, ServerRouteInfo> entry : routeInfo.getOfflineRoutingTable().entrySet()) {
          TableRouteInfo tableRouteInfo = new TableRouteInfo();
          tableRouteInfo.setTableName(routeInfo.getOfflineTableName());
          tableRouteInfo.setSegments(entry.getValue().getSegments());
          if (CollectionUtils.isNotEmpty(entry.getValue().getOptionalSegments())) {
            tableRouteInfo.setOptionalSegments(entry.getValue().getOptionalSegments());
          }

          offlineTableRouteInfo.computeIfAbsent(entry.getKey(), v -> new ArrayList<>()).add(tableRouteInfo);
        }
      }

      if (routeInfo.getRealtimeRoutingTable() != null) {
        for (Map.Entry<ServerInstance, ServerRouteInfo> entry : routeInfo.getRealtimeRoutingTable().entrySet()) {
          TableRouteInfo tableRouteInfo = new TableRouteInfo();
          tableRouteInfo.setTableName(routeInfo.getRealtimeTableName());
          tableRouteInfo.setSegments(entry.getValue().getSegments());
          if (CollectionUtils.isNotEmpty(entry.getValue().getOptionalSegments())) {
            tableRouteInfo.setOptionalSegments(entry.getValue().getOptionalSegments());
          }

          realtimeTableRouteInfo.computeIfAbsent(entry.getKey(), v -> new ArrayList<>()).add(tableRouteInfo);
        }
      }

      _numPrunedSegments += routeInfo.getNumPrunedSegments();
    }
    Map<ServerRoutingInstance, InstanceRequest> requestMap = new HashMap<>();

    for (Map.Entry<ServerInstance, List<TableRouteInfo>> entry : offlineTableRouteInfo.entrySet()) {
      requestMap.put(
          new ServerRoutingInstance(entry.getKey().getHostname(), entry.getKey().getPort(), TableType.OFFLINE),
          getInstanceRequest(_serverBrokerRequest, entry.getValue()));
    }

    for (Map.Entry<ServerInstance, List<TableRouteInfo>> entry : realtimeTableRouteInfo.entrySet()) {
      requestMap.put(
          new ServerRoutingInstance(entry.getKey().getHostname(), entry.getKey().getPort(), TableType.REALTIME),
          getInstanceRequest(_serverBrokerRequest, entry.getValue()));
    }

    return requestMap;
  }

  private InstanceRequest getInstanceRequest(BrokerRequest brokerRequest, List<TableRouteInfo> tableRouteInfo) {
    InstanceRequest instanceRequest = new InstanceRequest();
    instanceRequest.setRequestId(_requestId);
    instanceRequest.setQuery(brokerRequest);
    Map<String, String> queryOptions = brokerRequest.getPinotQuery().getQueryOptions();
    if (queryOptions != null) {
      instanceRequest.setEnableTrace(Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.TRACE)));
    }
    instanceRequest.setLogicalTableRouteInfo(tableRouteInfo);
    instanceRequest.setBrokerId(_brokerId);
    return instanceRequest;
  }
}
