package org.apache.pinot.core.transport;

import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.NotNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;


public abstract class AbstractRoute implements Route {
  public Map<ServerRoutingInstance, InstanceRequest> getRequestMap(long requestId, String brokerId, boolean preferTls) {
    Map<ServerRoutingInstance, InstanceRequest> requestMap = null;
    Map<ServerRoutingInstance, InstanceRequest> offlineRequestMap =
        getOfflineRequestMap(requestId, brokerId, preferTls);
    Map<ServerRoutingInstance, InstanceRequest> realtimeRequestMap =
        getRealtimeRequestMap(requestId, brokerId, preferTls);

    if (offlineRequestMap != null) {
      requestMap = offlineRequestMap;
    }
    if (realtimeRequestMap != null) {
      if (requestMap == null) {
        requestMap = realtimeRequestMap;
      } else {
        requestMap.putAll(realtimeRequestMap);
      }
    }

    return requestMap;
  }

  @NotNull
  protected Map<ServerRoutingInstance, InstanceRequest> getRequestMapFromRoutingTable(
      Map<ServerInstance, ServerRouteInfo> routingTable, BrokerRequest brokerRequest, long requestId, String brokerId,
      boolean preferTls) {
    Map<ServerRoutingInstance, InstanceRequest> requestMap = new HashMap<>();
    for (Map.Entry<ServerInstance, ServerRouteInfo> entry : routingTable.entrySet()) {
      ServerRoutingInstance serverRoutingInstance =
          entry.getKey().toServerRoutingInstance(TableType.OFFLINE, preferTls);
      InstanceRequest instanceRequest = getInstanceRequest(requestId, brokerId, brokerRequest, entry.getValue());
      requestMap.put(serverRoutingInstance, instanceRequest);
    }
    return requestMap;
  }

  protected InstanceRequest getInstanceRequest(long requestId, String brokerId, BrokerRequest brokerRequest,
      ServerRouteInfo segments) {
    InstanceRequest instanceRequest = new InstanceRequest();
    instanceRequest.setRequestId(requestId);
    instanceRequest.setQuery(brokerRequest);
    Map<String, String> queryOptions = brokerRequest.getPinotQuery().getQueryOptions();
    if (queryOptions != null) {
      instanceRequest.setEnableTrace(Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.TRACE)));
    }
    instanceRequest.setSearchSegments(segments.getSegments());
    instanceRequest.setBrokerId(brokerId);
    if (CollectionUtils.isNotEmpty(segments.getOptionalSegments())) {
      // Don't set this field, i.e. leave it as null, if there is no optional segment at all, to be more backward
      // compatible, as there are places like in multi-stage query engine where this field is not set today when
      // creating the InstanceRequest.
      instanceRequest.setOptionalSegments(segments.getOptionalSegments());
    }
    return instanceRequest;
  }
}
