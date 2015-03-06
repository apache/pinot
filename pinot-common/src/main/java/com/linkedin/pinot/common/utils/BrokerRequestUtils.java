package com.linkedin.pinot.common.utils;



public class BrokerRequestUtils {

  public static String getRealtimeResourceNameForResource(String hybridResource) {
    return hybridResource + CommonConstants.Broker.DataResource.REALTIME_RESOURCE_SUFFIX;
  }

  public static String getOfflineResourceNameForResource(String hybridResource) {
    return hybridResource + CommonConstants.Broker.DataResource.OFFLINE_RESOURCE_SUFFIX;
  }
}
