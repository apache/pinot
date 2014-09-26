package com.linkedin.pinot.controller.helix.properties;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.controller.helix.api.PinotResourceType;
import com.linkedin.pinot.controller.helix.api.PinotStandaloneResource;
import com.linkedin.pinot.controller.helix.api.PinotStandaloneResource.StandaloneType;
import com.linkedin.pinot.controller.helix.core.IncomingConfigParamKeys;


public class PinotResourceCreationRequestBuilder {

  /*
   * This method gets requestJSON only when a new pinot cluster needs to be created. it transforms
   * json into PinotResource
   * 
   * Keys that we expect to be present in the transformer
   * 
   * { clusterType: , offlineProperties : { offlineClusterName : , offlineNumReplicas : ,
   * offlineMaxPartitionId : , offlineInstances : [ { host:, port:, partitions:, } ] },
   * realtimeProperties : { realtimeClusterName : , realtimeNumReplicas : , realtimeMaxPartitionId :
   * , realtimeInstances : [ { host:, port:, partitions:, } ] }, hybridProperties : {
   * hybridClusterName : , realtimeClusterName : , hybridClusterName }, external : { // whatever you
   * pass in here will be sent along to the instance as it is // d2 is a good thing to put here } }
   */

  public static PinotStandaloneResource buildOfflineResource(Map<String, String> offlineProperties)
      throws JSONException {
    PinotStandaloneResource resource =
        new PinotStandaloneResource(StandaloneType.offline,
            offlineProperties.get(IncomingConfigParamKeys.Offline.offlineNumReplicas.toString()),
            offlineProperties.get(IncomingConfigParamKeys.Offline.offlineNumInstancesPerReplica.toString()));
    Map<String, String> resourceConfigs = new HashMap<String, String>();

    resourceConfigs.put(OfflineProperties.RESOURCE_LEVEL.pinot_resource_numReplicas.toString(),
        offlineProperties.get(IncomingConfigParamKeys.Offline.offlineNumReplicas.toString()));
    resourceConfigs.put(OfflineProperties.RESOURCE_LEVEL.pinot_resource_name.toString(),
        offlineProperties.get(IncomingConfigParamKeys.Offline.offlineResourceName.toString()));
    resourceConfigs.put(OfflineProperties.RESOURCE_LEVEL.pinot_resource_numInstancesPerReplica.toString(),
        offlineProperties.get(IncomingConfigParamKeys.Offline.offlineNumInstancesPerReplica.toString()));
    resourceConfigs.put(OfflineProperties.RESOURCE_LEVEL.pinot_offline_retention_time_unit.toString(),
        offlineProperties.get(IncomingConfigParamKeys.Offline.offlineRetentionTimeUnit.toString()));
    resourceConfigs.put(OfflineProperties.RESOURCE_LEVEL.pinot_offline_retention_time_column.toString(),
        offlineProperties.get(IncomingConfigParamKeys.Offline.offlineRetentionTimeColumn.toString()));
    resourceConfigs.put(OfflineProperties.RESOURCE_LEVEL.pinot_offline_retention_duration.toString(),
        offlineProperties.get(IncomingConfigParamKeys.Offline.offlineRetentionDuration.toString()));
    resourceConfigs.put(OfflineProperties.RESOURCE_LEVEL.pinot_resource_type.toString(),
        PinotResourceType.offline.toString());

    Iterator<String> keys = offlineProperties.keySet().iterator();

    while (keys.hasNext()) {
      String key = keys.next();
      if (OfflineProperties.DEFAULT_LEVEL.hasKey(key)) {
        resourceConfigs.put(key, offlineProperties.get(key));
      }
    }

    resource.setResourceProperties(resourceConfigs);

    return resource;
  }
}
