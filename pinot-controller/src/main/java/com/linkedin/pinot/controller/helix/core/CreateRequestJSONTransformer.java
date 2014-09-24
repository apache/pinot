package com.linkedin.pinot.controller.helix.core;

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
import com.linkedin.pinot.controller.helix.properties.OfflineProperties;
import com.linkedin.pinot.controller.helix.properties.RealtimeProperties;


public class CreateRequestJSONTransformer {

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

  public static PinotStandaloneResource buildOfflineResource(JSONObject offlinePropertiesJSON, JSONObject externalJSON)
      throws JSONException {
    PinotStandaloneResource resource =
        new PinotStandaloneResource(StandaloneType.offline,
            offlinePropertiesJSON.getString(IncomingConfigParamKeys.Offline.offlineNumReplicas.toString()),
            offlinePropertiesJSON.getString(IncomingConfigParamKeys.Offline.offlineNumInstancesPerReplica.toString()));
    Map<String, String> resourceConfigs = new HashMap<String, String>();

    resourceConfigs.put(OfflineProperties.RESOURCE_LEVEL.pinot_resource_numReplicas.toString(),
        offlinePropertiesJSON.getString(IncomingConfigParamKeys.Offline.offlineNumReplicas.toString()));
    resourceConfigs.put(OfflineProperties.RESOURCE_LEVEL.pinot_resource_name.toString(),
        offlinePropertiesJSON.getString(IncomingConfigParamKeys.Offline.offlineResourceName.toString()));
    resourceConfigs.put(OfflineProperties.RESOURCE_LEVEL.pinot_resource_numInstancesPerReplica.toString(),
        offlinePropertiesJSON.getString(IncomingConfigParamKeys.Offline.offlineNumInstancesPerReplica.toString()));
    resourceConfigs.put(OfflineProperties.RESOURCE_LEVEL.pinot_offline_retention_time_unit.toString(),
        offlinePropertiesJSON.getString(IncomingConfigParamKeys.Offline.offlineRetentionTimeUnit.toString()));
    resourceConfigs.put(OfflineProperties.RESOURCE_LEVEL.pinot_offline_retention_time_column.toString(),
        offlinePropertiesJSON.getString(IncomingConfigParamKeys.Offline.offlineRetentionTimeColumn.toString()));
    resourceConfigs.put(OfflineProperties.RESOURCE_LEVEL.pinot_offline_retention_duration.toString(),
        offlinePropertiesJSON.getString(IncomingConfigParamKeys.Offline.offlineRetentionDuration.toString()));
    resourceConfigs.put(OfflineProperties.RESOURCE_LEVEL.pinot_resource_type.toString(),
        PinotResourceType.offline.toString());

    Iterator<String> keys = offlinePropertiesJSON.keys();

    while (keys.hasNext()) {
      String key = keys.next();
      if (OfflineProperties.DEFAULT_LEVEL.hasKey(key)) {
        resourceConfigs.put(key, offlinePropertiesJSON.getString(key));
      }
    }

    resource.setResourceProperties(resourceConfigs);

    mergeExternal(resource, externalJSON);
    return resource;
  }

  public static PinotStandaloneResource buildRealtimeResource(JSONObject realtimePropertiesJSON, JSONObject externalJSON)
      throws JSONException {
    PinotStandaloneResource resource =
        new PinotStandaloneResource(StandaloneType.realtime,
            realtimePropertiesJSON.getString(IncomingConfigParamKeys.Realtime.realtimeNumReplicas.toString()), "1");
    Map<String, String> resourceConfigs = new HashMap<String, String>();

    resourceConfigs.put(RealtimeProperties.RESOURCE_LEVEL.httpServer_numReplicas.toString(),
        realtimePropertiesJSON.getString(IncomingConfigParamKeys.Realtime.realtimeNumReplicas.toString()));

    resourceConfigs.put(RealtimeProperties.RESOURCE_LEVEL.sensei_cluster_name.toString(),
        realtimePropertiesJSON.getString(IncomingConfigParamKeys.Realtime.realtimeResourceName.toString()));
    resourceConfigs.put(RealtimeProperties.RESOURCE_LEVEL.sensei_index_manager_default_maxpartition_id.toString(),
        realtimePropertiesJSON.getString(IncomingConfigParamKeys.Realtime.realtimeMaxPartitionId.toString()));

    resourceConfigs.put(RealtimeProperties.RESOURCE_LEVEL.indexingCoordinator_retention_timeUnit.toString(),
        realtimePropertiesJSON.getString(IncomingConfigParamKeys.Realtime.realtimeRetentionTimeUnit.toString()));

    if (realtimePropertiesJSON.has(IncomingConfigParamKeys.Realtime.realtimeRetentionDynamincDuration.toString())) {
      resourceConfigs.put(RealtimeProperties.RESOURCE_LEVEL.indexingCoordinator_retention_latency_time_column
          .toString(), realtimePropertiesJSON
          .getString(IncomingConfigParamKeys.Realtime.realtimeRetentionDynamincDuration.toString()));
    } else {
      resourceConfigs.put(
          RealtimeProperties.RESOURCE_LEVEL.indexingCoordinator_retention_latency_time_column.toString(),
          realtimePropertiesJSON.getString(IncomingConfigParamKeys.Realtime.realtimeRetentionDuration.toString()));
    }

    resourceConfigs.put(RealtimeProperties.RESOURCE_LEVEL.indexingCoordinator_retention_duration.toString(),
        realtimePropertiesJSON.getString(IncomingConfigParamKeys.Realtime.realtimeRetentionDuration.toString()));

    resourceConfigs.put(RealtimeProperties.RESOURCE_LEVEL.indexingCoordinator_shardedColumn.toString(),
        realtimePropertiesJSON.getString(IncomingConfigParamKeys.Realtime.realtimeShardedColumn.toString()));
    resourceConfigs.put(RealtimeProperties.RESOURCE_LEVEL.indexingCoordinator_sortedColumns.toString(),
        realtimePropertiesJSON.getString(IncomingConfigParamKeys.Realtime.realtimeSortedColumns.toString()));

    resourceConfigs.put(RealtimeProperties.RESOURCE_LEVEL.kafka_topic_name.toString(),
        realtimePropertiesJSON.getString(IncomingConfigParamKeys.Realtime.realtimeKafkaTopicName.toString()));
    resourceConfigs.put(RealtimeProperties.RESOURCE_LEVEL.pinot_cluster_type.toString(),
        PinotResourceType.realtime.toString());
    resourceConfigs.putAll(buildRealtimeSchema(realtimePropertiesJSON
        .getJSONArray(IncomingConfigParamKeys.Realtime.realtimeSchema.toString())));

    resource.setResourceProperties(resourceConfigs);

    JSONArray instancesJSON =
        realtimePropertiesJSON.getJSONArray(IncomingConfigParamKeys.Realtime.realtimeInstances.toString());

    mergeExternal(resource, externalJSON);
    return resource;
  }

  public static final String SCHEMA_KEY_PREFIX = "realtime.table";

  private static Map<String, String> buildRealtimeSchema(JSONArray schemaArrayJSON) throws JSONException {
    Map<String, String> schema = new HashMap<String, String>();
    for (int i = 0; i < schemaArrayJSON.length(); i++) {
      JSONObject schemaJSON = schemaArrayJSON.getJSONObject(i);
      String columnName = schemaJSON.getString(IncomingConfigParamKeys.Realtime.columName.toString());
      String[] columnTypeKey =
          { SCHEMA_KEY_PREFIX, columnName, IncomingConfigParamKeys.Realtime.columnType.toString() };
      schema.put(StringUtils.join(columnTypeKey, "."),
          schemaJSON.getString(IncomingConfigParamKeys.Realtime.columnType.toString()));

      if (schemaJSON.has("isMulti")) {
        String[] isMultiKey = { SCHEMA_KEY_PREFIX, columnName, IncomingConfigParamKeys.Realtime.isMulti.toString() };
        schema.put(StringUtils.join(isMultiKey, "."),
            schemaJSON.getString(IncomingConfigParamKeys.Realtime.isMulti.toString()));
      }

      if (schemaJSON.has("isMetric")) {
        String[] isMetricKey = { SCHEMA_KEY_PREFIX, columnName, IncomingConfigParamKeys.Realtime.isMetric.toString() };
        schema.put(StringUtils.join(isMetricKey, "."),
            schemaJSON.getString(IncomingConfigParamKeys.Realtime.isMetric.toString()));
      }
    }
    return schema;
  }

  private static void mergeExternal(PinotStandaloneResource res, JSONObject externalPropertiesJSON)
      throws JSONException {
    Iterator<String> keys = externalPropertiesJSON.keys();

    while (keys.hasNext()) {
      String key = keys.next();
      res.getResourceProperties().put("external." + key, externalPropertiesJSON.getString(key));
    }

  }
}
