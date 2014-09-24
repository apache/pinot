package com.linkedin.pinot.controller.helix.util;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.controller.helix.core.IncomingConfigParamKeys;


public class ClusterCreateJSONUtil {
  public static JSONObject createOfflineClusterJSON(int numInstancesPerReplica, int numReplicas, String resourceName)
      throws JSONException {
    JSONObject offlineClusterJSON = new JSONObject();
    offlineClusterJSON.put(IncomingConfigParamKeys.Offline.offlineResourceName.toString(), resourceName);
    offlineClusterJSON.put(IncomingConfigParamKeys.Offline.offlineNumInstancesPerReplica.toString(),
        String.valueOf(numInstancesPerReplica));
    offlineClusterJSON.put(IncomingConfigParamKeys.Offline.offlineNumReplicas.toString(), String.valueOf(numReplicas));

    offlineClusterJSON.put(IncomingConfigParamKeys.Offline.offlineRetentionDuration.toString(), "30");
    offlineClusterJSON.put(IncomingConfigParamKeys.Offline.offlineRetentionTimeColumn.toString(), "daysSinceEpoich");
    offlineClusterJSON.put(IncomingConfigParamKeys.Offline.offlineRetentionTimeUnit.toString(), "DAYS ");

    return offlineClusterJSON;
  }

  public static JSONObject createExternalPropsJSON(String d2ClusterName) throws JSONException {
    JSONObject external = new JSONObject();
    external.put("d2ClusterName", d2ClusterName);
    return external;
  }

  public static JSONObject createRealtimeClusterJSON(int numInstances, String clusterName) throws JSONException {
    JSONObject realtimeJSON = new JSONObject();
    realtimeJSON.put(IncomingConfigParamKeys.Realtime.realtimeResourceName.toString(), clusterName);
    realtimeJSON.put(IncomingConfigParamKeys.Realtime.realtimeKafkaTopicName.toString(),
        "MirrorDecoratedProfileViewEvent");
    realtimeJSON.put(IncomingConfigParamKeys.Realtime.realtimeMaxPartitionId.toString(), "0");
    realtimeJSON.put(IncomingConfigParamKeys.Realtime.realtimeRetentionDuration.toString(), "5");
    realtimeJSON.put(IncomingConfigParamKeys.Realtime.realtimeRetentionDynamincDuration.toString(), "3");
    realtimeJSON.put(IncomingConfigParamKeys.Realtime.realtimeRetentionTimeUnit.toString(), "DAYS");
    realtimeJSON.put(IncomingConfigParamKeys.Realtime.realtimeShardedColumn.toString(), "vieweeId");
    realtimeJSON.put(IncomingConfigParamKeys.Realtime.realtimeSortedColumns.toString(), "vieweeId,minutesSinceEpoch");
    realtimeJSON.put(IncomingConfigParamKeys.Realtime.realtimeNumReplicas.toString(), "3");

    JSONArray schemaJSON = new JSONArray();
    schemaJSON.put(constructSchemaEntry("viewerId", "int", false, false));
    schemaJSON.put(constructSchemaEntry("vieweeId", "int", false, false));
    schemaJSON.put(constructSchemaEntry("viewerPrivacySetting", "string", false, false));
    schemaJSON.put(constructSchemaEntry("vieweePrivacySetting", "string", false, false));
    schemaJSON.put(constructSchemaEntry("viewerObfuscationType", "string", false, false));
    schemaJSON.put(constructSchemaEntry("viewerIndustry", "int", false, false));
    schemaJSON.put(constructSchemaEntry("viewerSchool", "int", false, false));
    schemaJSON.put(constructSchemaEntry("referrerCategory", "string", false, false));
    schemaJSON.put(constructSchemaEntry("searchKeywordCategories", "int", true, false));
    schemaJSON.put(constructSchemaEntry("viewerCompanies", "int", true, false));
    schemaJSON.put(constructSchemaEntry("viewerOccupations", "int", true, false));
    schemaJSON.put(constructSchemaEntry("viewerRegionCode", "int", false, false));
    schemaJSON.put(constructSchemaEntry("weeksSinceEpochSunday", "int", false, false));
    schemaJSON.put(constructSchemaEntry("daysSinceEpoch", "int", false, false));
    schemaJSON.put(constructSchemaEntry("minutesSinceEpoch", "int", false, false));
    schemaJSON.put(constructSchemaEntry("count", "int", false, true));

    realtimeJSON.put(IncomingConfigParamKeys.Realtime.realtimeSchema.toString(), schemaJSON);

    JSONArray instancesJSON = new JSONArray();

    for (int i = 0; i < numInstances; i++) {
      JSONObject entryOne = new JSONObject();
      entryOne.put(IncomingConfigParamKeys.Realtime.host.toString(), clusterName + ".host" + i);
      entryOne.put(IncomingConfigParamKeys.Realtime.port.toString(), "0091" + i);
      entryOne.put(IncomingConfigParamKeys.Realtime.partitions.toString(), "0");
      instancesJSON.put(entryOne);
    }

    realtimeJSON.put(IncomingConfigParamKeys.Realtime.realtimeInstances.toString(), instancesJSON);

    return realtimeJSON;
  }

  private static JSONObject constructSchemaEntry(String columnName, String type, boolean isMulti, boolean isMetric)
      throws JSONException {
    JSONObject col = new JSONObject();
    col.put(IncomingConfigParamKeys.Realtime.columName.toString(), columnName);
    col.put(IncomingConfigParamKeys.Realtime.columnType.toString(), type);

    if (isMetric) {
      col.put(IncomingConfigParamKeys.Realtime.isMetric.toString(), isMetric);
    }

    if (isMulti) {
      col.put(IncomingConfigParamKeys.Realtime.isMulti.toString(), isMulti);
    }
    return col;
  }
}
