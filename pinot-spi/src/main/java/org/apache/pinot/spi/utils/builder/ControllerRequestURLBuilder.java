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
package org.apache.pinot.spi.utils.builder;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.StringUtil;


public class ControllerRequestURLBuilder {
  private final String _baseUrl;

  private ControllerRequestURLBuilder(String baseUrl) {
    int length = baseUrl.length();
    if (baseUrl.charAt(length - 1) == '/') {
      _baseUrl = baseUrl.substring(0, length - 1);
    } else {
      _baseUrl = baseUrl;
    }
  }

  public static ControllerRequestURLBuilder baseUrl(String baseUrl) {
    return new ControllerRequestURLBuilder(baseUrl);
  }

  public String getBaseUrl() {
    return _baseUrl;
  }

  public String forDataFileUpload() {
    return StringUtil.join("/", _baseUrl, "segments");
  }

  public String forInstanceCreate() {
    return StringUtil.join("/", _baseUrl, "instances");
  }

  public String forInstanceState(String instanceName) {
    return StringUtil.join("/", _baseUrl, "instances", instanceName, "state");
  }

  public String forInstance(String instanceName) {
    return StringUtil.join("/", _baseUrl, "instances", instanceName);
  }

  public String forInstanceUpdateTags(String instanceName, List<String> tags) {
    return forInstanceUpdateTags(instanceName, tags, false);
  }

  public String forInstanceUpdateTags(String instanceName, List<String> tags, boolean updateBrokerResource) {
    return StringUtil.join("/", _baseUrl, "instances", instanceName,
        "updateTags?tags=" + StringUtils.join(tags, ",") + "&updateBrokerResource=" + updateBrokerResource);
  }

  public String forInstanceList() {
    return StringUtil.join("/", _baseUrl, "instances");
  }

  public String forTablesFromTenant(String tenantName) {
    return StringUtil.join("/", _baseUrl, "tenants", tenantName, "tables");
  }

  public String forTablesFromTenant(String tenantName, String componentType) {
    return StringUtil.join("/", _baseUrl, "tenants", tenantName, "tables") + "?type=" + componentType;
  }

  // V2 API started
  public String forTenantCreate() {
    return StringUtil.join("/", _baseUrl, "tenants");
  }

  public String forUserCreate() {
    return StringUtil.join("/", _baseUrl, "users");
  }

  public String forTenantGet() {
    return StringUtil.join("/", _baseUrl, "tenants");
  }

  public String forUserGet(String username, String componentTypeStr) {
    StringBuilder params = new StringBuilder();
    if (StringUtils.isNotBlank(username)) {
      params.append("?component=" + componentTypeStr);
    }
    return StringUtil.join("/", _baseUrl, "users", username, params.toString());
  }

  public String forPeriodTaskRun(String taskName) {
    return StringUtil.join("/", _baseUrl, "periodictask", "run?taskname=" + taskName);
  }

  public String forUpdateUserConfig(String username, String componentTypeStr, boolean passwordChanged) {
    StringBuilder params = new StringBuilder();
    if (StringUtils.isNotBlank(username)) {
      params.append("?component=" + componentTypeStr);
    }
    params.append(String.format("&&passwordChanged=%s", passwordChanged));
    return StringUtil.join("/", _baseUrl, "users", username, params.toString());
  }

  public String forTenantGet(String tenantName) {
    return StringUtil.join("/", _baseUrl, "tenants", tenantName);
  }

  public String forBrokerTenantGet(String tenantName) {
    return StringUtil.join("/", _baseUrl, "tenants", tenantName, "?type=broker");
  }

  public String forServerTenantGet(String tenantName) {
    return StringUtil.join("/", _baseUrl, "tenants", tenantName, "?type=server");
  }

  public String forBrokerTenantDelete(String tenantName) {
    return StringUtil.join("/", _baseUrl, "tenants", tenantName, "?type=broker");
  }

  public String forServerTenantDelete(String tenantName) {
    return StringUtil.join("/", _baseUrl, "tenants", tenantName, "?type=server");
  }

  public String forBrokersGet(String state) {
    if (state == null) {
      return StringUtil.join("/", _baseUrl, "brokers");
    }
    return StringUtil.join("/", _baseUrl, "brokers", "?state=" + state);
  }

  public String forBrokerTenantsGet(String state) {
    if (state == null) {
      return StringUtil.join("/", _baseUrl, "brokers", "tenants");
    }
    return StringUtil.join("/", _baseUrl, "brokers", "tenants", "?state=" + state);
  }

  public String forBrokerTenantGet(String tenant, String state) {
    if (state == null) {
      return StringUtil.join("/", _baseUrl, "brokers", "tenants", tenant);
    }
    return StringUtil.join("/", _baseUrl, "brokers", "tenants", tenant, "?state=" + state);
  }

  public String forBrokerTablesGet(String state) {
    if (state == null) {
      return StringUtil.join("/", _baseUrl, "brokers", "tables");
    }
    return StringUtil.join("/", _baseUrl, "brokers", "tables", "?state=" + state);
  }

  public String forTenantInstancesToggle(String tenant, String tenantType, String state) {
    return StringUtil.join("/", _baseUrl, "tenants", tenant)
        + "?type=" + tenantType + "&state=" + state;
  }

  public String forLiveBrokerTablesGet() {
    return StringUtil.join("/", _baseUrl, "tables", "livebrokers");
  }

  public String forBrokerTableGet(String table, String tableType, String state) {
    StringBuilder params = new StringBuilder();
    if (tableType != null) {
      params.append("?type=" + tableType);
    }
    if (state != null) {
      if (params.length() > 0) {
        params.append("&");
      }
      params.append("?state=" + state);
    }
    return StringUtil.join("/", _baseUrl, "brokers", "tables", table, params.toString());
  }

  public String forTableCreate() {
    return StringUtil.join("/", _baseUrl, "tables");
  }

  public String forUpdateTableConfig(String tableName) {
    return StringUtil.join("/", _baseUrl, "tables", tableName);
  }

  public String forTableRebalance(String tableName, String tableType) {
    return forTableRebalance(tableName, tableType, false, false, false, false, 1);
  }

  public String forTableRebalance(String tableName, String tableType, boolean dryRun, boolean reassignInstances,
      boolean includeConsuming, boolean downtime, int minAvailableReplicas) {
    StringBuilder stringBuilder =
        new StringBuilder(StringUtil.join("/", _baseUrl, "tables", tableName, "rebalance?type=" + tableType));
    if (dryRun) {
      stringBuilder.append("&dryRun=").append(dryRun);
    }
    if (reassignInstances) {
      stringBuilder.append("&reassignInstances=").append(reassignInstances);
    }
    if (includeConsuming) {
      stringBuilder.append("&includeConsuming=").append(includeConsuming);
    }
    if (downtime) {
      stringBuilder.append("&downtime=").append(downtime);
    }
    if (minAvailableReplicas != 1) {
      stringBuilder.append("&minAvailableReplicas=").append(minAvailableReplicas);
    }
    return stringBuilder.toString();
  }

  public String forTableForceCommit(String tableName) {
    return StringUtil.join("/", _baseUrl, "tables", tableName, "forceCommit");
  }

  public String forForceCommitJobStatus(String jobId) {
    return StringUtil.join("/", _baseUrl, "tables", "forceCommitStatus", jobId);
  }

  public String forTableReload(String tableName, TableType tableType, boolean forceDownload) {
    String query = String.format("reload?type=%s&forceDownload=%s", tableType.name(), forceDownload);
    return StringUtil.join("/", _baseUrl, "segments", tableName, query);
  }

  public String forTableRebalanceStatus(String jobId) {
    return StringUtil.join("/", _baseUrl, "rebalanceStatus", jobId);
  }

  public String forTableReset(String tableNameWithType, @Nullable String targetInstance) {
    String query = targetInstance == null ? "reset" : String.format("reset?targetInstance=%s", targetInstance);
    return StringUtil.join("/", _baseUrl, "segments", tableNameWithType, query);
  }

  public String forSegmentReloadStatus(String jobId) {
    return StringUtil.join("/", _baseUrl, "segments", "segmentReloadStatus", jobId);
  }

  public String forTableSize(String tableName) {
    return StringUtil.join("/", _baseUrl, "tables", tableName, "size");
  }

  public String forTableUpdateIndexingConfigs(String tableName) {
    return StringUtil.join("/", _baseUrl, "tables", tableName, "indexingConfigs");
  }

  public String forTableGetServerInstances(String tableName) {
    return StringUtil.join("/", _baseUrl, "tables", tableName, "instances?type=server");
  }

  public String forTableGetBrokerInstances(String tableName) {
    return StringUtil.join("/", _baseUrl, "tables", tableName, "instances?type=broker");
  }

  public String forTableGet(String tableName) {
    return StringUtil.join("/", _baseUrl, "tables", tableName);
  }

  public String forTableDelete(String tableName) {
    return StringUtil.join("/", _baseUrl, "tables", tableName);
  }

  public String forTableView(String tableName, String view, @Nullable String tableType) {
    String url = StringUtil.join("/", _baseUrl, "tables", tableName, view);
    if (tableType != null) {
      url += "?tableType=" + tableType;
    }
    return url;
  }

  public String forTableSchemaGet(String tableName) {
    return StringUtil.join("/", _baseUrl, "tables", tableName, "schema");
  }

  public String forTableExternalView(String tableName) {
    return StringUtil.join("/", _baseUrl, "tables", tableName, "externalview");
  }

  public String forSchemaValidate() {
    return StringUtil.join("/", _baseUrl, "schemas", "validate");
  }

  public String forSchemaCreate() {
    return StringUtil.join("/", _baseUrl, "schemas");
  }

  public String forSchemaUpdate(String schemaName) {
    return StringUtil.join("/", _baseUrl, "schemas", schemaName);
  }

  public String forSchemaGet(String schemaName) {
    return StringUtil.join("/", _baseUrl, "schemas", schemaName);
  }

  public String forSchemaDelete(String schemaName) {
    return StringUtil.join("/", _baseUrl, "schemas", schemaName);
  }

  public String forTableConfigsCreate() {
    return StringUtil.join("/", _baseUrl, "tableConfigs");
  }

  public String forTableConfigsGet(String configName) {
    return StringUtil.join("/", _baseUrl, "tableConfigs", configName);
  }

  public String forTableConfigsList() {
    return StringUtil.join("/", _baseUrl, "tableConfigs");
  }

  public String forTableConfigsUpdate(String configName) {
    return StringUtil.join("/", _baseUrl, "tableConfigs", configName);
  }

  public String forTableConfigsDelete(String configName) {
    return StringUtil.join("/", _baseUrl, "tableConfigs", configName);
  }

  public String forTableConfigsValidate() {
    return StringUtil.join("/", _baseUrl, "tableConfigs", "validate");
  }

  public String forSegmentReload(String tableName, String segmentName, boolean forceDownload) {
    return StringUtil.join("/", _baseUrl, "segments", tableName, encode(segmentName),
        "reload?forceDownload=" + forceDownload);
  }

  public String forSegmentReset(String tableNameWithType, String segmentName, String targetInstance) {
    String query = targetInstance == null ? "reset" : String.format("reset?targetInstance=%s", targetInstance);
    return StringUtil.join("/", _baseUrl, "segments", tableNameWithType, encode(segmentName), query);
  }

  public String forSegmentDownload(String tableName, String segmentName) {
    return StringUtil.join("/", _baseUrl, "segments", tableName, encode(segmentName));
  }

  public String forSegmentDelete(String tableName, String segmentName) {
    return StringUtil.join("/", _baseUrl, "segments", tableName, encode(segmentName));
  }

  public String forSegmentDeleteAll(String tableName, String tableType) {
    return StringUtil.join("/", _baseUrl, "segments", tableName + "?type=" + tableType);
  }

  public String forListAllSegments(String tableName) {
    return StringUtil.join("/", _baseUrl, "tables", tableName, "segments");
  }

  public String forSegmentsMetadataFromServer(String tableName) {
    return forSegmentsMetadataFromServer(tableName, null);
  }

  public String forSegmentsMetadataFromServer(String tableName, @Nullable String columns) {
    String url = StringUtil.join("/", _baseUrl, "segments", tableName, "metadata");
    if (columns != null) {
      url += "?columns=" + columns;
    }
    return url;
  }

  public String forSegmentMetadata(String tableName, String segmentName) {
    return StringUtil.join("/", _baseUrl, "segments", tableName, encode(segmentName), "metadata");
  }

  public String forListAllSegmentLineages(String tableName, String tableType) {
    return StringUtil.join("/", _baseUrl, "segments", tableName, "lineage?type=" + tableType);
  }

  public String forListAllCrcInformationForTable(String tableName) {
    return StringUtil.join("/", _baseUrl, "segments", tableName, "crc");
  }

  public String forDeleteTableWithType(String tableName, String tableType) {
    return StringUtil.join("/", _baseUrl, "tables", tableName + "?type=" + tableType);
  }

  public String forSegmentListAPI(String tableName) {
    return forSegmentListAPI(tableName, null, false, Long.MIN_VALUE, Long.MAX_VALUE, false);
  }

  public String forSegmentListAPI(String tableName, String tableType) {
    return forSegmentListAPI(tableName, tableType, false, Long.MIN_VALUE, Long.MAX_VALUE, false);
  }

  public String forSegmentListAPI(String tableName, String tableType, boolean excludeReplacedSegments) {
    return forSegmentListAPI(tableName, tableType, excludeReplacedSegments, Long.MIN_VALUE, Long.MAX_VALUE, false);
  }

  public String forSegmentListAPI(String tableName, @Nullable String tableType, boolean excludeReplacedSegments,
      long startTimestamp, long endTimestamp, boolean excludeOverlapping) {
    StringBuilder url = new StringBuilder();
    url.append(StringUtil.join("/", _baseUrl, "segments", tableName));

    StringBuilder parameter = new StringBuilder();
    if (tableType != null) {
      appendUrlParameter(parameter, "type", tableType);
    }
    if (excludeReplacedSegments) {
      appendUrlParameter(parameter, "excludeReplacedSegments", "true");
    }
    if (startTimestamp != Long.MIN_VALUE) {
      appendUrlParameter(parameter, "startTimestamp", Long.toString(startTimestamp));
    }
    if (endTimestamp != Long.MAX_VALUE) {
      appendUrlParameter(parameter, "endTimestamp", Long.toString(endTimestamp));
    }
    if (excludeOverlapping) {
      appendUrlParameter(parameter, "excludeOverlapping", "true");
    }
    return url.append(parameter).toString();
  }

  public String forSegmentDeleteWithTimeWindowAPI(String tableName, long startTimeInMilliSeconds,
      long endTimeInMilliSeconds) {
    StringBuilder url = new StringBuilder();
    url.append(StringUtil.join("/", _baseUrl, "segments", tableName,
        String.format("choose?startTimestamp=%d&endTimestamp=%d", startTimeInMilliSeconds, endTimeInMilliSeconds)));
    return url.toString();
  }

  public String forDeleteMultipleSegments(String tableName, String tableType, List<String> segments) {
    StringBuilder fullUrl = new StringBuilder(
        StringUtil.join("?", StringUtil.join("/", _baseUrl, "segments", tableName),
             "type=" + tableType));
    for (String segment : segments) {
      fullUrl.append("&segments=").append(segment);
    }
    return fullUrl.toString();
  }

  private void appendUrlParameter(StringBuilder url, String urlParameterKey, String urlParameterValue) {
    if (url.length() == 0) {
      url.append("?").append(urlParameterKey).append("=").append(urlParameterValue);
    } else {
      url.append("&").append(urlParameterKey).append("=").append(urlParameterValue);
    }
  }

  public String forInstancePartitions(String tableName, @Nullable String instancePartitionsType) {
    String url = StringUtil.join("/", _baseUrl, "tables", tableName, "instancePartitions");
    if (instancePartitionsType != null) {
      url += "?type=" + instancePartitionsType;
    }
    return url;
  }

  public String forInstanceAssign(String tableName, @Nullable InstancePartitionsType instancePartitionsType,
      boolean dryRun) {
    String url = StringUtil.join("/", _baseUrl, "tables", tableName, "assignInstances");
    if (instancePartitionsType != null) {
      url += "?type=" + instancePartitionsType;
      if (dryRun) {
        url += "&dryRun=true";
      }
    } else {
      if (dryRun) {
        url += "?dryRun=true";
      }
    }
    return url;
  }

  public String forInstanceReplace(String tableName, @Nullable InstancePartitionsType instancePartitionsType,
      String oldInstanceId, String newInstanceId) {
    String url =
        StringUtil.join("/", _baseUrl, "tables", tableName, "replaceInstance") + "?oldInstanceId=" + oldInstanceId
            + "&newInstanceId=" + newInstanceId;
    if (instancePartitionsType != null) {
      url += "&type=" + instancePartitionsType;
    }
    return url;
  }

  public String forIngestFromFile(String tableNameWithType, String batchConfigMapStr)
      throws UnsupportedEncodingException {
    return String.format("%s?tableNameWithType=%s&batchConfigMapStr=%s",
        StringUtil.join("/", _baseUrl, "ingestFromFile"), tableNameWithType,
        URLEncoder.encode(batchConfigMapStr, StandardCharsets.UTF_8.toString()));
  }

  public String forIngestFromFile(String tableNameWithType, Map<String, String> batchConfigMap)
      throws UnsupportedEncodingException {
    String batchConfigMapStr =
        batchConfigMap.entrySet().stream().map(e -> String.format("\"%s\":\"%s\"", e.getKey(), e.getValue()))
            .collect(Collectors.joining(",", "{", "}"));
    return forIngestFromFile(tableNameWithType, batchConfigMapStr);
  }

  public String forIngestFromURI(String tableNameWithType, String batchConfigMapStr, String sourceURIStr)
      throws UnsupportedEncodingException {
    return String.format("%s?tableNameWithType=%s&batchConfigMapStr=%s&sourceURIStr=%s",
        StringUtil.join("/", _baseUrl, "ingestFromURI"), tableNameWithType,
        URLEncoder.encode(batchConfigMapStr, StandardCharsets.UTF_8.toString()),
        URLEncoder.encode(sourceURIStr, StandardCharsets.UTF_8.toString()));
  }

  public String forIngestFromURI(String tableNameWithType, Map<String, String> batchConfigMap, String sourceURIStr)
      throws UnsupportedEncodingException {
    String batchConfigMapStr =
        batchConfigMap.entrySet().stream().map(e -> String.format("\"%s\":\"%s\"", e.getKey(), e.getValue()))
            .collect(Collectors.joining(",", "{", "}"));
    return forIngestFromURI(tableNameWithType, batchConfigMapStr, sourceURIStr);
  }

  public String forClusterConfigs() {
    return StringUtil.join("/", _baseUrl, "cluster/configs");
  }

  public String forAppConfigs() {
    return StringUtil.join("/", _baseUrl, "appconfigs");
  }

  public String forZkPut() {
    return StringUtil.join("/", _baseUrl, "zk/put");
  }

  public String forZkPutChildren(String path) {
    return StringUtil.join("/", _baseUrl, "zk/putChildren", "?path=" + path);
  }

  public String forZKCreate() {
    return StringUtil.join("/", _baseUrl, "zk/create");
  }

  public String forZkDelete() {
    return StringUtil.join("/", _baseUrl, "zk/delete");
  }

  public String forZkGet(String path) {
    return StringUtil.join("/", _baseUrl, "zk/get", "?path=" + path);
  }

  public String forZkGetChildren(String path) {
    return StringUtil.join("/", _baseUrl, "zk/getChildren", "?path=" + path);
  }

  public String forUpsertTableHeapEstimation(long cardinality, int primaryKeySize, int numPartitions) {
    return StringUtil.join("/", _baseUrl, "upsert/estimateHeapUsage",
        "?cardinality=" + cardinality + "&primaryKeySize=" + primaryKeySize + "&numPartitions=" + numPartitions);
  }

  public String forPauseConsumption(String tableName) {
    return StringUtil.join("/", _baseUrl, "tables", tableName, "pauseConsumption");
  }

  public String forResumeConsumption(String tableName) {
    return StringUtil.join("/", _baseUrl, "tables", tableName, "resumeConsumption");
  }

  public String forPauseStatus(String tableName) {
    return StringUtil.join("/", _baseUrl, "tables", tableName, "pauseStatus");
  }

  public String forUpdateTagsValidation() {
    return String.format("%s/instances/updateTags/validate", _baseUrl);
  }

  private static String encode(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (Exception e) {
      // Should never happen
      throw new RuntimeException(e);
    }
  }

  public String forSegmentUpload() {
    return StringUtil.join("/", _baseUrl, "v2/segments");
  }
}
