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
package org.apache.pinot.controller.helix;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfigConstants;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;


public class ControllerRequestURLBuilder {
  private final String _baseUrl;

  private ControllerRequestURLBuilder(String baseUrl) {
    _baseUrl = StringUtils.chomp(baseUrl, "/");
  }

  public static ControllerRequestURLBuilder baseUrl(String baseUrl) {
    return new ControllerRequestURLBuilder(baseUrl);
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
    return StringUtil.join("/", _baseUrl, "instances", instanceName, "updateTags?tags=" + StringUtils.join(tags, ","));
  }

  public String forInstanceList() {
    return StringUtil.join("/", _baseUrl, "instances");
  }

  public String forTablesFromTenant(String tenantName) {
    return StringUtil.join("/", _baseUrl, "tenants", tenantName, "tables");
  }

  // V2 API started
  public String forTenantCreate() {
    return StringUtil.join("/", _baseUrl, "tenants");
  }

  public String forTenantGet() {
    return StringUtil.join("/", _baseUrl, "tenants");
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
    return forTableRebalance(tableName, tableType, RebalanceConfigConstants.DEFAULT_DRY_RUN,
        RebalanceConfigConstants.DEFAULT_REASSIGN_INSTANCES, RebalanceConfigConstants.DEFAULT_INCLUDE_CONSUMING,
        RebalanceConfigConstants.DEFAULT_DOWNTIME,
        RebalanceConfigConstants.DEFAULT_MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME);
  }

  public String forTableRebalance(String tableName, String tableType, boolean dryRun, boolean reassignInstances,
      boolean includeConsuming, boolean downtime, int minAvailableReplicas) {
    StringBuilder stringBuilder =
        new StringBuilder(StringUtil.join("/", _baseUrl, "tables", tableName, "rebalance?type=" + tableType));
    if (dryRun != RebalanceConfigConstants.DEFAULT_DRY_RUN) {
      stringBuilder.append("&dryRun=").append(dryRun);
    }
    if (reassignInstances != RebalanceConfigConstants.DEFAULT_REASSIGN_INSTANCES) {
      stringBuilder.append("&reassignInstances=").append(reassignInstances);
    }
    if (includeConsuming != RebalanceConfigConstants.DEFAULT_INCLUDE_CONSUMING) {
      stringBuilder.append("&includeConsuming=").append(includeConsuming);
    }
    if (downtime != RebalanceConfigConstants.DEFAULT_DOWNTIME) {
      stringBuilder.append("&downtime=").append(downtime);
    }
    if (minAvailableReplicas != RebalanceConfigConstants.DEFAULT_MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME) {
      stringBuilder.append("&minAvailableReplicas=").append(minAvailableReplicas);
    }
    return stringBuilder.toString();
  }

  public String forTableReload(String tableName, TableType tableType, boolean forceDownload) {
    String query = String.format("reload?forceDownload=%s&type=%s", forceDownload, tableType.name());
    return StringUtil.join("/", _baseUrl, "segments", tableName, query);
  }

  public String forSegmentReload(String tableName, String segmentName, boolean forceDownload)
      throws UnsupportedEncodingException {
    String query = "reload?forceDownload=" + forceDownload;
    String segName = URLEncoder.encode(segmentName, StandardCharsets.UTF_8.toString());
    return StringUtil.join("/", _baseUrl, "segments", tableName, segName, query);
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

  public String forSchemaCreate() {
    return StringUtil.join("/", _baseUrl, "schemas");
  }

  public String forSchemaUpdate(String schemaName) {
    return StringUtil.join("/", _baseUrl, "schemas", schemaName);
  }

  public String forSchemaGet(String schemaName) {
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

  public String forSegmentDownload(String tableName, String segmentName) {
    return URIUtils.constructDownloadUrl(_baseUrl, tableName, segmentName);
  }

  public String forSegmentDelete(String tableName, String segmentName) {
    return StringUtil.join("/", _baseUrl, "segments", tableName, segmentName);
  }

  public String forSegmentDeleteAPI(String tableName, String segmentName, String tableType) {
    return URIUtils.getPath(_baseUrl, "segments", tableName, URIUtils.encode(segmentName)) + "?type=" + tableType;
  }

  public String forSegmentDeleteAllAPI(String tableName, String tableType) {
    return StringUtil.join("/", _baseUrl, "segments", tableName + "?type=" + tableType);
  }

  public String forListAllSegments(String tableName) {
    return StringUtil.join("/", _baseUrl, "tables", tableName, "segments");
  }

  public String forSegmentMetadata(String tableName, String segmentName) {
    return StringUtil.join("/", _baseUrl, "segments", tableName, segmentName, "metadata");
  }

  public String forListAllCrcInformationForTable(String tableName) {
    return StringUtil.join("/", _baseUrl, "tables", tableName, "segments", "crc");
  }

  public String forDeleteTableWithType(String tableName, String tableType) {
    return StringUtil.join("/", _baseUrl, "tables", tableName + "?type=" + tableType);
  }

  public String forSegmentListAPIWithTableType(String tableName, String tableType) {
    return StringUtil.join("/", _baseUrl, "segments", tableName + "?type=" + tableType);
  }

  public String forSegmentListAPI(String tableName) {
    return StringUtil.join("/", _baseUrl, "segments", tableName);
  }

  public String forInstancePartitions(String tableName, @Nullable InstancePartitionsType instancePartitionsType) {
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
    return String
        .format("%s?tableNameWithType=%s&batchConfigMapStr=%s", StringUtil.join("/", _baseUrl, "ingestFromFile"),
            tableNameWithType, URLEncoder.encode(batchConfigMapStr, StandardCharsets.UTF_8.toString()));
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
}
