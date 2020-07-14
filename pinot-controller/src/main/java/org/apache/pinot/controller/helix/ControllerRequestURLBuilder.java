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

import javax.annotation.Nullable;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfigConstants;
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

  public String forTableReload(String tableName, String tableType) {
    String query = "reload?type=" + tableType;
    return StringUtil.join("/", _baseUrl, "tables", tableName, "segments", query);
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

  public String forSchemaCreate() {
    return StringUtil.join("/", _baseUrl, "schemas");
  }

  public String forSchemaUpdate(String schemaName) {
    return StringUtil.join("/", _baseUrl, "schemas", schemaName);
  }

  public String forSchemaGet(String schemaName) {
    return StringUtil.join("/", _baseUrl, "schemas", schemaName);
  }

  public String forSegmentDownload(String tableName, String segmentName) {
    return URIUtils.constructDownloadUrl(_baseUrl, tableName, segmentName);
  }

  public String forSegmentDelete(String resourceName, String segmentName) {
    return StringUtil.join("/", _baseUrl, "datafiles", resourceName, segmentName);
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
}
