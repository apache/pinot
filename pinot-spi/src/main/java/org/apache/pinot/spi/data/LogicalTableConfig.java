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
package org.apache.pinot.spi.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.utils.JsonUtils;


public class LogicalTableConfig extends BaseJsonConfig {

  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

  public static final String LOGICAL_TABLE_NAME_KEY = "tableName";
  public static final String PHYSICAL_TABLE_CONFIG_KEY = "physicalTableConfigMap";
  public static final String BROKER_TENANT_KEY = "brokerTenant";
  public static final String QUERY_CONFIG_KEY = "query";
  public static final String QUOTA_CONFIG_KEY = "quota";
  public static final String REF_OFFLINE_TABLE_NAME_KEY = "refOfflineTableName";
  public static final String REF_REALTIME_TABLE_NAME_KEY = "refRealtimeTableName";

  private String _tableName;
  private String _brokerTenant;
  private Map<String, PhysicalTableConfig> _physicalTableConfigMap;
  @JsonProperty(QUERY_CONFIG_KEY)
  private QueryConfig _queryConfig;
  @JsonProperty(QUOTA_CONFIG_KEY)
  private QuotaConfig _quotaConfig;
  private String _refOfflineTableName;
  private String _refRealtimeTableName;

  public static LogicalTableConfig fromString(String logicalTableString)
      throws IOException {
    return JsonUtils.stringToObject(logicalTableString, LogicalTableConfig.class);
  }

  public String getTableName() {
    return _tableName;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  public Map<String, PhysicalTableConfig> getPhysicalTableConfigMap() {
    return _physicalTableConfigMap;
  }

  public void setPhysicalTableConfigMap(
      Map<String, PhysicalTableConfig> physicalTableConfigMap) {
    _physicalTableConfigMap = physicalTableConfigMap;
  }

  public String getBrokerTenant() {
    return _brokerTenant;
  }

  public void setBrokerTenant(String brokerTenant) {
    _brokerTenant = brokerTenant;
  }

  @JsonProperty(QUERY_CONFIG_KEY)
  @Nullable
  public QueryConfig getQueryConfig() {
    return _queryConfig;
  }

  public void setQueryConfig(QueryConfig queryConfig) {
    _queryConfig = queryConfig;
  }

  @JsonProperty(QUOTA_CONFIG_KEY)
  @Nullable
  public QuotaConfig getQuotaConfig() {
    return _quotaConfig;
  }

  public void setQuotaConfig(QuotaConfig quotaConfig) {
    _quotaConfig = quotaConfig;
  }

  public String getRefOfflineTableName() {
    return _refOfflineTableName;
  }

  public void setRefOfflineTableName(String refOfflineTableName) {
    _refOfflineTableName = refOfflineTableName;
  }

  public String getRefRealtimeTableName() {
    return _refRealtimeTableName;
  }

  public void setRefRealtimeTableName(String refRealtimeTableName) {
    _refRealtimeTableName = refRealtimeTableName;
  }

  private JsonNode toJsonObject() {
    return DEFAULT_MAPPER.valueToTree(this);
  }

  /**
   * Returns a single-line json string representation of the schema.
   */
  public String toSingleLineJsonString() {
    return toJsonObject().toString();
  }

  /**
   * Returns a pretty json string representation of the schema.
   */
  public String toPrettyJsonString() {
    try {
      return JsonUtils.objectToPrettyString(toJsonObject());
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return toSingleLineJsonString();
  }
}
