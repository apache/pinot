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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.config.ConfigRecord;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.LogicalTableConfigBuilder;


/**
 * Represents the configuration for a logical table in Pinot.
 *
 * <p>
 * <ul>
 *   <li><b>tableName</b>: The name of the logical table.</li>
 *   <li><b>physicalTableConfigMap</b>: A map of physical table names to their configurations.</li>
 *   <li><b>brokerTenant</b>: The tenant for the broker.</li>
 *   <li><b>queryConfig</b>: Configuration for query execution on the logical table.</li>
 *   <li><b>quotaConfig</b>: Configuration for quota management on the logical table.</li>
 *   <li><b>refOfflineTableName</b>: The name of the offline table whose table config is referenced by this logical
 *   table.</li>
 *   <li><b>refRealtimeTableName</b>: The name of the realtime table whose table config is referenced by this logical
 *   table.</li>
 *   <li><b>timeBoundaryConfig</b>: Configuration for time boundaries of the logical table. This is used to determine
 *   the time boundaries for queries on the logical table, especially in hybrid scenarios where both offline and
 *   realtime data are present.</li>
 * </ul>
 * </p>
 */
public class LogicalTableConfig extends BaseJsonConfig {

  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

  public static final String LOGICAL_TABLE_NAME_KEY = "tableName";
  public static final String PHYSICAL_TABLE_CONFIG_KEY = "physicalTableConfigMap";
  public static final String BROKER_TENANT_KEY = "brokerTenant";
  public static final String QUERY_CONFIG_KEY = "query";
  public static final String QUOTA_CONFIG_KEY = "quota";
  public static final String REF_OFFLINE_TABLE_NAME_KEY = "refOfflineTableName";
  public static final String REF_REALTIME_TABLE_NAME_KEY = "refRealtimeTableName";
  public static final String TIME_BOUNDARY_CONFIG_KEY = "timeBoundaryConfig";

  private String _tableName;
  private String _brokerTenant;
  private Map<String, PhysicalTableConfig> _physicalTableConfigMap;
  @JsonProperty(QUERY_CONFIG_KEY)
  private QueryConfig _queryConfig;
  @JsonProperty(QUOTA_CONFIG_KEY)
  private QuotaConfig _quotaConfig;
  private String _refOfflineTableName;
  private String _refRealtimeTableName;
  private TimeBoundaryConfig _timeBoundaryConfig;

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

  @Nullable
  public String getRefOfflineTableName() {
    return _refOfflineTableName;
  }

  public void setRefOfflineTableName(String refOfflineTableName) {
    _refOfflineTableName = refOfflineTableName;
  }

  @Nullable
  public String getRefRealtimeTableName() {
    return _refRealtimeTableName;
  }

  public void setRefRealtimeTableName(String refRealtimeTableName) {
    _refRealtimeTableName = refRealtimeTableName;
  }

  @Nullable
  public TimeBoundaryConfig getTimeBoundaryConfig() {
    return _timeBoundaryConfig;
  }

  public void setTimeBoundaryConfig(TimeBoundaryConfig timeBoundaryConfig) {
    _timeBoundaryConfig = timeBoundaryConfig;
  }

  public static LogicalTableConfig fromConfigRecord(ConfigRecord record)
      throws IOException {
    LogicalTableConfigBuilder builder = new LogicalTableConfigBuilder()
        .setTableName(record.getSimpleField(LOGICAL_TABLE_NAME_KEY))
        .setBrokerTenant(record.getSimpleField(BROKER_TENANT_KEY));

    if (record.getSimpleField(QUERY_CONFIG_KEY) != null) {
      builder.setQueryConfig(JsonUtils.stringToObject(record.getSimpleField(QUERY_CONFIG_KEY), QueryConfig.class));
    }
    if (record.getSimpleField(QUOTA_CONFIG_KEY) != null) {
      builder.setQuotaConfig(JsonUtils.stringToObject(record.getSimpleField(QUOTA_CONFIG_KEY), QuotaConfig.class));
    }
    if (record.getSimpleField(REF_OFFLINE_TABLE_NAME_KEY) != null) {
      builder.setRefOfflineTableName(record.getSimpleField(REF_OFFLINE_TABLE_NAME_KEY));
    }
    if (record.getSimpleField(REF_REALTIME_TABLE_NAME_KEY) != null) {
      builder.setRefRealtimeTableName(record.getSimpleField(REF_REALTIME_TABLE_NAME_KEY));
    }
    String timeBoundaryConfigJson = record.getSimpleField(TIME_BOUNDARY_CONFIG_KEY);
    if (timeBoundaryConfigJson != null) {
      builder.setTimeBoundaryConfig(JsonUtils.stringToObject(timeBoundaryConfigJson, TimeBoundaryConfig.class));
    }

    Map<String, PhysicalTableConfig> physicalTableConfigMap = new HashMap<>();
    Map<String, String> physicalTableMapField = record.getMapField(PHYSICAL_TABLE_CONFIG_KEY);
    if (physicalTableMapField != null) {
      for (Map.Entry<String, String> entry : physicalTableMapField.entrySet()) {
        String physicalTableName = entry.getKey();
        String physicalTableConfigJson = entry.getValue();
        physicalTableConfigMap.put(physicalTableName,
            JsonUtils.stringToObject(physicalTableConfigJson, PhysicalTableConfig.class));
      }
    }
    builder.setPhysicalTableConfigMap(physicalTableConfigMap);
    return builder.build();
  }

  public ConfigRecord toConfigRecord()
      throws JsonProcessingException {
    Map<String, String> simpleFields = new HashMap<>();
    simpleFields.put(LOGICAL_TABLE_NAME_KEY, getTableName());
    simpleFields.put(BROKER_TENANT_KEY, getBrokerTenant());

    if (getQueryConfig() != null) {
      simpleFields.put(QUERY_CONFIG_KEY, getQueryConfig().toJsonString());
    }
    if (getQuotaConfig() != null) {
      simpleFields.put(QUOTA_CONFIG_KEY, getQuotaConfig().toJsonString());
    }
    if (getRefOfflineTableName() != null) {
      simpleFields.put(REF_OFFLINE_TABLE_NAME_KEY, getRefOfflineTableName());
    }
    if (getRefRealtimeTableName() != null) {
      simpleFields.put(REF_REALTIME_TABLE_NAME_KEY, getRefRealtimeTableName());
    }
    if (getTimeBoundaryConfig() != null) {
      simpleFields.put(TIME_BOUNDARY_CONFIG_KEY, getTimeBoundaryConfig().toJsonString());
    }

    Map<String, String> physicalTableConfigMapField = new HashMap<>();
    for (Map.Entry<String, PhysicalTableConfig> entry : getPhysicalTableConfigMap().entrySet()) {
      physicalTableConfigMapField.put(entry.getKey(), entry.getValue().toJsonString());
    }

    Map<String, Map<String, String>> mapFields = new HashMap<>();
    mapFields.put(PHYSICAL_TABLE_CONFIG_KEY, physicalTableConfigMapField);

    return new ConfigRecord(getTableName(), simpleFields, mapFields);
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

  @JsonIgnore
  public boolean isHybridLogicalTable() {
    return _refOfflineTableName != null && _refRealtimeTableName != null;
  }

  @Override
  public String toString() {
    return toSingleLineJsonString();
  }
}
