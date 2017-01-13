/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.config;

import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.ZNRecord;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractTableConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTableConfig.class);
  private static final String TABLE_TYPE_REALTIME = "realtime";

  protected String tableName;
  protected String tableType;
  protected SegmentsValidationAndRetentionConfig validationConfig;
  protected TenantConfig tenantConfig;
  protected TableCustomConfig customConfigs;
  protected final @Nullable QuotaConfig quotaConfig;

  protected AbstractTableConfig(String tableName, String tableType,
      SegmentsValidationAndRetentionConfig validationConfig,
      TenantConfig tenantConfig, TableCustomConfig customConfigs,
      @Nullable QuotaConfig quotaConfig) {
    this.tableName = new TableNameBuilder(TableType.valueOf(tableType.toUpperCase())).forTable(tableName);
    this.tableType = tableType;
    this.validationConfig = validationConfig;
    this.tenantConfig = tenantConfig;
    this.customConfigs = customConfigs;
    this.quotaConfig = quotaConfig;
  }

  public static AbstractTableConfig init(String jsonString) throws JSONException, IOException {
    JSONObject tableJson = new JSONObject(jsonString);
    String tableType = tableJson.getString("tableType").toLowerCase();
    String tableName =
        new TableNameBuilder(TableType.valueOf(tableType.toUpperCase())).forTable(tableJson.getString("tableName"));
    SegmentsValidationAndRetentionConfig validationConfig =
        loadSegmentsConfig(new ObjectMapper().readTree(tableJson.getJSONObject("segmentsConfig").toString()));
    TenantConfig tenantConfig = loadTenantsConfig(new ObjectMapper().readTree(tableJson.getJSONObject("tenants").toString()));
    TableCustomConfig customConfig =
        loadCustomConfig(new ObjectMapper().readTree(tableJson.getJSONObject("metadata").toString()));
    IndexingConfig indexingConfig =
        loadIndexingConfig(new ObjectMapper().readTree(tableJson.getJSONObject("tableIndexConfig").toString()));
    QuotaConfig quotaConfig = null;
    if (tableJson.has(QuotaConfig.QUOTA_SECTION_NAME)) {
      quotaConfig = loadQuotaConfig(new ObjectMapper().readTree(
          tableJson.getJSONObject(QuotaConfig.QUOTA_SECTION_NAME).toString()));
    }

    if (tableType.equals("offline")) {
      return new OfflineTableConfig(tableName, tableType, validationConfig, tenantConfig, customConfig, indexingConfig, quotaConfig);
    } else if (tableType.equals(TABLE_TYPE_REALTIME)) {
      return new RealtimeTableConfig(tableName, tableType, validationConfig, tenantConfig, customConfig, indexingConfig, quotaConfig);
    }
    throw new UnsupportedOperationException("unknown tableType : " + tableType);
  }

  public static AbstractTableConfig fromZnRecord(ZNRecord record) throws JsonParseException, JsonMappingException,
      JsonProcessingException, JSONException, IOException {
    Map<String, String> simpleFields = record.getSimpleFields();
    JSONObject str = new JSONObject();
    str.put("tableName", simpleFields.get("tableName"));
    str.put("tableType", simpleFields.get("tableType"));
    str.put("segmentsConfig", new JSONObject(simpleFields.get("segmentsConfig")));
    str.put("tenants", new JSONObject(simpleFields.get("tenants")));
    str.put("tableIndexConfig", new JSONObject(simpleFields.get("tableIndexConfig")));
    str.put("metadata", new JSONObject(simpleFields.get("metadata")));
    String quotaConfig = simpleFields.get(QuotaConfig.QUOTA_SECTION_NAME);
    if (quotaConfig != null) {
      str.put(QuotaConfig.QUOTA_SECTION_NAME, new JSONObject(quotaConfig));
    }
    return init(str.toString());
  }

  public static ZNRecord toZnRecord(AbstractTableConfig config) throws JsonGenerationException, JsonMappingException,
      IOException {
    ZNRecord rec = new ZNRecord(config.getTableName());
    Map<String, String> rawMap = new HashMap<String, String>();
    rawMap.put("tableName", config.getTableName());
    rawMap.put("tableType", config.getTableType());
    rawMap.put("segmentsConfig", new ObjectMapper().writeValueAsString(config.getValidationConfig()));
    rawMap.put("tenants", new ObjectMapper().writeValueAsString(config.getTenantConfig()));
    rawMap.put("metadata", new ObjectMapper().writeValueAsString(config.getCustomConfigs()));
    rawMap.put("tableIndexConfig", new ObjectMapper().writeValueAsString(config.getIndexingConfig()));
    if (config.quotaConfig != null) {
      rawMap.put("quota", new ObjectMapper().writeValueAsString(config.getQuotaConfig()));
    }
    rec.setSimpleFields(rawMap);
    return rec;
  }

  public static SegmentsValidationAndRetentionConfig loadSegmentsConfig(JsonNode node) throws JsonParseException,
      JsonMappingException, IOException {
    return new ObjectMapper().readValue(node, SegmentsValidationAndRetentionConfig.class);
  }

  public static TenantConfig loadTenantsConfig(JsonNode node) throws JsonParseException, JsonMappingException,
      IOException {
    return new ObjectMapper().readValue(node, TenantConfig.class);
  }

  public static TableCustomConfig loadCustomConfig(JsonNode node) throws JsonParseException, JsonMappingException,
      IOException {
    return new ObjectMapper().readValue(node, TableCustomConfig.class);
  }

  public static IndexingConfig loadIndexingConfig(JsonNode node) throws JsonParseException, JsonMappingException,
      IOException {
    return new ObjectMapper().readValue(node, IndexingConfig.class);
  }

  private static QuotaConfig loadQuotaConfig(JsonNode jsonNode)
      throws IOException {
    QuotaConfig quotaConfig = new ObjectMapper().readValue(jsonNode, QuotaConfig.class);
    quotaConfig.validate();
    return quotaConfig;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setTableType(String tableType) {
    this.tableType = tableType;
  }

  public void setValidationConfig(SegmentsValidationAndRetentionConfig validationConfig) {
    this.validationConfig = validationConfig;
  }

  public void setTenantConfig(TenantConfig tenantConfig) {
    this.tenantConfig = tenantConfig;
  }

  public void setCustomConfigs(TableCustomConfig customConfigs) {
    this.customConfigs = customConfigs;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTableType() {
    return tableType;
  }

  public boolean isRealTime() {
    return tableType.equalsIgnoreCase(TABLE_TYPE_REALTIME);
  }

  public SegmentsValidationAndRetentionConfig getValidationConfig() {
    return validationConfig;
  }

  public TenantConfig getTenantConfig() {
    return tenantConfig;
  }

  public TableCustomConfig getCustomConfigs() {
    return customConfigs;
  }

  public abstract IndexingConfig getIndexingConfig();

  public @Nullable QuotaConfig getQuotaConfig() {
    return quotaConfig;
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    result.append("tableName:" + tableName + "\n");
    result.append("tableType:" + tableType + "\n");
    result.append("tenant : " + tenantConfig.toString() + " \n");
    result.append("segments : " + validationConfig.toString() + "\n");
    result.append("customConfigs : " + customConfigs.toString() + "\n");
    if (quotaConfig != null) {
      result.append("quota : " + quotaConfig.toString() + "\n");
    }
    return result.toString();
  }

  public abstract JSONObject toJSON() throws JSONException, IOException;
}
