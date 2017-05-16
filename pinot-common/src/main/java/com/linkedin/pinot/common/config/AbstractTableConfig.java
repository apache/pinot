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
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;


public abstract class AbstractTableConfig {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected String _tableName;
  protected TableType _tableType;
  protected SegmentsValidationAndRetentionConfig _validationConfig;
  protected TenantConfig _tenantConfig;
  protected IndexingConfig _indexingConfig;
  protected TableCustomConfig _customConfigs;
  protected QuotaConfig _quotaConfig;

  protected AbstractTableConfig(String tableName, TableType tableType,
      SegmentsValidationAndRetentionConfig validationConfig, TenantConfig tenantConfig, IndexingConfig indexingConfig,
      TableCustomConfig customConfigs, @Nullable QuotaConfig quotaConfig) {
    _tableName = TableNameBuilder.forType(tableType).tableNameWithType(tableName);
    _tableType = tableType;
    _validationConfig = validationConfig;
    _tenantConfig = tenantConfig;
    _indexingConfig = indexingConfig;
    _customConfigs = customConfigs;
    _quotaConfig = quotaConfig;
  }

  public static AbstractTableConfig init(String jsonString)
      throws JSONException, IOException {
    JSONObject jsonConfig = new JSONObject(jsonString);
    TableType tableType = TableType.valueOf(jsonConfig.getString("tableType").toUpperCase());
    String tableName = TableNameBuilder.forType(tableType).tableNameWithType(jsonConfig.getString("tableName"));
    SegmentsValidationAndRetentionConfig validationConfig =
        OBJECT_MAPPER.readValue(jsonConfig.getJSONObject("segmentsConfig").toString(),
            SegmentsValidationAndRetentionConfig.class);
    TenantConfig tenantConfig =
        OBJECT_MAPPER.readValue(jsonConfig.getJSONObject("tenants").toString(), TenantConfig.class);
    TableCustomConfig customConfig =
        OBJECT_MAPPER.readValue(jsonConfig.getJSONObject("metadata").toString(), TableCustomConfig.class);
    IndexingConfig indexingConfig =
        OBJECT_MAPPER.readValue(jsonConfig.getJSONObject("tableIndexConfig").toString(), IndexingConfig.class);
    QuotaConfig quotaConfig = null;
    if (jsonConfig.has(QuotaConfig.QUOTA_SECTION_NAME)) {
      quotaConfig = OBJECT_MAPPER.readValue(jsonConfig.getJSONObject(QuotaConfig.QUOTA_SECTION_NAME).toString(),
          QuotaConfig.class);
      quotaConfig.validate();
    }

    if (tableType == TableType.OFFLINE) {
      return new OfflineTableConfig(tableName, tableType, validationConfig, tenantConfig, indexingConfig, customConfig,
          quotaConfig);
    } else {
      return new RealtimeTableConfig(tableName, tableType, validationConfig, tenantConfig, indexingConfig, customConfig,
          quotaConfig);
    }
  }

  public static AbstractTableConfig fromZnRecord(ZNRecord record)
      throws JSONException, IOException {
    Map<String, String> simpleFields = record.getSimpleFields();
    JSONObject jsonConfig = new JSONObject();
    jsonConfig.put("tableName", simpleFields.get("tableName"));
    jsonConfig.put("tableType", simpleFields.get("tableType"));
    jsonConfig.put("segmentsConfig", new JSONObject(simpleFields.get("segmentsConfig")));
    jsonConfig.put("tenants", new JSONObject(simpleFields.get("tenants")));
    jsonConfig.put("tableIndexConfig", new JSONObject(simpleFields.get("tableIndexConfig")));
    jsonConfig.put("metadata", new JSONObject(simpleFields.get("metadata")));
    String quotaConfig = simpleFields.get(QuotaConfig.QUOTA_SECTION_NAME);
    if (quotaConfig != null) {
      jsonConfig.put(QuotaConfig.QUOTA_SECTION_NAME, new JSONObject(quotaConfig));
    }
    return init(jsonConfig.toString());
  }

  public static ZNRecord toZnRecord(AbstractTableConfig config)
      throws IOException {
    ZNRecord record = new ZNRecord(config.getTableName());
    Map<String, String> simpleFields = new HashMap<>();
    simpleFields.put("tableName", config.getTableName());
    simpleFields.put("tableType", config.getTableType().toString().toLowerCase());
    simpleFields.put("segmentsConfig", OBJECT_MAPPER.writeValueAsString(config.getValidationConfig()));
    simpleFields.put("tenants", OBJECT_MAPPER.writeValueAsString(config.getTenantConfig()));
    simpleFields.put("tableIndexConfig", OBJECT_MAPPER.writeValueAsString(config.getIndexingConfig()));
    simpleFields.put("metadata", OBJECT_MAPPER.writeValueAsString(config.getCustomConfigs()));
    if (config._quotaConfig != null) {
      simpleFields.put(QuotaConfig.QUOTA_SECTION_NAME, OBJECT_MAPPER.writeValueAsString(config.getQuotaConfig()));
    }
    record.setSimpleFields(simpleFields);
    return record;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  public void setTableType(TableType tableType) {
    _tableType = tableType;
  }

  public void setValidationConfig(SegmentsValidationAndRetentionConfig validationConfig) {
    _validationConfig = validationConfig;
  }

  public void setTenantConfig(TenantConfig tenantConfig) {
    _tenantConfig = tenantConfig;
  }

  public void setIndexingConfig(IndexingConfig indexingConfig) {
    _indexingConfig = indexingConfig;
  }

  public void setCustomConfigs(TableCustomConfig customConfigs) {
    _customConfigs = customConfigs;
  }

  public void setQuotaConfig(@Nullable QuotaConfig quotaConfig) {
    _quotaConfig = quotaConfig;
  }

  public String getTableName() {
    return _tableName;
  }

  public TableType getTableType() {
    return _tableType;
  }

  public SegmentsValidationAndRetentionConfig getValidationConfig() {
    return _validationConfig;
  }

  public TenantConfig getTenantConfig() {
    return _tenantConfig;
  }

  public IndexingConfig getIndexingConfig() {
    return _indexingConfig;
  }

  public TableCustomConfig getCustomConfigs() {
    return _customConfigs;
  }

  @Nullable
  public QuotaConfig getQuotaConfig() {
    return _quotaConfig;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("tableName: ").append(_tableName).append('\n');
    stringBuilder.append("tableType: ").append(_tableType).append('\n');
    stringBuilder.append("segmentsConfig: ").append(_validationConfig).append('\n');
    stringBuilder.append("tenants: ").append(_tenantConfig).append('\n');
    stringBuilder.append("tableIndexConfig: ").append(_indexingConfig).append('\n');
    stringBuilder.append("metadata: ").append(_customConfigs).append('\n');
    if (_quotaConfig != null) {
      stringBuilder.append("quota: ").append(_quotaConfig).append('\n');
    }
    return stringBuilder.toString();
  }

  public JSONObject toJSON()
      throws JSONException, IOException {
    JSONObject jsonConfig = new JSONObject();
    jsonConfig.put("tableName", _tableName);
    jsonConfig.put("tableType", _tableType.toString().toLowerCase());
    jsonConfig.put("segmentsConfig", new JSONObject(OBJECT_MAPPER.writeValueAsString(_validationConfig)));
    jsonConfig.put("tenants", new JSONObject(OBJECT_MAPPER.writeValueAsString(_tenantConfig)));
    jsonConfig.put("tableIndexConfig", new JSONObject(OBJECT_MAPPER.writeValueAsString(_indexingConfig)));
    jsonConfig.put("metadata", new JSONObject(OBJECT_MAPPER.writeValueAsString(_customConfigs)));
    if (_quotaConfig != null) {
      jsonConfig.put(QuotaConfig.QUOTA_SECTION_NAME, new JSONObject(OBJECT_MAPPER.writeValueAsString(_quotaConfig)));
    }
    return jsonConfig;
  }
}
