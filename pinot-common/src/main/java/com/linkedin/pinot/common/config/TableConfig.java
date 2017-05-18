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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.helix.ZNRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;


public class TableConfig {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String TABLE_NAME_KEY = "tableName";
  private static final String TABLE_TYPE_KEY = "tableType";
  private static final String VALIDATION_CONFIG_KEY = "segmentsConfig";
  private static final String TENANT_CONFIG_KEY = "tenants";
  private static final String INDEXING_CONFIG_KEY = "tableIndexConfig";
  private static final String CUSTOM_CONFIG_KEY = "metadata";
  private static final String QUOTA_CONFIG_KEY = "quota";

  private String _tableName;
  private TableType _tableType;
  private SegmentsValidationAndRetentionConfig _validationConfig;
  private TenantConfig _tenantConfig;
  private IndexingConfig _indexingConfig;
  private TableCustomConfig _customConfig;
  private QuotaConfig _quotaConfig;

  private TableConfig(@Nonnull String tableName, @Nonnull TableType tableType,
      @Nonnull SegmentsValidationAndRetentionConfig validationConfig, @Nonnull TenantConfig tenantConfig,
      @Nonnull IndexingConfig indexingConfig, @Nonnull TableCustomConfig customConfig,
      @Nullable QuotaConfig quotaConfig) {
    _tableName = TableNameBuilder.forType(tableType).tableNameWithType(tableName);
    _tableType = tableType;
    _validationConfig = validationConfig;
    _tenantConfig = tenantConfig;
    _indexingConfig = indexingConfig;
    _customConfig = customConfig;
    _quotaConfig = quotaConfig;
  }

  @Nonnull
  public static TableConfig init(@Nonnull String jsonString)
      throws IOException, JSONException {
    JSONObject jsonConfig = new JSONObject(jsonString);

    TableType tableType = TableType.valueOf(jsonConfig.getString(TABLE_TYPE_KEY).toUpperCase());
    String tableName = TableNameBuilder.forType(tableType).tableNameWithType(jsonConfig.getString(TABLE_NAME_KEY));
    SegmentsValidationAndRetentionConfig validationConfig =
        OBJECT_MAPPER.readValue(jsonConfig.getJSONObject(VALIDATION_CONFIG_KEY).toString(),
            SegmentsValidationAndRetentionConfig.class);
    TenantConfig tenantConfig =
        OBJECT_MAPPER.readValue(jsonConfig.getJSONObject(TENANT_CONFIG_KEY).toString(), TenantConfig.class);
    IndexingConfig indexingConfig =
        OBJECT_MAPPER.readValue(jsonConfig.getJSONObject(INDEXING_CONFIG_KEY).toString(), IndexingConfig.class);
    TableCustomConfig customConfig =
        OBJECT_MAPPER.readValue(jsonConfig.getJSONObject(CUSTOM_CONFIG_KEY).toString(), TableCustomConfig.class);
    QuotaConfig quotaConfig = null;
    if (jsonConfig.has(QUOTA_CONFIG_KEY)) {
      quotaConfig = OBJECT_MAPPER.readValue(jsonConfig.getJSONObject(QUOTA_CONFIG_KEY).toString(), QuotaConfig.class);
      quotaConfig.validate();
    }

    return new TableConfig(tableName, tableType, validationConfig, tenantConfig, indexingConfig, customConfig,
        quotaConfig);
  }

  @Nonnull
  public static TableConfig fromZnRecord(@Nonnull ZNRecord record)
      throws IOException, JSONException {
    Map<String, String> simpleFields = record.getSimpleFields();
    JSONObject jsonConfig = new JSONObject();
    jsonConfig.put(TABLE_NAME_KEY, simpleFields.get(TABLE_NAME_KEY));
    jsonConfig.put(TABLE_TYPE_KEY, simpleFields.get(TABLE_TYPE_KEY));
    jsonConfig.put(VALIDATION_CONFIG_KEY, new JSONObject(simpleFields.get(VALIDATION_CONFIG_KEY)));
    jsonConfig.put(TENANT_CONFIG_KEY, new JSONObject(simpleFields.get(TENANT_CONFIG_KEY)));
    jsonConfig.put(INDEXING_CONFIG_KEY, new JSONObject(simpleFields.get(INDEXING_CONFIG_KEY)));
    jsonConfig.put(CUSTOM_CONFIG_KEY, new JSONObject(simpleFields.get(CUSTOM_CONFIG_KEY)));
    String quotaConfig = simpleFields.get(QUOTA_CONFIG_KEY);
    if (quotaConfig != null) {
      jsonConfig.put(QUOTA_CONFIG_KEY, new JSONObject(quotaConfig));
    }
    return init(jsonConfig.toString());
  }

  @Nonnull
  public static ZNRecord toZnRecord(@Nonnull TableConfig config)
      throws IOException {
    ZNRecord record = new ZNRecord(config.getTableName());
    Map<String, String> simpleFields = new HashMap<>();
    simpleFields.put(TABLE_NAME_KEY, config._tableName);
    simpleFields.put(TABLE_TYPE_KEY, config._tableType.toString().toLowerCase());
    simpleFields.put(VALIDATION_CONFIG_KEY, OBJECT_MAPPER.writeValueAsString(config._validationConfig));
    simpleFields.put(TENANT_CONFIG_KEY, OBJECT_MAPPER.writeValueAsString(config._tenantConfig));
    simpleFields.put(INDEXING_CONFIG_KEY, OBJECT_MAPPER.writeValueAsString(config._indexingConfig));
    simpleFields.put(CUSTOM_CONFIG_KEY, OBJECT_MAPPER.writeValueAsString(config._customConfig));
    if (config._quotaConfig != null) {
      simpleFields.put(QUOTA_CONFIG_KEY, OBJECT_MAPPER.writeValueAsString(config._quotaConfig));
    }
    record.setSimpleFields(simpleFields);
    return record;
  }

  @Nonnull
  public String getTableName() {
    return _tableName;
  }

  public void setTableName(@Nonnull String tableName) {
    _tableName = tableName;
  }

  @Nonnull
  public TableType getTableType() {
    return _tableType;
  }

  public void setTableType(@Nonnull TableType tableType) {
    _tableType = tableType;
  }

  @Nonnull
  public SegmentsValidationAndRetentionConfig getValidationConfig() {
    return _validationConfig;
  }

  public void setValidationConfig(@Nonnull SegmentsValidationAndRetentionConfig validationConfig) {
    _validationConfig = validationConfig;
  }

  @Nonnull
  public TenantConfig getTenantConfig() {
    return _tenantConfig;
  }

  public void setTenantConfig(@Nonnull TenantConfig tenantConfig) {
    _tenantConfig = tenantConfig;
  }

  @Nonnull
  public IndexingConfig getIndexingConfig() {
    return _indexingConfig;
  }

  public void setIndexingConfig(@Nonnull IndexingConfig indexingConfig) {
    _indexingConfig = indexingConfig;
  }

  @Nonnull
  public TableCustomConfig getCustomConfig() {
    return _customConfig;
  }

  public void setCustomConfig(@Nonnull TableCustomConfig customConfig) {
    _customConfig = customConfig;
  }

  @Nullable
  public QuotaConfig getQuotaConfig() {
    return _quotaConfig;
  }

  public void setQuotaConfig(@Nullable QuotaConfig quotaConfig) {
    _quotaConfig = quotaConfig;
  }

  @Nonnull
  public JSONObject toJSON()
      throws IOException, JSONException {
    JSONObject jsonConfig = new JSONObject();
    jsonConfig.put(TABLE_NAME_KEY, _tableName);
    jsonConfig.put(TABLE_TYPE_KEY, _tableType.toString().toLowerCase());
    jsonConfig.put(VALIDATION_CONFIG_KEY, new JSONObject(OBJECT_MAPPER.writeValueAsString(_validationConfig)));
    jsonConfig.put(TENANT_CONFIG_KEY, new JSONObject(OBJECT_MAPPER.writeValueAsString(_tenantConfig)));
    jsonConfig.put(INDEXING_CONFIG_KEY, new JSONObject(OBJECT_MAPPER.writeValueAsString(_indexingConfig)));
    jsonConfig.put(CUSTOM_CONFIG_KEY, new JSONObject(OBJECT_MAPPER.writeValueAsString(_customConfig)));
    if (_quotaConfig != null) {
      jsonConfig.put(QUOTA_CONFIG_KEY, new JSONObject(OBJECT_MAPPER.writeValueAsString(_quotaConfig)));
    }
    return jsonConfig;
  }

  @Override
  public String toString() {
    try {
      return toJSON().toString(2);
    } catch (Exception e) {
      return e.toString();
    }
  }
}
