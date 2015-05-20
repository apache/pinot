package com.linkedin.pinot.common.config;

import java.io.IOException;

import org.apache.helix.ZNRecord;
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

  private final String tableName;
  private final String tableType;
  private final SegmentsValidationAndRetentionConfig validationConfig;
  private final TenantConfig tenantConfig;
  private final TableCustomConfig customConfigs;
  private final String jsonString;

  protected AbstractTableConfig(String tableName, String tableType,
      SegmentsValidationAndRetentionConfig validationConfig, TenantConfig tenantConfig,
      TableCustomConfig customConfigs, String jsonString) {
    this.tableName = tableName;
    this.tableType = tableType;
    this.validationConfig = validationConfig;
    this.tenantConfig = tenantConfig;
    this.customConfigs = customConfigs;
    this.jsonString = jsonString;
  }

  public static AbstractTableConfig init(String jsonString) throws JSONException, JsonParseException,
      JsonMappingException, JsonProcessingException, IOException {
    JSONObject o = new JSONObject(jsonString);
    String tableName = o.getString("tableName");
    String tableType = o.getString("tableType").toLowerCase();
    SegmentsValidationAndRetentionConfig validationConfig =
        loadSegmentsConfig(new ObjectMapper().readTree(o.getJSONObject("segmentsConfig").toString()));
    TenantConfig tenantConfig = loadTenantsConfig(new ObjectMapper().readTree(o.getJSONObject("tenants").toString()));
    TableCustomConfig customConfig =
        loadCustomConfig(new ObjectMapper().readTree(o.getJSONObject("metadata").toString()));
    IndexingConfig config =
        loadIndexingConfig(new ObjectMapper().readTree(o.getJSONObject("tableIndexConfig").toString()));
    if (tableType.equals("offline")) {
      return new RealtimeTableConfig(tableName, tableType, validationConfig, tenantConfig, customConfig, jsonString,
          config);
    } else if (tableType.equals("realtime")) {
      return new OfflineTableConfig(tableName, tableType, validationConfig, tenantConfig, customConfig, jsonString,
          config);
    }
    throw new UnsupportedOperationException("unknown tableType : " + tableType);
  }

  public static AbstractTableConfig fromZnRecord(ZNRecord record) throws JsonParseException, JsonMappingException,
      JsonProcessingException, JSONException, IOException {
    String jsonString = record.getSimpleField("configs");
    return init(jsonString);
  }

  public static ZNRecord toZnRecord(AbstractTableConfig config) {
    ZNRecord rec = new ZNRecord(config.getTableName() + "_" + config.getTableType());
    rec.setSimpleField("configs", config.getJsonString());
    return rec;
  }

  private static SegmentsValidationAndRetentionConfig loadSegmentsConfig(JsonNode node) throws JsonParseException,
      JsonMappingException, IOException {
    return new ObjectMapper().readValue(node, SegmentsValidationAndRetentionConfig.class);
  }

  private static TenantConfig loadTenantsConfig(JsonNode node) throws JsonParseException, JsonMappingException,
      IOException {
    return new ObjectMapper().readValue(node, TenantConfig.class);
  }

  private static TableCustomConfig loadCustomConfig(JsonNode node) throws JsonParseException, JsonMappingException,
      IOException {
    return new ObjectMapper().readValue(node, TableCustomConfig.class);
  }

  private static IndexingConfig loadIndexingConfig(JsonNode node) throws JsonParseException, JsonMappingException,
      IOException {
    return new ObjectMapper().readValue(node, IndexingConfig.class);
  }

  public String getJsonString() {
    return jsonString;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTableType() {
    return tableType;
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

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    result.append("tableName:" + tableName + "\n");
    result.append("tableType:" + tableType + "\n");
    result.append("tenant : " + tenantConfig.toString() + " \n");
    result.append("segments : " + validationConfig.toString() + "\n");
    result.append("customConfigs : " + customConfigs.toString() + "\n");
    return result.toString();
  }
}
