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
package org.apache.pinot.common.utils.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.DimensionTableConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.TunerConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.SegmentAssignmentConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.sampler.TableSamplerConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Util class for serializing and deserializing [TableConfig] to and from [ZNRecord].
public class TableConfigSerDeUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableConfigSerDeUtils.class);

  private TableConfigSerDeUtils() {
  }

  private static final String FIELD_MISSING_MESSAGE_TEMPLATE = "Mandatory field '%s' is missing";

  public static TableConfig fromZNRecord(ZNRecord znRecord)
      throws IOException {
    Map<String, String> simpleFields = znRecord.getSimpleFields();

    // Mandatory fields
    String tableName = znRecord.getId();

    String tableType = simpleFields.get(TableConfig.TABLE_TYPE_KEY);
    boolean isDimTable = Boolean.parseBoolean(simpleFields.get(TableConfig.IS_DIM_TABLE_KEY));
    boolean isMaterializedView =
        Boolean.parseBoolean(simpleFields.get(TableConfig.IS_MATERIALIZED_VIEW_KEY));
    Preconditions.checkState(tableType != null, FIELD_MISSING_MESSAGE_TEMPLATE, TableConfig.TABLE_TYPE_KEY);

    String validationConfigString = simpleFields.get(TableConfig.VALIDATION_CONFIG_KEY);
    Preconditions.checkState(validationConfigString != null, FIELD_MISSING_MESSAGE_TEMPLATE,
        TableConfig.VALIDATION_CONFIG_KEY);
    SegmentsValidationAndRetentionConfig validationConfig =
        JsonUtils.stringToObject(validationConfigString, SegmentsValidationAndRetentionConfig.class);

    String tenantConfigString = simpleFields.get(TableConfig.TENANT_CONFIG_KEY);
    Preconditions.checkState(tenantConfigString != null, FIELD_MISSING_MESSAGE_TEMPLATE, TableConfig.TENANT_CONFIG_KEY);
    TenantConfig tenantConfig = JsonUtils.stringToObject(tenantConfigString, TenantConfig.class);

    String indexingConfigString = simpleFields.get(TableConfig.INDEXING_CONFIG_KEY);
    Preconditions.checkState(indexingConfigString != null, FIELD_MISSING_MESSAGE_TEMPLATE,
        TableConfig.INDEXING_CONFIG_KEY);
    IndexingConfig indexingConfig = JsonUtils.stringToObject(indexingConfigString, IndexingConfig.class);

    String customConfigString = simpleFields.get(TableConfig.CUSTOM_CONFIG_KEY);
    Preconditions.checkState(customConfigString != null, FIELD_MISSING_MESSAGE_TEMPLATE, TableConfig.CUSTOM_CONFIG_KEY);
    TableCustomConfig customConfig = JsonUtils.stringToObject(customConfigString, TableCustomConfig.class);

    // Optional fields
    QuotaConfig quotaConfig = null;
    String quotaConfigString = simpleFields.get(TableConfig.QUOTA_CONFIG_KEY);
    if (quotaConfigString != null) {
      quotaConfig = JsonUtils.stringToObject(quotaConfigString, QuotaConfig.class);
    }

    TableTaskConfig taskConfig = null;
    String taskConfigString = simpleFields.get(TableConfig.TASK_CONFIG_KEY);
    if (taskConfigString != null) {
      taskConfig = JsonUtils.stringToObject(taskConfigString, TableTaskConfig.class);
    }

    RoutingConfig routingConfig = null;
    String routingConfigString = simpleFields.get(TableConfig.ROUTING_CONFIG_KEY);
    if (routingConfigString != null) {
      routingConfig = JsonUtils.stringToObject(routingConfigString, RoutingConfig.class);
    }

    QueryConfig queryConfig = null;
    String queryConfigString = simpleFields.get(TableConfig.QUERY_CONFIG_KEY);
    if (queryConfigString != null) {
      queryConfig = JsonUtils.stringToObject(queryConfigString, QueryConfig.class);
    }

    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = null;
    String instanceAssignmentConfigMapString = simpleFields.get(TableConfig.INSTANCE_ASSIGNMENT_CONFIG_MAP_KEY);
    if (instanceAssignmentConfigMapString != null) {
      instanceAssignmentConfigMap = JsonUtils.stringToObject(instanceAssignmentConfigMapString, new TypeReference<>() {
      });
    }

    List<FieldConfig> fieldConfigList = null;
    String fieldConfigListString = simpleFields.get(TableConfig.FIELD_CONFIG_LIST_KEY);
    if (fieldConfigListString != null) {
      fieldConfigList = JsonUtils.stringToObject(fieldConfigListString, new TypeReference<>() {
      });
    }

    UpsertConfig upsertConfig = null;
    String upsertConfigString = simpleFields.get(TableConfig.UPSERT_CONFIG_KEY);
    if (upsertConfigString != null) {
      upsertConfig = JsonUtils.stringToObject(upsertConfigString, UpsertConfig.class);
    }

    DedupConfig dedupConfig = null;
    String dedupConfigString = simpleFields.get(TableConfig.DEDUP_CONFIG_KEY);
    if (dedupConfigString != null) {
      dedupConfig = JsonUtils.stringToObject(dedupConfigString, DedupConfig.class);
    }

    DimensionTableConfig dimensionTableConfig = null;
    String dimensionTableConfigString = simpleFields.get(TableConfig.DIMENSION_TABLE_CONFIG_KEY);
    if (dimensionTableConfigString != null) {
      dimensionTableConfig = JsonUtils.stringToObject(dimensionTableConfigString, DimensionTableConfig.class);
    }

    IngestionConfig ingestionConfig = null;
    String ingestionConfigString = simpleFields.get(TableConfig.INGESTION_CONFIG_KEY);
    if (ingestionConfigString != null) {
      ingestionConfig = JsonUtils.stringToObject(ingestionConfigString, IngestionConfig.class);
    }

    List<TierConfig> tierConfigList = null;
    String tierConfigListString = simpleFields.get(TableConfig.TIER_CONFIGS_LIST_KEY);
    if (tierConfigListString != null) {
      tierConfigList = JsonUtils.stringToObject(tierConfigListString, new TypeReference<>() {
      });
    }

    List<TunerConfig> tunerConfigList = null;
    String tunerConfigListString = simpleFields.get(TableConfig.TUNER_CONFIG_LIST_KEY);
    if (tunerConfigListString != null) {
      tunerConfigList = JsonUtils.stringToObject(tunerConfigListString, new TypeReference<>() {
      });
    }

    Map<InstancePartitionsType, String> instancePartitionsMap = null;
    String instancePartitionsMapString = simpleFields.get(TableConfig.INSTANCE_PARTITIONS_MAP_CONFIG_KEY);
    if (instancePartitionsMapString != null) {
      instancePartitionsMap = JsonUtils.stringToObject(instancePartitionsMapString, new TypeReference<>() {
      });
    }

    Map<String, SegmentAssignmentConfig> segmentAssignmentConfigMap = null;
    String segmentAssignmentConfigMapString = simpleFields.get(TableConfig.SEGMENT_ASSIGNMENT_CONFIG_MAP_KEY);
    if (segmentAssignmentConfigMapString != null) {
      segmentAssignmentConfigMap = JsonUtils.stringToObject(segmentAssignmentConfigMapString, new TypeReference<>() {
      });
    }

    List<TableSamplerConfig> tableSamplerConfigs = null;
    String tableSamplerConfigsString = simpleFields.get(TableConfig.TABLE_SAMPLERS_KEY);
    if (tableSamplerConfigsString != null) {
      tableSamplerConfigs = JsonUtils.stringToObject(tableSamplerConfigsString, new TypeReference<>() {
      });
    }

    String description = simpleFields.get(TableConfig.DESCRIPTION_KEY);

    List<String> tags = null;
    String tagsString = simpleFields.get(TableConfig.TAGS_KEY);
    if (tagsString != null) {
      tags = JsonUtils.stringToObject(tagsString, new TypeReference<>() {
      });
    }

    TableConfig tableConfig =
        new TableConfig(tableName, tableType, validationConfig, tenantConfig, indexingConfig, customConfig,
            quotaConfig, taskConfig, routingConfig, queryConfig, instanceAssignmentConfigMap, fieldConfigList,
            upsertConfig, dedupConfig, dimensionTableConfig, ingestionConfig, tierConfigList, isDimTable,
            tunerConfigList, instancePartitionsMap, segmentAssignmentConfigMap,
            tableSamplerConfigs, isMaterializedView);
    tableConfig.setDescription(description);
    tableConfig.setTags(tags);
    return tableConfig;
  }

  public static ZNRecord toZNRecord(TableConfig tableConfig)
      throws JsonProcessingException {
    Map<String, String> simpleFields = new HashMap<>();

    // Mandatory fields
    simpleFields.put(TableConfig.TABLE_NAME_KEY, tableConfig.getTableName());
    simpleFields.put(TableConfig.TABLE_TYPE_KEY, tableConfig.getTableType().toString());
    simpleFields.put(TableConfig.VALIDATION_CONFIG_KEY, tableConfig.getValidationConfig().toJsonString());
    simpleFields.put(TableConfig.TENANT_CONFIG_KEY, tableConfig.getTenantConfig().toJsonString());
    simpleFields.put(TableConfig.INDEXING_CONFIG_KEY, tableConfig.getIndexingConfig().toJsonString());
    simpleFields.put(TableConfig.CUSTOM_CONFIG_KEY, tableConfig.getCustomConfig().toJsonString());
    simpleFields.put(TableConfig.IS_DIM_TABLE_KEY, Boolean.toString(tableConfig.isDimTable()));
    simpleFields.put(TableConfig.IS_MATERIALIZED_VIEW_KEY, Boolean.toString(tableConfig.isMaterializedView()));

    // Optional fields
    QuotaConfig quotaConfig = tableConfig.getQuotaConfig();
    if (quotaConfig != null) {
      simpleFields.put(TableConfig.QUOTA_CONFIG_KEY, quotaConfig.toJsonString());
    }
    TableTaskConfig taskConfig = tableConfig.getTaskConfig();
    if (taskConfig != null) {
      simpleFields.put(TableConfig.TASK_CONFIG_KEY, taskConfig.toJsonString());
    }
    RoutingConfig routingConfig = tableConfig.getRoutingConfig();
    if (routingConfig != null) {
      simpleFields.put(TableConfig.ROUTING_CONFIG_KEY, routingConfig.toJsonString());
    }
    QueryConfig queryConfig = tableConfig.getQueryConfig();
    if (queryConfig != null) {
      simpleFields.put(TableConfig.QUERY_CONFIG_KEY, queryConfig.toJsonString());
    }
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = tableConfig.getInstanceAssignmentConfigMap();
    if (instanceAssignmentConfigMap != null) {
      simpleFields.put(TableConfig.INSTANCE_ASSIGNMENT_CONFIG_MAP_KEY,
          JsonUtils.objectToString(instanceAssignmentConfigMap));
    }
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList != null) {
      simpleFields.put(TableConfig.FIELD_CONFIG_LIST_KEY, JsonUtils.objectToString(fieldConfigList));
    }
    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    if (upsertConfig != null) {
      simpleFields.put(TableConfig.UPSERT_CONFIG_KEY, JsonUtils.objectToString(upsertConfig));
    }
    DedupConfig dedupConfig = tableConfig.getDedupConfig();
    if (dedupConfig != null) {
      simpleFields.put(TableConfig.DEDUP_CONFIG_KEY, JsonUtils.objectToString(dedupConfig));
    }
    DimensionTableConfig dimensionTableConfig = tableConfig.getDimensionTableConfig();
    if (dimensionTableConfig != null) {
      simpleFields.put(TableConfig.DIMENSION_TABLE_CONFIG_KEY, JsonUtils.objectToString(dimensionTableConfig));
    }
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    if (ingestionConfig != null) {
      simpleFields.put(TableConfig.INGESTION_CONFIG_KEY, JsonUtils.objectToString(ingestionConfig));
    }
    List<TierConfig> tierConfigList = tableConfig.getTierConfigsList();
    if (tierConfigList != null) {
      simpleFields.put(TableConfig.TIER_CONFIGS_LIST_KEY, JsonUtils.objectToString(tierConfigList));
    }
    List<TunerConfig> tunerConfigList = tableConfig.getTunerConfigsList();
    if (tunerConfigList != null) {
      simpleFields.put(TableConfig.TUNER_CONFIG_LIST_KEY, JsonUtils.objectToString(tunerConfigList));
    }
    if (tableConfig.getInstancePartitionsMap() != null) {
      simpleFields.put(TableConfig.INSTANCE_PARTITIONS_MAP_CONFIG_KEY,
          JsonUtils.objectToString(tableConfig.getInstancePartitionsMap()));
    }
    Map<String, SegmentAssignmentConfig> segmentAssignmentConfigMap = tableConfig.getSegmentAssignmentConfigMap();
    if (segmentAssignmentConfigMap != null) {
      simpleFields.put(TableConfig.SEGMENT_ASSIGNMENT_CONFIG_MAP_KEY,
          JsonUtils.objectToString(segmentAssignmentConfigMap));
    }
    List<TableSamplerConfig> tableSamplerConfigs = tableConfig.getTableSamplers();
    if (tableSamplerConfigs != null) {
      simpleFields.put(TableConfig.TABLE_SAMPLERS_KEY, JsonUtils.objectToString(tableSamplerConfigs));
    }
    String description = tableConfig.getDescription();
    if (StringUtils.isNotBlank(description)) {
      simpleFields.put(TableConfig.DESCRIPTION_KEY, description);
    }
    List<String> tags = tableConfig.getTags();
    if (tags != null && !tags.isEmpty()) {
      simpleFields.put(TableConfig.TAGS_KEY, JsonUtils.objectToString(tags));
    }

    ZNRecord znRecord = new ZNRecord(tableConfig.getTableName());
    znRecord.setSimpleFields(simpleFields);
    return znRecord;
  }

  /// Reconstructs the table-config JSON tree from a stored [ZNRecord], preserving every key originally written to
  /// ZK. Unlike `TableConfig.toJsonNode()`, this method does not round-trip through the Java bean and therefore
  /// does not strip fields that the bean's getters mark with `@JsonIgnore` or `@JsonInclude(NON_DEFAULT)`. It is
  /// intended for code paths (e.g. update-time deprecation diffing) that need to compare an incoming request
  /// against the exact bytes that were stored.
  ///
  /// JSON parsing is restricted to values whose first non-whitespace character is `{` or `[` (objects/arrays).
  /// Other values are kept as raw text nodes so that a stored simple field like a tableName `"123"` is not
  /// coerced by Jackson's lenient parser into a numeric node.
  ///
  /// @param znRecord the raw ZNRecord read from the property store
  /// @return a JsonNode equivalent to what the user originally PUT/POST-ed for the table, or `null` if the input is
  /// `null`
  @Nullable
  public static JsonNode toRawJsonNode(@Nullable ZNRecord znRecord) {
    if (znRecord == null) {
      return null;
    }
    ObjectNode root = JsonNodeFactory.instance.objectNode();
    Map<String, String> simpleFields = znRecord.getSimpleFields();
    for (Map.Entry<String, String> entry : simpleFields.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (value == null) {
        /// ZNRecord simpleFields is a regular HashMap that permits null values. Preserve the null in the JSON tree
        /// (NullNode) so a downstream diff sees the original ZK state byte-faithfully.
        root.putNull(key);
        continue;
      }
      if (looksLikeJsonContainer(value)) {
        try {
          root.set(key, JsonUtils.stringToJsonNode(value));
          continue;
        } catch (IOException e) {
          /// Malformed JSON container under a key that we expected to be parseable. Operators need to know — the
          /// downstream diff will treat the value as a text node instead of a structured object, which can
          /// produce spurious "path newly introduced" warnings on update. WARN-log so the corruption is visible
          /// (the table is still usable since fromZNRecord owns the deserialization path; this only affects the
          /// raw-JSON view used by the deprecation validator).
          LOGGER.warn("Stored simpleField {} on znode {} starts with '{}'/'[' but failed to parse as JSON: {}. "
                  + "Falling back to a text-node representation; downstream diffs may treat the path as newly "
                  + "introduced.", key, znRecord.getId(), e.getMessage());
        }
      }
      root.put(key, value);
    }
    if (!root.has(TableConfig.TABLE_NAME_KEY)) {
      root.put(TableConfig.TABLE_NAME_KEY, znRecord.getId());
    }
    return root;
  }

  /// Returns true if the value's first non-whitespace character is `{` or `[`, indicating a JSON object/array
  /// payload. Restricts JSON parsing in [#toRawJsonNode] so that simple textual fields whose contents happen to
  /// look like JSON primitives (`"true"`, `"null"`, `"123"`, etc.) are kept as text nodes rather than coerced.
  private static boolean looksLikeJsonContainer(String value) {
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (Character.isWhitespace(c)) {
        continue;
      }
      return c == '{' || c == '[';
    }
    return false;
  }
}
