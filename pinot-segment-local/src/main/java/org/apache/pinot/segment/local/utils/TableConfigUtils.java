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
package org.apache.pinot.segment.local.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.core.util.ReplicationUtils;
import org.apache.pinot.segment.local.function.FunctionEvaluator;
import org.apache.pinot.segment.local.function.FunctionEvaluatorFactory;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfig;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.quartz.CronScheduleBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utils related to table config operations
 * FIXME: Merge this TableConfigUtils with the TableConfigUtils from pinot-common when merging of modules is done
 */
public final class TableConfigUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableConfigUtils.class);
  private static final String SEGMENT_GENERATION_AND_PUSH_TASK_TYPE = "SegmentGenerationAndPushTask";
  private static final String SCHEDULE_KEY = "schedule";

  private TableConfigUtils() {
  }

  /**
   * Performs table config validations. Includes validations for the following:
   * 1. Validation config
   * 2. IngestionConfig
   * 3. TierConfigs
   * 4. Indexing config
   * 5. Field Config List
   *
   * TODO: Add more validations for each section (e.g. validate conditions are met for aggregateMetrics)
   */
  public static void validate(TableConfig tableConfig, @Nullable Schema schema) {
    if (tableConfig.getTableType() == TableType.REALTIME) {
      Preconditions.checkState(schema != null, "Schema should not be null for REALTIME table");
    }
    // Sanitize the table config before validation
    sanitize(tableConfig);
    validateValidationConfig(tableConfig, schema);
    validateIngestionConfig(tableConfig, schema);
    validateTierConfigList(tableConfig.getTierConfigsList());
    validateIndexingConfig(tableConfig.getIndexingConfig(), schema);
    validateFieldConfigList(tableConfig.getFieldConfigList(), tableConfig.getIndexingConfig(), schema);
    validateUpsertConfig(tableConfig, schema);
    validatePartialUpsertStrategies(tableConfig, schema);
    validateTaskConfigs(tableConfig);
  }

  /**
   * Validates the table name with the following rules:
   * <ul>
   *   <li>Table name shouldn't contain dot or space in it</li>
   * </ul>
   */
  public static void validateTableName(TableConfig tableConfig) {
    String tableName = tableConfig.getTableName();
    if (tableName.contains(".") || tableName.contains(" ")) {
      throw new IllegalStateException("Table name: '" + tableName + "' containing '.' or space is not allowed");
    }
  }

  /**
   * Validates retention config. Checks for following things:
   * - Valid segmentPushType
   * - Valid retentionTimeUnit
   */
  private static void validateRetentionConfig(TableConfig tableConfig) {
    SegmentsValidationAndRetentionConfig segmentsConfig = tableConfig.getValidationConfig();
    String tableName = tableConfig.getTableName();

    if (segmentsConfig == null) {
      throw new IllegalStateException(
          String.format("Table: %s, \"segmentsConfig\" field is missing in table config", tableName));
    }

    String segmentPushType = IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig);
    // segmentPushType is not needed for Realtime table
    if (tableConfig.getTableType() == TableType.OFFLINE && segmentPushType != null && !segmentPushType.isEmpty()) {
      if (!segmentPushType.equalsIgnoreCase("REFRESH") && !segmentPushType.equalsIgnoreCase("APPEND")) {
        throw new IllegalStateException(String.format("Table: %s, invalid push type: %s", tableName, segmentPushType));
      }
    }

    // Retention may not be specified. Ignore validation in that case.
    String timeUnitString = segmentsConfig.getRetentionTimeUnit();
    if (timeUnitString == null || timeUnitString.isEmpty()) {
      return;
    }
    try {
      TimeUnit.valueOf(timeUnitString.toUpperCase());
    } catch (Exception e) {
      throw new IllegalStateException(String.format("Table: %s, invalid time unit: %s", tableName, timeUnitString));
    }
  }

  /**
   * Validates the following in the validationConfig of the table
   * 1. For REALTIME table
   * - checks for non-null timeColumnName
   * - checks for valid field spec for timeColumnName in schema
   * - Validates retention config
   *
   * 2. For OFFLINE table
   * - checks for valid field spec for timeColumnName in schema, if timeColumnName and schema are non-null
   * - for Dimension tables checks the primary key requirement
   *
   * 3. Checks peerDownloadSchema
   */
  private static void validateValidationConfig(TableConfig tableConfig, @Nullable Schema schema) {
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    String timeColumnName = validationConfig.getTimeColumnName();
    if (tableConfig.getTableType() == TableType.REALTIME) {
      // For REALTIME table, must have a non-null timeColumnName
      Preconditions.checkState(timeColumnName != null, "'timeColumnName' cannot be null in REALTIME table config");
    }
    // timeColumnName can be null in OFFLINE table
    if (timeColumnName != null && !timeColumnName.isEmpty() && schema != null) {
      Preconditions.checkState(schema.getSpecForTimeColumn(timeColumnName) != null,
          "Cannot find valid fieldSpec for timeColumn: %s from the table config: %s, in the schema: %s", timeColumnName,
          tableConfig.getTableName(), schema.getSchemaName());
    }
    if (tableConfig.isDimTable()) {
      Preconditions.checkState(tableConfig.getTableType() == TableType.OFFLINE,
          "Dimension table must be of OFFLINE table type.");
      Preconditions.checkState(schema != null, "Dimension table must have an associated schema");
      Preconditions.checkState(schema.getPrimaryKeyColumns().size() > 0, "Dimension table must have primary key[s]");
    }

    String peerSegmentDownloadScheme = validationConfig.getPeerSegmentDownloadScheme();
    if (peerSegmentDownloadScheme != null) {
      if (!CommonConstants.HTTP_PROTOCOL.equalsIgnoreCase(peerSegmentDownloadScheme) && !CommonConstants.HTTPS_PROTOCOL
          .equalsIgnoreCase(peerSegmentDownloadScheme)) {
        throw new IllegalStateException("Invalid value '" + peerSegmentDownloadScheme
            + "' for peerSegmentDownloadScheme. Must be one of http or https");
      }
    }

    validateRetentionConfig(tableConfig);
  }

  /**
   * Validates the following:
   * 1. validity of filter function
   * 2. checks for duplicate transform configs
   * 3. checks for null column name or transform function in transform config
   * 4. validity of transform function string
   * 5. checks for source fields used in destination columns
   * 6. ingestion type for dimension tables
   */
  @VisibleForTesting
  public static void validateIngestionConfig(TableConfig tableConfig, @Nullable Schema schema) {
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();

    if (ingestionConfig != null) {
      String tableNameWithType = tableConfig.getTableName();

      // Batch
      if (ingestionConfig.getBatchIngestionConfig() != null) {
        BatchIngestionConfig cfg = ingestionConfig.getBatchIngestionConfig();
        List<Map<String, String>> batchConfigMaps = cfg.getBatchConfigMaps();
        try {
          if (CollectionUtils.isNotEmpty(batchConfigMaps)) {
            // Validate that BatchConfig can be created
            batchConfigMaps.forEach(b -> new BatchConfig(tableNameWithType, b));
          }
        } catch (Exception e) {
          throw new IllegalStateException("Could not create BatchConfig using the batchConfig map", e);
        }
        if (tableConfig.isDimTable()) {
          Preconditions.checkState(cfg.getSegmentIngestionType().equalsIgnoreCase("REFRESH"),
              "Dimension tables must have segment ingestion type REFRESH");
        }
      }
      if (tableConfig.isDimTable()) {
        Preconditions.checkState(ingestionConfig.getBatchIngestionConfig() != null,
            "Dimension tables must have batch ingestion configuration");
      }

      // Stream
      if (ingestionConfig.getStreamIngestionConfig() != null) {
        IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
        Preconditions.checkState(indexingConfig == null || MapUtils.isEmpty(indexingConfig.getStreamConfigs()),
            "Should not use indexingConfig#getStreamConfigs if ingestionConfig#StreamIngestionConfig is provided");
        List<Map<String, String>> streamConfigMaps = ingestionConfig.getStreamIngestionConfig().getStreamConfigMaps();
        Preconditions.checkState(streamConfigMaps.size() == 1, "Only 1 stream is supported in REALTIME table");
        try {
          // Validate that StreamConfig can be created
          new StreamConfig(tableNameWithType, streamConfigMaps.get(0));
        } catch (Exception e) {
          throw new IllegalStateException("Could not create StreamConfig using the streamConfig map", e);
        }
      }

      // Filter config
      FilterConfig filterConfig = ingestionConfig.getFilterConfig();
      if (filterConfig != null) {
        String filterFunction = filterConfig.getFilterFunction();
        if (filterFunction != null) {
          try {
            FunctionEvaluatorFactory.getExpressionEvaluator(filterFunction);
          } catch (Exception e) {
            throw new IllegalStateException("Invalid filter function " + filterFunction, e);
          }
        }
      }

      // Transform configs
      List<TransformConfig> transformConfigs = ingestionConfig.getTransformConfigs();
      if (transformConfigs != null) {
        Set<String> transformColumns = new HashSet<>();
        for (TransformConfig transformConfig : transformConfigs) {
          String columnName = transformConfig.getColumnName();
          if (schema != null) {
            Preconditions.checkState(schema.getFieldSpecFor(columnName) != null,
                "The destination column '" + columnName + "' of the transform function must be present in the schema");
          }
          String transformFunction = transformConfig.getTransformFunction();
          if (columnName == null || transformFunction == null) {
            throw new IllegalStateException(
                "columnName/transformFunction cannot be null in TransformConfig " + transformConfig);
          }
          if (!transformColumns.add(columnName)) {
            throw new IllegalStateException("Duplicate transform config found for column '" + columnName + "'");
          }
          FunctionEvaluator expressionEvaluator;
          try {
            expressionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(transformFunction);
          } catch (Exception e) {
            throw new IllegalStateException(
                "Invalid transform function '" + transformFunction + "' for column '" + columnName + "'");
          }
          List<String> arguments = expressionEvaluator.getArguments();
          if (arguments.contains(columnName)) {
            throw new IllegalStateException(
                "Arguments of a transform function '" + arguments + "' cannot contain the destination column '"
                    + columnName + "'");
          }
        }
      }
    }
  }

  private static void validateTaskConfigs(TableConfig tableConfig) {
    TableTaskConfig taskConfig = tableConfig.getTaskConfig();
    if (taskConfig != null && taskConfig.isTaskTypeEnabled(SEGMENT_GENERATION_AND_PUSH_TASK_TYPE)) {
      Map<String, String> taskTypeConfig = taskConfig.getConfigsForTaskType(SEGMENT_GENERATION_AND_PUSH_TASK_TYPE);
      if (taskTypeConfig != null && taskTypeConfig.containsKey(SCHEDULE_KEY)) {
        String cronExprStr = taskTypeConfig.get(SCHEDULE_KEY);
        try {
          CronScheduleBuilder.cronSchedule(cronExprStr);
        } catch (Exception e) {
          throw new IllegalStateException(
              String.format("SegmentGenerationAndPushTask contains an invalid cron schedule: %s", cronExprStr), e);
        }
      }
    }
  }

  /**
   * Validates the upsert-related configurations
   *  - check table type is realtime
   *  - the primary key exists on the schema
   *  - strict replica-group is configured for routing type
   *  - consumer type must be low-level
   */
  @VisibleForTesting
  static void validateUpsertConfig(TableConfig tableConfig, Schema schema) {
    if (tableConfig.getUpsertMode() == UpsertConfig.Mode.NONE) {
      return;
    }
    // check table type is realtime
    Preconditions
        .checkState(tableConfig.getTableType() == TableType.REALTIME, "Upsert table is for realtime table only.");
    // primary key exists
    Preconditions.checkState(CollectionUtils.isNotEmpty(schema.getPrimaryKeyColumns()),
        "Upsert table must have primary key columns in the schema");
    // consumer type must be low-level
    Map<String, String> streamConfigsMap = IngestionConfigUtils.getStreamConfigMap(tableConfig);
    StreamConfig streamConfig = new StreamConfig(tableConfig.getTableName(), streamConfigsMap);
    Preconditions.checkState(streamConfig.hasLowLevelConsumerType() && !streamConfig.hasHighLevelConsumerType(),
        "Upsert table must use low-level streaming consumer type");
    // replica group is configured for routing
    Preconditions.checkState(
        tableConfig.getRoutingConfig() != null && RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE
            .equalsIgnoreCase(tableConfig.getRoutingConfig().getInstanceSelectorType()),
        "Upsert table must use strict replica-group (i.e. strictReplicaGroup) based routing");
    // no startree index
    Preconditions.checkState(
        CollectionUtils.isEmpty(tableConfig.getIndexingConfig().getStarTreeIndexConfigs()) && !tableConfig
            .getIndexingConfig().isEnableDefaultStarTree(), "The upsert table cannot have star-tree index.");
  }

  /**
   * Validates the partial upsert-related configurations:
   *  - Null handling must be enabled
   *  - Merger cannot be applied to private key columns
   *  - Merger cannot be applied to non-existing columns
   *  - INCREMENT merger must be applied to numeric columns
   */
  @VisibleForTesting
  static void validatePartialUpsertStrategies(TableConfig tableConfig, Schema schema) {
    if (tableConfig.getUpsertMode() != UpsertConfig.Mode.PARTIAL) {
      return;
    }

    Preconditions.checkState(tableConfig.getIndexingConfig().isNullHandlingEnabled(),
        "Null handling must be enabled for partial upsert tables");

    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    assert upsertConfig != null;
    Map<String, UpsertConfig.Strategy> partialUpsertStrategies = upsertConfig.getPartialUpsertStrategies();

    List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
    for (Map.Entry<String, UpsertConfig.Strategy> entry : partialUpsertStrategies.entrySet()) {
      String column = entry.getKey();
      Preconditions.checkState(!primaryKeyColumns.contains(column), "Merger cannot be applied to primary key columns");

      FieldSpec fieldSpec = schema.getFieldSpecFor(column);
      Preconditions.checkState(fieldSpec != null, "Merger cannot be applied to non-existing column: %s", column);

      if (entry.getValue() == UpsertConfig.Strategy.INCREMENT) {
        Preconditions.checkState(fieldSpec.getDataType().getStoredType().isNumeric(),
            "INCREMENT merger cannot be applied to non-numeric column: %s", column);
        Preconditions.checkState(!schema.getDateTimeNames().contains(column),
            "INCREMENT merger cannot be applied to date time column: %s", column);
      }
    }
  }

  /**
   * Validates the tier configs
   * Checks for the right segmentSelectorType and its required properties
   * Checks for the right storageType and its required properties
   */
  private static void validateTierConfigList(@Nullable List<TierConfig> tierConfigList) {
    if (tierConfigList == null) {
      return;
    }

    Set<String> tierNames = new HashSet<>();
    for (TierConfig tierConfig : tierConfigList) {
      String tierName = tierConfig.getName();
      Preconditions.checkState(!tierName.isEmpty(), "Tier name cannot be blank");
      Preconditions.checkState(tierNames.add(tierName), "Tier name: %s already exists in tier configs", tierName);

      String segmentSelectorType = tierConfig.getSegmentSelectorType();
      String segmentAge = tierConfig.getSegmentAge();
      if (segmentSelectorType.equalsIgnoreCase(TierFactory.TIME_SEGMENT_SELECTOR_TYPE)) {
        Preconditions
            .checkState(segmentAge != null, "Must provide 'segmentAge' for segmentSelectorType: %s in tier: %s",
                segmentSelectorType, tierName);
        Preconditions.checkState(TimeUtils.isPeriodValid(segmentAge),
            "segmentAge: %s must be a valid period string (eg. 30d, 24h) in tier: %s", segmentAge, tierName);
      } else {
        throw new IllegalStateException(
            "Unsupported segmentSelectorType: " + segmentSelectorType + " in tier: " + tierName);
      }

      String storageType = tierConfig.getStorageType();
      String serverTag = tierConfig.getServerTag();
      if (storageType.equalsIgnoreCase(TierFactory.PINOT_SERVER_STORAGE_TYPE)) {
        Preconditions
            .checkState(serverTag != null, "Must provide 'serverTag' for storageType: %s in tier: %s", storageType,
                tierName);
        Preconditions.checkState(TagNameUtils.isServerTag(serverTag),
            "serverTag: %s must have a valid server tag format (<tenantName>_OFFLINE or <tenantName>_REALTIME) in tier: %s",
            serverTag, tierName);
      } else {
        throw new IllegalStateException("Unsupported storageType: " + storageType + " in tier: " + tierName);
      }
    }
  }

  /**
   * Validates the Indexing Config
   * Ensures that every referred column name exists in the corresponding schema.
   * Also ensures proper dependency between index types (eg: Inverted Index columns
   * cannot be present in no-dictionary columns).
   */
  private static void validateIndexingConfig(@Nullable IndexingConfig indexingConfig, @Nullable Schema schema) {
    if (indexingConfig == null || schema == null) {
      return;
    }
    ArrayListMultimap<String, String> columnNameToConfigMap = ArrayListMultimap.create();
    Set<String> noDictionaryColumnsSet = new HashSet<>();
    String STAR_TREE_CONFIG_NAME = "StarTreeIndex Config";

    if (indexingConfig.getNoDictionaryColumns() != null) {
      for (String columnName : indexingConfig.getNoDictionaryColumns()) {
        columnNameToConfigMap.put(columnName, "No Dictionary Column Config");
        noDictionaryColumnsSet.add(columnName);
      }
    }
    Set<String> bloomFilterColumns = new HashSet<>();
    if (indexingConfig.getBloomFilterColumns() != null) {
      bloomFilterColumns.addAll(indexingConfig.getBloomFilterColumns());
    }
    if (indexingConfig.getBloomFilterConfigs() != null) {
      bloomFilterColumns.addAll(indexingConfig.getBloomFilterConfigs().keySet());
    }
    for (String bloomFilterColumn : bloomFilterColumns) {
      if (noDictionaryColumnsSet.contains(bloomFilterColumn)) {
        throw new IllegalStateException("Cannot create a Bloom Filter on column " + bloomFilterColumn
            + " specified in the noDictionaryColumns config");
      }
      columnNameToConfigMap.put(bloomFilterColumn, "Bloom Filter Config");
    }
    if (indexingConfig.getInvertedIndexColumns() != null) {
      for (String columnName : indexingConfig.getInvertedIndexColumns()) {
        if (noDictionaryColumnsSet.contains(columnName)) {
          throw new IllegalStateException("Cannot create an Inverted index on column " + columnName
              + " specified in the noDictionaryColumns config");
        }
        columnNameToConfigMap.put(columnName, "Inverted Index Config");
      }
    }

    if (indexingConfig.getOnHeapDictionaryColumns() != null) {
      for (String columnName : indexingConfig.getOnHeapDictionaryColumns()) {
        columnNameToConfigMap.put(columnName, "On Heap Dictionary Column Config");
      }
    }
    if (indexingConfig.getRangeIndexColumns() != null) {
      for (String columnName : indexingConfig.getRangeIndexColumns()) {
        columnNameToConfigMap.put(columnName, "Range Column Config");
      }
    }
    if (indexingConfig.getSortedColumn() != null) {
      for (String columnName : indexingConfig.getSortedColumn()) {
        columnNameToConfigMap.put(columnName, "Sorted Column Config");
      }
    }
    if (indexingConfig.getVarLengthDictionaryColumns() != null) {
      for (String columnName : indexingConfig.getVarLengthDictionaryColumns()) {
        columnNameToConfigMap.put(columnName, "Var Length Column Config");
      }
    }
    if (indexingConfig.getSegmentPartitionConfig() != null
        && indexingConfig.getSegmentPartitionConfig().getColumnPartitionMap() != null) {
      for (String columnName : indexingConfig.getSegmentPartitionConfig().getColumnPartitionMap().keySet()) {
        columnNameToConfigMap.put(columnName, "Segment Partition Config");
      }
    }
    if (indexingConfig.getJsonIndexColumns() != null) {
      for (String columnName : indexingConfig.getJsonIndexColumns()) {
        columnNameToConfigMap.put(columnName, "Json Index Config");
      }
    }

    List<StarTreeIndexConfig> starTreeIndexConfigList = indexingConfig.getStarTreeIndexConfigs();
    if (starTreeIndexConfigList != null) {
      for (StarTreeIndexConfig starTreeIndexConfig : starTreeIndexConfigList) {
        // Dimension split order cannot be null
        for (String columnName : starTreeIndexConfig.getDimensionsSplitOrder()) {
          columnNameToConfigMap.put(columnName, STAR_TREE_CONFIG_NAME);
        }
        // Function column pairs cannot be null
        for (String functionColumnPair : starTreeIndexConfig.getFunctionColumnPairs()) {
          AggregationFunctionColumnPair columnPair;
          try {
            columnPair = AggregationFunctionColumnPair.fromColumnName(functionColumnPair);
          } catch (Exception e) {
            throw new IllegalStateException("Invalid StarTreeIndex config: " + functionColumnPair + ". Must be"
                + "in the form <Aggregation function>__<Column name>");
          }
          String columnName = columnPair.getColumn();
          if (!columnName.equals(AggregationFunctionColumnPair.STAR)) {
            columnNameToConfigMap.put(columnName, STAR_TREE_CONFIG_NAME);
          }
        }
        List<String> skipDimensionList = starTreeIndexConfig.getSkipStarNodeCreationForDimensions();
        if (skipDimensionList != null) {
          for (String columnName : skipDimensionList) {
            columnNameToConfigMap.put(columnName, STAR_TREE_CONFIG_NAME);
          }
        }
      }
    }

    for (Map.Entry<String, String> entry : columnNameToConfigMap.entries()) {
      String columnName = entry.getKey();
      String configName = entry.getValue();
      FieldSpec columnFieldSpec = schema.getFieldSpecFor(columnName);
      Preconditions.checkState(columnFieldSpec != null,
          "Column Name " + columnName + " defined in " + configName + " must be a valid column defined in the schema");
      if (configName.equals(STAR_TREE_CONFIG_NAME)) {
        Preconditions.checkState(columnFieldSpec.isSingleValueField(),
            "Column Name " + columnName + " defined in " + configName + " must be a single value column");
      }
    }

    // Range index semantic validation
    // Range index can be defined on numeric columns and any column with a dictionary
    if (indexingConfig.getRangeIndexColumns() != null) {
      for (String rangeIndexCol : indexingConfig.getRangeIndexColumns()) {
        Preconditions.checkState(
            schema.getFieldSpecFor(rangeIndexCol).getDataType().isNumeric() || !noDictionaryColumnsSet
                .contains(rangeIndexCol),
            "Cannot create a range index on non-numeric/no-dictionary column " + rangeIndexCol);
      }
    }

    // Var length dictionary semantic validation
    if (indexingConfig.getVarLengthDictionaryColumns() != null) {
      for (String varLenDictCol : indexingConfig.getVarLengthDictionaryColumns()) {
        FieldSpec varLenDictFieldSpec = schema.getFieldSpecFor(varLenDictCol);
        switch (varLenDictFieldSpec.getDataType().getStoredType()) {
          case STRING:
          case BYTES:
            continue;
          default:
            throw new IllegalStateException(
                "var length dictionary can only be created for columns of type STRING and BYTES. Invalid for column "
                    + varLenDictCol);
        }
      }
    }

    if (indexingConfig.getJsonIndexColumns() != null) {
      for (String jsonIndexCol : indexingConfig.getJsonIndexColumns()) {
        FieldSpec fieldSpec = schema.getFieldSpecFor(jsonIndexCol);
        Preconditions
            .checkState(fieldSpec.isSingleValueField() && fieldSpec.getDataType().getStoredType() == DataType.STRING,
                "Json index can only be created for single value String column. Invalid for column: %s", jsonIndexCol);
      }
    }
  }

  /**
   * Validates the Field Config List in the given TableConfig
   * Ensures that every referred column name exists in the corresponding schema
   * Additional checks for TEXT and FST index types
   */
  private static void validateFieldConfigList(@Nullable List<FieldConfig> fieldConfigList,
      @Nullable IndexingConfig indexingConfigs, @Nullable Schema schema) {
    if (fieldConfigList == null || schema == null) {
      return;
    }

    for (FieldConfig fieldConfig : fieldConfigList) {
      String columnName = fieldConfig.getName();
      FieldSpec fieldConfigColSpec = schema.getFieldSpecFor(columnName);
      Preconditions.checkState(fieldConfigColSpec != null,
          "Column Name " + columnName + " defined in field config list must be a valid column defined in the schema");

      List<String> noDictionaryColumns = indexingConfigs.getNoDictionaryColumns();
      switch (fieldConfig.getEncodingType()) {
        case DICTIONARY:
          if (noDictionaryColumns != null) {
            Preconditions.checkArgument(!noDictionaryColumns.contains(columnName),
                "FieldConfig encoding type is different from indexingConfig for column: " + columnName);
          }
          Preconditions.checkArgument(fieldConfig.getCompressionCodec() == null,
              "Set compression codec to null for dictionary encoding type");
          break;
      }

      switch (fieldConfig.getIndexType()) {
        case FST:
          Preconditions.checkArgument(fieldConfig.getEncodingType() == FieldConfig.EncodingType.DICTIONARY,
              "FST Index is only enabled on dictionary encoded columns");
          Preconditions.checkState(
              fieldConfigColSpec.isSingleValueField() && fieldConfigColSpec.getDataType() == DataType.STRING,
              "FST Index is only supported for single value string columns");
          break;
        case TEXT:
          Preconditions.checkState(
              fieldConfigColSpec.isSingleValueField() && fieldConfigColSpec.getDataType() == DataType.STRING,
              "TEXT Index is only supported for single value string columns");
      }
    }
  }

  private static void sanitize(TableConfig tableConfig) {
    tableConfig.setIndexingConfig(sanitizeIndexingConfig(tableConfig.getIndexingConfig()));
  }

  private static IndexingConfig sanitizeIndexingConfig(IndexingConfig indexingConfig) {
    indexingConfig.setInvertedIndexColumns(sanitizeListBasedIndexingColumns(indexingConfig.getInvertedIndexColumns()));
    indexingConfig.setNoDictionaryColumns(sanitizeListBasedIndexingColumns(indexingConfig.getNoDictionaryColumns()));
    indexingConfig.setSortedColumn(sanitizeListBasedIndexingColumns(indexingConfig.getSortedColumn()));
    indexingConfig.setBloomFilterColumns(sanitizeListBasedIndexingColumns(indexingConfig.getBloomFilterColumns()));
    indexingConfig
        .setOnHeapDictionaryColumns(sanitizeListBasedIndexingColumns(indexingConfig.getOnHeapDictionaryColumns()));
    indexingConfig.setRangeIndexColumns(sanitizeListBasedIndexingColumns(indexingConfig.getRangeIndexColumns()));
    indexingConfig.setVarLengthDictionaryColumns(
        sanitizeListBasedIndexingColumns(indexingConfig.getVarLengthDictionaryColumns()));
    return indexingConfig;
  }

  private static List<String> sanitizeListBasedIndexingColumns(List<String> indexingColumns) {
    if (indexingColumns != null) {
      // Disregard the empty string value for indexing columns
      return indexingColumns.stream().filter(v -> !v.isEmpty()).collect(Collectors.toList());
    }
    return null;
  }

  /**
   * Ensure that the table config has the minimum number of replicas set as per cluster configs.
   * If is doesn't, set the required amount of replication in the table config
   */
  public static void ensureMinReplicas(TableConfig tableConfig, int defaultTableMinReplicas) {
    // For self-serviced cluster, ensure that the tables are created with at least min replication factor irrespective
    // of table configuration value
    SegmentsValidationAndRetentionConfig segmentsConfig = tableConfig.getValidationConfig();
    boolean verifyReplicasPerPartition;
    boolean verifyReplication;

    try {
      verifyReplicasPerPartition = ReplicationUtils.useReplicasPerPartition(tableConfig);
      verifyReplication = ReplicationUtils.useReplication(tableConfig);
    } catch (Exception e) {
      throw new IllegalStateException(String.format("Invalid tableIndexConfig or streamConfig: %s", e.getMessage()), e);
    }

    if (verifyReplication) {
      int requestReplication;
      try {
        requestReplication = segmentsConfig.getReplicationNumber();
        if (requestReplication < defaultTableMinReplicas) {
          LOGGER.info("Creating table with minimum replication factor of: {} instead of requested replication: {}",
              defaultTableMinReplicas, requestReplication);
          segmentsConfig.setReplication(String.valueOf(defaultTableMinReplicas));
        }
      } catch (NumberFormatException e) {
        throw new IllegalStateException("Invalid replication number", e);
      }
    }

    if (verifyReplicasPerPartition) {
      String replicasPerPartitionStr = segmentsConfig.getReplicasPerPartition();
      if (replicasPerPartitionStr == null) {
        throw new IllegalStateException("Field replicasPerPartition needs to be specified");
      }
      try {
        int replicasPerPartition = Integer.parseInt(replicasPerPartitionStr);
        if (replicasPerPartition < defaultTableMinReplicas) {
          LOGGER.info(
              "Creating table with minimum replicasPerPartition of: {} instead of requested replicasPerPartition: {}",
              defaultTableMinReplicas, replicasPerPartition);
          segmentsConfig.setReplicasPerPartition(String.valueOf(defaultTableMinReplicas));
        }
      } catch (NumberFormatException e) {
        throw new IllegalStateException("Invalid value for replicasPerPartition: '" + replicasPerPartitionStr + "'", e);
      }
    }
  }

  /**
   * Ensure the table config has storage quota set as per cluster configs.
   * If it doesn't, set the quota config into the table config
   */
  public static void ensureStorageQuotaConstraints(TableConfig tableConfig, String maxAllowedSize) {
    // Dim tables must adhere to cluster level storage size limits
    if (tableConfig.isDimTable()) {
      QuotaConfig quotaConfig = tableConfig.getQuotaConfig();
      long maxAllowedSizeInBytes = DataSizeUtils.toBytes(maxAllowedSize);

      if (quotaConfig == null) {
        // set a default storage quota
        tableConfig.setQuotaConfig(new QuotaConfig(maxAllowedSize, null));
        LOGGER.info("Assigning default storage quota ({}) for dimension table: {}", maxAllowedSize,
            tableConfig.getTableName());
      } else {
        if (quotaConfig.getStorage() == null) {
          // set a default storage quota and keep the RPS value
          tableConfig.setQuotaConfig(new QuotaConfig(maxAllowedSize, quotaConfig.getMaxQueriesPerSecond()));
          LOGGER.info("Assigning default storage quota ({}) for dimension table: {}", maxAllowedSize,
              tableConfig.getTableName());
        } else {
          if (quotaConfig.getStorageInBytes() > maxAllowedSizeInBytes) {
            throw new IllegalStateException(String
                .format("Invalid storage quota: %d, max allowed size: %d", quotaConfig.getStorageInBytes(),
                    maxAllowedSizeInBytes));
          }
        }
      }
    }
  }

  /**
   * Consistency checks across the offline and realtime counterparts of a hybrid table
   */
  public static void verifyHybridTableConfigs(String rawTableName, TableConfig offlineTableConfig,
      TableConfig realtimeTableConfig) {
    Preconditions
        .checkNotNull(offlineTableConfig, "Found null offline table config in hybrid table check for table: %s",
            rawTableName);
    Preconditions
        .checkNotNull(realtimeTableConfig, "Found null realtime table config in hybrid table check for table: %s",
            rawTableName);
    LOGGER.info("Validating realtime and offline configs for the hybrid table: {}", rawTableName);
    SegmentsValidationAndRetentionConfig offlineSegmentConfig = offlineTableConfig.getValidationConfig();
    SegmentsValidationAndRetentionConfig realtimeSegmentConfig = realtimeTableConfig.getValidationConfig();
    String offlineTimeColumnName = offlineSegmentConfig.getTimeColumnName();
    String realtimeTimeColumnName = realtimeSegmentConfig.getTimeColumnName();
    if (offlineTimeColumnName == null || realtimeTimeColumnName == null) {
      throw new IllegalStateException(String.format(
          "'timeColumnName' cannot be null for table: %s! Offline time column name: %s. Realtime time column name: %s",
          rawTableName, offlineTimeColumnName, realtimeTimeColumnName));
    }
    if (!offlineTimeColumnName.equals(realtimeTimeColumnName)) {
      throw new IllegalStateException(String.format(
          "Time column names are different for table: %s! Offline time column name: %s. Realtime time column name: %s",
          rawTableName, offlineTimeColumnName, realtimeTimeColumnName));
    }
  }
}
