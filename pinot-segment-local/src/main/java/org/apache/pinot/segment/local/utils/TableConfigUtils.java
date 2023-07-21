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
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.segment.local.function.FunctionEvaluator;
import org.apache.pinot.segment.local.function.FunctionEvaluatorFactory;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitSlicedRangeIndexCreator;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
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
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfig;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
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
  private TableConfigUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TableConfigUtils.class);
  private static final String SCHEDULE_KEY = "schedule";
  private static final String STAR_TREE_CONFIG_NAME = "StarTreeIndex Config";

  // supported TableTaskTypes, must be identical to the one return in the impl of {@link PinotTaskGenerator}.
  private static final String REALTIME_TO_OFFLINE_TASK_TYPE = "RealtimeToOfflineSegmentsTask";

  // this is duplicate with KinesisConfig.STREAM_TYPE, while instead of use KinesisConfig.STREAM_TYPE directly, we
  // hardcode the value here to avoid pulling the entire pinot-kinesis module as dependency.
  private static final String KINESIS_STREAM_TYPE = "kinesis";
  private static final EnumSet<AggregationFunctionType> SUPPORTED_INGESTION_AGGREGATIONS =
      EnumSet.of(AggregationFunctionType.SUM, AggregationFunctionType.MIN, AggregationFunctionType.MAX,
          AggregationFunctionType.COUNT);
  private static final Set<String> UPSERT_DEDUP_ALLOWED_ROUTING_STRATEGIES =
      ImmutableSet.of(RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE,
          RoutingConfig.MULTI_STAGE_REPLICA_GROUP_SELECTOR_TYPE);

  /**
   * @see TableConfigUtils#validate(TableConfig, Schema, String, boolean)
   */
  public static void validate(TableConfig tableConfig, @Nullable Schema schema) {
    validate(tableConfig, schema, null, false);
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
  public static void validate(TableConfig tableConfig, @Nullable Schema schema, @Nullable String typesToSkip,
      boolean disableGroovy) {
    Set<ValidationType> skipTypes = parseTypesToSkipString(typesToSkip);
    if (tableConfig.getTableType() == TableType.REALTIME) {
      Preconditions.checkState(schema != null, "Schema should not be null for REALTIME table");
    }
    // Sanitize the table config before validation
    sanitize(tableConfig);
    // skip all validation if skip type ALL is selected.
    if (!skipTypes.contains(ValidationType.ALL)) {
      validateValidationConfig(tableConfig, schema);
      validateIngestionConfig(tableConfig, schema, disableGroovy);
      validateTierConfigList(tableConfig.getTierConfigsList());
      validateIndexingConfig(tableConfig.getIndexingConfig(), schema);
      validateFieldConfigList(tableConfig.getFieldConfigList(), tableConfig.getIndexingConfig(), schema);
      validateInstancePartitionsTypeMapConfig(tableConfig);
      validatePartitionedReplicaGroupInstance(tableConfig);
      if (!skipTypes.contains(ValidationType.UPSERT)) {
        validateUpsertAndDedupConfig(tableConfig, schema);
        validatePartialUpsertStrategies(tableConfig, schema);
      }
      if (!skipTypes.contains(ValidationType.TASK)) {
        validateTaskConfigs(tableConfig, schema);
      }
    }
  }

  private static Set<ValidationType> parseTypesToSkipString(@Nullable String typesToSkip) {
    return typesToSkip == null ? Collections.emptySet()
        : Arrays.stream(typesToSkip.split(",")).map(s -> ValidationType.valueOf(s.toUpperCase()))
            .collect(Collectors.toSet());
  }

  /**
   * Validates the table name with the following rules:
   * <ul>
   *   <li>If there is a flag allowing database name in it, table name can have one dot in it.
   *   <li>Otherwise, there is no dot allowed in table name.</li>
   * </ul>
   */
  public static void validateTableName(TableConfig tableConfig, boolean allowTableNameWithDatabase) {
    String tableName = tableConfig.getTableName();
    int dotCount = StringUtils.countMatches(tableName, '.');
    // For transitioning into full [database_name].[table_name] support, we allow the table name
    // with one dot at max, so the admin may create mydb.mytable with a feature knob.
    if (allowTableNameWithDatabase && dotCount > 1) {
      throw new IllegalStateException("Table name: '" + tableName + "' containing more than one '.' is not allowed");
    }
    if (!allowTableNameWithDatabase && dotCount > 0) {
      throw new IllegalStateException("Table name: '" + tableName + "' containing '.' is not allowed");
    }
    if (StringUtils.containsWhitespace(tableName)) {
      throw new IllegalStateException("Table name: '" + tableName + "' containing space is not allowed");
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
   * 4. Checks time column existence if null handling for time column is enabled
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
      Preconditions.checkState(!schema.getPrimaryKeyColumns().isEmpty(), "Dimension table must have primary key[s]");
    }

    String peerSegmentDownloadScheme = validationConfig.getPeerSegmentDownloadScheme();
    if (peerSegmentDownloadScheme != null) {
      if (!CommonConstants.HTTP_PROTOCOL.equalsIgnoreCase(peerSegmentDownloadScheme)
          && !CommonConstants.HTTPS_PROTOCOL.equalsIgnoreCase(peerSegmentDownloadScheme)) {
        throw new IllegalStateException("Invalid value '" + peerSegmentDownloadScheme
            + "' for peerSegmentDownloadScheme. Must be one of http or https");
      }
    }

    validateRetentionConfig(tableConfig);
  }

  @VisibleForTesting
  public static void validateIngestionConfig(TableConfig tableConfig, @Nullable Schema schema) {
    validateIngestionConfig(tableConfig, schema, false);
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
  public static void validateIngestionConfig(TableConfig tableConfig, @Nullable Schema schema, boolean disableGroovy) {
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

        StreamConfig streamConfig;
        try {
          // Validate that StreamConfig can be created
          streamConfig = new StreamConfig(tableNameWithType, streamConfigMaps.get(0));
        } catch (Exception e) {
          throw new IllegalStateException("Could not create StreamConfig using the streamConfig map", e);
        }
        validateDecoder(streamConfig);
      }

      // Filter config
      FilterConfig filterConfig = ingestionConfig.getFilterConfig();
      if (filterConfig != null) {
        String filterFunction = filterConfig.getFilterFunction();
        if (filterFunction != null) {
          if (disableGroovy && FunctionEvaluatorFactory.isGroovyExpression(filterFunction)) {
            throw new IllegalStateException(
                "Groovy filter functions are disabled for table config. Found '" + filterFunction + "'");
          }
          try {
            FunctionEvaluatorFactory.getExpressionEvaluator(filterFunction);
          } catch (Exception e) {
            throw new IllegalStateException("Invalid filter function " + filterFunction, e);
          }
        }
      }

      // Aggregation configs
      List<AggregationConfig> aggregationConfigs = ingestionConfig.getAggregationConfigs();
      Set<String> aggregationSourceColumns = new HashSet<>();
      if (!CollectionUtils.isEmpty(aggregationConfigs)) {
        Preconditions.checkState(!tableConfig.getIndexingConfig().isAggregateMetrics(),
            "aggregateMetrics cannot be set with AggregationConfig");
        Set<String> aggregationColumns = new HashSet<>();
        for (AggregationConfig aggregationConfig : aggregationConfigs) {
          String columnName = aggregationConfig.getColumnName();
          String aggregationFunction = aggregationConfig.getAggregationFunction();
          if (columnName == null || aggregationFunction == null) {
            throw new IllegalStateException(
                "columnName/aggregationFunction cannot be null in AggregationConfig " + aggregationConfig);
          }

          if (schema != null) {
            FieldSpec fieldSpec = schema.getFieldSpecFor(columnName);
            Preconditions.checkState(fieldSpec != null, "The destination column '" + columnName
                + "' of the aggregation function must be present in the schema");
            Preconditions.checkState(fieldSpec.getFieldType() == FieldSpec.FieldType.METRIC,
                "The destination column '" + columnName + "' of the aggregation function must be a metric column");
          }

          if (!aggregationColumns.add(columnName)) {
            throw new IllegalStateException("Duplicate aggregation config found for column '" + columnName + "'");
          }
          ExpressionContext expressionContext;
          try {
            expressionContext = RequestContextUtils.getExpression(aggregationConfig.getAggregationFunction());
          } catch (Exception e) {
            throw new IllegalStateException(
                "Invalid aggregation function '" + aggregationFunction + "' for column '" + columnName + "'", e);
          }
          Preconditions.checkState(expressionContext.getType() == ExpressionContext.Type.FUNCTION,
              "aggregation function must be a function for: %s", aggregationConfig);

          FunctionContext functionContext = expressionContext.getFunction();
          validateIngestionAggregation(functionContext.getFunctionName());
          Preconditions.checkState(functionContext.getArguments().size() == 1,
              "aggregation function can only have one argument: %s", aggregationConfig);

          ExpressionContext argument = functionContext.getArguments().get(0);
          Preconditions.checkState(argument.getType() == ExpressionContext.Type.IDENTIFIER,
              "aggregator function argument must be a identifier: %s", aggregationConfig);

          aggregationSourceColumns.add(argument.getIdentifier());
        }
        if (schema != null) {
          Preconditions.checkState(new HashSet<>(schema.getMetricNames()).equals(aggregationColumns),
              "all metric columns must be aggregated");
        }
      }

      // Transform configs
      List<TransformConfig> transformConfigs = ingestionConfig.getTransformConfigs();
      if (transformConfigs != null) {
        Set<String> transformColumns = new HashSet<>();
        for (TransformConfig transformConfig : transformConfigs) {
          String columnName = transformConfig.getColumnName();
          String transformFunction = transformConfig.getTransformFunction();
          if (columnName == null || transformFunction == null) {
            throw new IllegalStateException(
                "columnName/transformFunction cannot be null in TransformConfig " + transformConfig);
          }
          if (!transformColumns.add(columnName)) {
            throw new IllegalStateException("Duplicate transform config found for column '" + columnName + "'");
          }
          if (schema != null) {
            Preconditions.checkState(
                schema.getFieldSpecFor(columnName) != null || aggregationSourceColumns.contains(columnName),
                "The destination column '" + columnName
                    + "' of the transform function must be present in the schema or as a source column for "
                    + "aggregations");
          }
          FunctionEvaluator expressionEvaluator;
          if (disableGroovy && FunctionEvaluatorFactory.isGroovyExpression(transformFunction)) {
            throw new IllegalStateException(
                "Groovy transform functions are disabled for table config. Found '" + transformFunction
                    + "' for column '" + columnName + "'");
          }
          try {
            expressionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(transformFunction);
          } catch (Exception e) {
            throw new IllegalStateException(
                "Invalid transform function '" + transformFunction + "' for column '" + columnName + "'", e);
          }
          List<String> arguments = expressionEvaluator.getArguments();
          if (arguments.contains(columnName)) {
            throw new IllegalStateException(
                "Arguments of a transform function '" + arguments + "' cannot contain the destination column '"
                    + columnName + "'");
          }
        }
      }

      // Complex configs
      ComplexTypeConfig complexTypeConfig = ingestionConfig.getComplexTypeConfig();
      if (complexTypeConfig != null && schema != null) {
        Map<String, String> prefixesToRename = complexTypeConfig.getPrefixesToRename();
        if (MapUtils.isNotEmpty(prefixesToRename)) {
          Set<String> fieldNames = schema.getColumnNames();
          for (String prefix : prefixesToRename.keySet()) {
            for (String field : fieldNames) {
              Preconditions.checkState(!field.startsWith(prefix),
                  "Fields in the schema may not begin with any prefix specified in the prefixesToRename"
                      + " config. Name conflict with field: " + field + " and prefix: " + prefix);
            }
          }
        }
      }
    }
  }

  /**
   * Currently only, ValueAggregators with fixed width types are allowed, so MIN, MAX, SUM, and COUNT. The reason
   * is that only the {@link org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSVMutableForwardIndex}
   * supports random inserts and lookups. The
   * {@link org.apache.pinot.segment.local.realtime.impl.forward.VarByteSVMutableForwardIndex only supports
   * sequential inserts.
   */
  public static void validateIngestionAggregation(String name) {
    for (AggregationFunctionType functionType : SUPPORTED_INGESTION_AGGREGATIONS) {
      if (functionType.getName().equals(name)) {
        return;
      }
    }
    throw new IllegalStateException(
        String.format("aggregation function %s must be one of %s", name, SUPPORTED_INGESTION_AGGREGATIONS));
  }

  @VisibleForTesting
  static void validateDecoder(StreamConfig streamConfig) {
    if (streamConfig.getDecoderClass().equals("org.apache.pinot.plugin.inputformat.protobuf.ProtoBufMessageDecoder")) {
      // check the existence of the needed decoder props
      if (!streamConfig.getDecoderProperties().containsKey("stream.kafka.decoder.prop.descriptorFile")) {
        throw new IllegalStateException("Missing property of descriptorFile for ProtoBufMessageDecoder");
      }
      if (!streamConfig.getDecoderProperties().containsKey("stream.kafka.decoder.prop.protoClassName")) {
        throw new IllegalStateException("Missing property of protoClassName for ProtoBufMessageDecoder");
      }
    }
  }

  @VisibleForTesting
  static void validateTaskConfigs(TableConfig tableConfig, Schema schema) {
    TableTaskConfig taskConfig = tableConfig.getTaskConfig();
    if (taskConfig != null) {
      for (Map.Entry<String, Map<String, String>> taskConfigEntry : taskConfig.getTaskTypeConfigsMap().entrySet()) {
        String taskTypeConfigName = taskConfigEntry.getKey();
        Map<String, String> taskTypeConfig = taskConfigEntry.getValue();
        // Task configuration cannot be null.
        Preconditions.checkNotNull(taskTypeConfig,
            String.format("Task configuration for \"%s\" cannot be null!", taskTypeConfigName));
        // Schedule key for task config has to be set.
        if (taskTypeConfig.containsKey(SCHEDULE_KEY)) {
          String cronExprStr = taskTypeConfig.get(SCHEDULE_KEY);
          try {
            CronScheduleBuilder.cronSchedule(cronExprStr);
          } catch (Exception e) {
            throw new IllegalStateException(
                String.format("Task %s contains an invalid cron schedule: %s", taskTypeConfigName, cronExprStr), e);
          }
        }
        // Task Specific validation for REALTIME_TO_OFFLINE_TASK_TYPE
        // TODO task specific validate logic should directly call to PinotTaskGenerator API
        if (taskTypeConfigName.equals(REALTIME_TO_OFFLINE_TASK_TYPE)) {
          // check table is not upsert
          Preconditions.checkState(tableConfig.getUpsertMode() == UpsertConfig.Mode.NONE,
              "RealtimeToOfflineTask doesn't support upsert table!");
          // check no malformed period
          TimeUtils.convertPeriodToMillis(taskTypeConfig.getOrDefault("bufferTimePeriod", "2d"));
          TimeUtils.convertPeriodToMillis(taskTypeConfig.getOrDefault("bucketTimePeriod", "1d"));
          TimeUtils.convertPeriodToMillis(taskTypeConfig.getOrDefault("roundBucketTimePeriod", "1s"));
          // check mergeType is correct
          Preconditions.checkState(ImmutableSet.of("CONCAT", "ROLLUP", "DEDUP")
                  .contains(taskTypeConfig.getOrDefault("mergeType", "CONCAT").toUpperCase()),
              "MergeType must be one of [CONCAT, ROLLUP, DEDUP]!");
          // check no mis-configured columns
          Set<String> columnNames = schema.getColumnNames();
          for (Map.Entry<String, String> entry : taskTypeConfig.entrySet()) {
            if (entry.getKey().endsWith(".aggregationType")) {
              Preconditions.checkState(columnNames.contains(StringUtils.removeEnd(entry.getKey(), ".aggregationType")),
                  String.format("Column \"%s\" not found in schema!", entry.getKey()));
              Preconditions.checkState(ImmutableSet.of("SUM", "MAX", "MIN").contains(entry.getValue().toUpperCase()),
                  String.format("Column \"%s\" has invalid aggregate type: %s", entry.getKey(), entry.getValue()));
            }
          }
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
   *  - comparison column exists
   */
  @VisibleForTesting
  static void validateUpsertAndDedupConfig(TableConfig tableConfig, Schema schema) {
    if (tableConfig.getUpsertMode() == UpsertConfig.Mode.NONE && (tableConfig.getDedupConfig() == null
        || !tableConfig.getDedupConfig().isDedupEnabled())) {
      return;
    }

    boolean isUpsertEnabled = tableConfig.getUpsertMode() != UpsertConfig.Mode.NONE;
    boolean isDedupEnabled = tableConfig.getDedupConfig() != null && tableConfig.getDedupConfig().isDedupEnabled();

    // check both upsert and dedup are not enabled simultaneously
    Preconditions.checkState(!(isUpsertEnabled && isDedupEnabled),
        "A table can have either Upsert or Dedup enabled, but not both");
    // check table type is realtime
    Preconditions.checkState(tableConfig.getTableType() == TableType.REALTIME,
        "Upsert/Dedup table is for realtime table only.");
    // primary key exists
    Preconditions.checkState(CollectionUtils.isNotEmpty(schema.getPrimaryKeyColumns()),
        "Upsert/Dedup table must have primary key columns in the schema");
    // consumer type must be low-level
    Map<String, String> streamConfigsMap = IngestionConfigUtils.getStreamConfigMap(tableConfig);
    StreamConfig streamConfig = new StreamConfig(tableConfig.getTableName(), streamConfigsMap);
    Preconditions.checkState(streamConfig.hasLowLevelConsumerType() && !streamConfig.hasHighLevelConsumerType(),
        "Upsert/Dedup table must use low-level streaming consumer type");
    // replica group is configured for routing
    Preconditions.checkState(tableConfig.getRoutingConfig() != null
            && isRoutingStrategyAllowedForUpsert(tableConfig.getRoutingConfig()),
        "Upsert/Dedup table must use strict replica-group (i.e. strictReplicaGroup) based routing");

    // specifically for upsert
    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    if (upsertConfig != null) {
      // no startree index
      Preconditions.checkState(CollectionUtils.isEmpty(tableConfig.getIndexingConfig().getStarTreeIndexConfigs())
              && !tableConfig.getIndexingConfig().isEnableDefaultStarTree(),
          "The upsert table cannot have star-tree index.");

      // comparison column exists
      List<String> comparisonColumns = upsertConfig.getComparisonColumns();
      if (comparisonColumns != null) {
        for (String column : comparisonColumns) {
          Preconditions.checkState(schema.hasColumn(column), "The comparison column does not exist on schema");
        }
      }

      // Delete record column exist and is a BOOLEAN field
      String deleteRecordColumn = upsertConfig.getDeleteRecordColumn();
      if (deleteRecordColumn != null) {
        FieldSpec fieldSpec = schema.getFieldSpecFor(deleteRecordColumn);
        Preconditions.checkState(
            fieldSpec != null && fieldSpec.isSingleValueField() && fieldSpec.getDataType() == DataType.BOOLEAN,
            "The delete record column must be a single-valued BOOLEAN column");
      }
    }
    validateAggregateMetricsForUpsertConfig(tableConfig);
    validateTTLForUpsertConfig(tableConfig, schema);
  }

  /**
   * Validates whether the comparison columns is compatible with Upsert TTL feature.
   * Validation fails when one of the comparison columns is not a numeric value.
   */
  @VisibleForTesting
  static void validateTTLForUpsertConfig(TableConfig tableConfig, Schema schema) {
    if (tableConfig.getUpsertMode() == UpsertConfig.Mode.NONE
        || tableConfig.getUpsertConfig().getMetadataTTL() == 0) {
      return;
    }

    // comparison columns should hold timestamp values in numeric values
    List<String> comparisonColumns = tableConfig.getUpsertConfig().getComparisonColumns();
    if (comparisonColumns != null && !comparisonColumns.isEmpty()) {

      // currently we only support 1 comparison column since we need to fetch endTime in comparisonValue time unit from
      // columnMetadata. If we have multiple comparison columns, we can only use the first comparison column as filter.
      Preconditions.checkState(comparisonColumns.size() == 1,
          String.format("Currently upsert TTL only support 1 comparison columns."));

      String column = comparisonColumns.size() == 1 ? comparisonColumns.get(0)
          : tableConfig.getValidationConfig().getTimeColumnName();
      Preconditions.checkState(schema.getFieldSpecFor(column).getDataType().isNumeric(), String.format(
          "Upsert TTL must have numeric value for the comparison columns. The column %s: %s is not a "
              + "numeric values", column, schema.getFieldSpecFor(column).getDataType()));
    }

    // snapshotEnabled has to be enabled for TTL feature
    Preconditions.checkState(tableConfig.getUpsertConfig().isEnableSnapshot(),
        String.format("Snapshot has to be enabled for TTL feature."));

    // delete feature cannot co-exist with upsert TTL feature.
    // TODO: Fix the deletion handling for TTL. Currently TTL cannot co-exist with delete.
    Preconditions.checkState(tableConfig.getUpsertConfig().getDeleteRecordColumn() == null,
        String.format("TTL feature cannot co-exist with delete in upsert."));
  }

  /**
   * Detects whether both InstanceAssignmentConfig and InstancePartitionsMap are set for a given
   * instance partitions type. Validation fails because the table would ignore InstanceAssignmentConfig
   * when the partitions are already set.
   */
  @VisibleForTesting
  static void validateInstancePartitionsTypeMapConfig(TableConfig tableConfig) {
    if (MapUtils.isEmpty(tableConfig.getInstancePartitionsMap())
        || MapUtils.isEmpty(tableConfig.getInstanceAssignmentConfigMap())) {
      return;
    }
    for (InstancePartitionsType instancePartitionsType : tableConfig.getInstancePartitionsMap().keySet()) {
      Preconditions.checkState(
          !tableConfig.getInstanceAssignmentConfigMap().containsKey(instancePartitionsType.toString()),
          String.format("Both InstanceAssignmentConfigMap and InstancePartitionsMap set for %s",
              instancePartitionsType));
    }
  }

  /**
   * Detects whether both replicaGroupStrategyConfig and replicaGroupPartitionConfig are set for a given
   * table. Validation fails because the table would ignore replicaGroupStrategyConfig
   * when the replicaGroupPartitionConfig is already set.
   */
  @VisibleForTesting
  static void validatePartitionedReplicaGroupInstance(TableConfig tableConfig) {
    if (tableConfig.getValidationConfig().getReplicaGroupStrategyConfig() == null
        || MapUtils.isEmpty(tableConfig.getInstanceAssignmentConfigMap())) {
      return;
    }
    for (Map.Entry<String, InstanceAssignmentConfig> entry: tableConfig.getInstanceAssignmentConfigMap().entrySet()) {
      boolean isNullReplicaGroupPartitionConfig = entry.getValue().getReplicaGroupPartitionConfig() == null;
      Preconditions.checkState(isNullReplicaGroupPartitionConfig,
          "Both replicaGroupStrategyConfig and replicaGroupPartitionConfig is provided");
    }
  }

  /**
   * Validates metrics aggregation when upsert config is enabled
   * - Metrics aggregation cannot be enabled when Upsert Config is enabled.
   * - Aggregation cannot be enabled in the Ingestion Config and Indexing Config at the same time.
   */
  private static void validateAggregateMetricsForUpsertConfig(TableConfig config) {
    boolean isAggregateMetricsEnabledInIndexingConfig = config.getIndexingConfig().isAggregateMetrics();
    boolean hasAggregationConfigs = config.getIngestionConfig() != null && CollectionUtils.isNotEmpty(
        config.getIngestionConfig().getAggregationConfigs());
    boolean bothAggregationConfigsEnabled = isAggregateMetricsEnabledInIndexingConfig && hasAggregationConfigs;
    boolean anyOneAggregationConfigsEnabled = isAggregateMetricsEnabledInIndexingConfig || hasAggregationConfigs;
    Preconditions.checkState(!bothAggregationConfigsEnabled,
        "Metrics aggregation cannot be enabled in the Indexing Config and Ingestion Config at the same time");
    Preconditions.checkState(!anyOneAggregationConfigsEnabled,
        "Metrics aggregation and upsert cannot be enabled together");
  }

  /**
   * Validates the partial upsert-related configurations:
   *  - Null handling must be enabled
   *  - Merger cannot be applied to private key columns
   *  - Merger cannot be applied to non-existing columns
   *  - INCREMENT merger must be applied to numeric columns
   *  - APPEND/UNION merger cannot be applied to single-value columns
   *  - INCREMENT merger cannot be applied to date time column
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
      UpsertConfig.Strategy columnStrategy = entry.getValue();
      Preconditions.checkState(!primaryKeyColumns.contains(column), "Merger cannot be applied to primary key columns");

      if (upsertConfig.getComparisonColumns() != null) {
        Preconditions.checkState(!upsertConfig.getComparisonColumns().contains(column),
            "Merger cannot be applied to comparison column");
      } else {
        Preconditions.checkState(!tableConfig.getValidationConfig().getTimeColumnName().equals(column),
            "Merger cannot be applied to time column");
      }

      FieldSpec fieldSpec = schema.getFieldSpecFor(column);
      Preconditions.checkState(fieldSpec != null, "Merger cannot be applied to non-existing column: %s", column);

      if (columnStrategy == UpsertConfig.Strategy.INCREMENT) {
        Preconditions.checkState(fieldSpec.getDataType().getStoredType().isNumeric(),
            "INCREMENT merger cannot be applied to non-numeric column: %s", column);
        Preconditions.checkState(!schema.getDateTimeNames().contains(column),
            "INCREMENT merger cannot be applied to date time column: %s", column);
      } else if (columnStrategy == UpsertConfig.Strategy.APPEND || columnStrategy == UpsertConfig.Strategy.UNION) {
        Preconditions.checkState(!fieldSpec.isSingleValueField(),
            "%s merger cannot be applied to single-value column: %s", columnStrategy.toString(), column);
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
        Preconditions.checkState(segmentAge != null,
            "Must provide 'segmentAge' for segmentSelectorType: %s in tier: %s", segmentSelectorType, tierName);
        Preconditions.checkState(TimeUtils.isPeriodValid(segmentAge),
            "segmentAge: %s must be a valid period string (eg. 30d, 24h) in tier: %s", segmentAge, tierName);
      } else if (!segmentSelectorType.equalsIgnoreCase(TierFactory.FIXED_SEGMENT_SELECTOR_TYPE)) {
        throw new IllegalStateException(
            "Unsupported segmentSelectorType: " + segmentSelectorType + " in tier: " + tierName);
      }

      String storageType = tierConfig.getStorageType();
      String serverTag = tierConfig.getServerTag();
      if (storageType.equalsIgnoreCase(TierFactory.PINOT_SERVER_STORAGE_TYPE)) {
        Preconditions.checkState(serverTag != null, "Must provide 'serverTag' for storageType: %s in tier: %s",
            storageType, tierName);
        Preconditions.checkState(TagNameUtils.isServerTag(serverTag),
            "serverTag: %s must have a valid server tag format (<tenantName>_OFFLINE or <tenantName>_REALTIME) in "
                + "tier: %s", serverTag, tierName);
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
    Set<String> jsonIndexColumns = new HashSet<>();
    // Ignore jsonIndexColumns when jsonIndexConfigs is configured
    if (indexingConfig.getJsonIndexConfigs() != null) {
      jsonIndexColumns.addAll(indexingConfig.getJsonIndexConfigs().keySet());
    } else {
      if (indexingConfig.getJsonIndexColumns() != null) {
        jsonIndexColumns.addAll(indexingConfig.getJsonIndexColumns());
      }
    }
    for (String columnName : jsonIndexColumns) {
      columnNameToConfigMap.put(columnName, "Json Index Config");
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
            schema.getFieldSpecFor(rangeIndexCol).getDataType().isNumeric() || !noDictionaryColumnsSet.contains(
                rangeIndexCol), "Cannot create a range index on non-numeric/no-dictionary column " + rangeIndexCol);
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

    for (String jsonIndexColumn : jsonIndexColumns) {
      FieldSpec fieldSpec = schema.getFieldSpecFor(jsonIndexColumn);
      Preconditions.checkState(
          fieldSpec.isSingleValueField() && fieldSpec.getDataType().getStoredType() == DataType.STRING,
          "Json index can only be created for single value String column. Invalid for column: %s", jsonIndexColumn);
    }
  }

  /**
   * Validates the Field Config List in the given TableConfig
   * Ensures that every referred column name exists in the corresponding schema
   * Additional checks for TEXT and FST index types
   * Validates index compatibility for forward index disabled columns
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

      if (indexingConfigs != null) {
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
          default:
            break;
        }

        // Validate the forward index disabled compatibility with other indexes if enabled for this column
        validateForwardIndexDisabledIndexCompatibility(columnName, fieldConfig, indexingConfigs, noDictionaryColumns,
            schema);
      }

      if (CollectionUtils.isNotEmpty(fieldConfig.getIndexTypes())) {
        for (FieldConfig.IndexType indexType : fieldConfig.getIndexTypes()) {
          switch (indexType) {
            case FST:
              Preconditions.checkArgument(fieldConfig.getEncodingType() == FieldConfig.EncodingType.DICTIONARY,
                  "FST Index is only enabled on dictionary encoded columns");
              Preconditions.checkState(fieldConfigColSpec.isSingleValueField()
                      && fieldConfigColSpec.getDataType().getStoredType() == DataType.STRING,
                  "FST Index is only supported for single value string columns");
              break;
            case TEXT:
              Preconditions.checkState(fieldConfigColSpec.getDataType().getStoredType() == DataType.STRING,
                  "TEXT Index is only supported for string columns");
              break;
            case TIMESTAMP:
              Preconditions.checkState(fieldConfigColSpec.getDataType() == DataType.TIMESTAMP,
                  "TIMESTAMP Index is only supported for timestamp columns");
              break;
            default:
              break;
          }
        }
      }
    }
  }

  /**
   * Validates the compatibility of the indexes if the column has the forward index disabled. Throws exceptions due to
   * compatibility mismatch. The checks performed are:
   *
   *     - Validate that either no range index exists for column or the range index version is at least 2 and isn't a
   *       multi-value column (since multi-value defaults to index v1).
   *
   * To rebuild the forward index when it has been disabled the dictionary and inverted index must be enabled for the
   * given column. If either the inverted index or dictionary are disabled then the only way to get the forward index
   * back or generate a new index for existing segments is to either refresh or back-fill the segments.
   */
  private static void validateForwardIndexDisabledIndexCompatibility(String columnName, FieldConfig fieldConfig,
      IndexingConfig indexingConfigs, List<String> noDictionaryColumns, Schema schema) {
    Map<String, String> fieldConfigProperties = fieldConfig.getProperties();
    if (fieldConfigProperties == null) {
      return;
    }

    boolean forwardIndexDisabled =
        Boolean.parseBoolean(fieldConfigProperties.getOrDefault(FieldConfig.FORWARD_INDEX_DISABLED,
            FieldConfig.DEFAULT_FORWARD_INDEX_DISABLED));
    if (!forwardIndexDisabled) {
      return;
    }

    FieldSpec fieldSpec = schema.getFieldSpecFor(columnName);
    // Check for the range index since the index itself relies on the existence of the forward index to work.
    if (indexingConfigs.getRangeIndexColumns() != null && indexingConfigs.getRangeIndexColumns().contains(columnName)) {
      Preconditions.checkState(fieldSpec.isSingleValueField(), String.format("Feature not supported for multi-value "
          + "columns with range index. Cannot disable forward index for column %s. Disable range index on this "
          + "column to use this feature", columnName));
      Preconditions.checkState(indexingConfigs.getRangeIndexVersion() == BitSlicedRangeIndexCreator.VERSION,
          String.format("Feature not supported for single-value columns with range index version < 2. Cannot disable "
              + "forward index for column %s. Either disable range index or create range index with"
              + " version >= 2 to use this feature", columnName));
    }

    Preconditions.checkState(
        !indexingConfigs.isOptimizeDictionaryForMetrics() && !indexingConfigs.isOptimizeDictionary(),
        String.format("Dictionary override optimization options (OptimizeDictionary, optimizeDictionaryForMetrics)"
            + " not supported with forward index for column: %s, disabled", columnName));

    boolean hasDictionary = fieldConfig.getEncodingType() == FieldConfig.EncodingType.DICTIONARY
        || noDictionaryColumns == null || !noDictionaryColumns.contains(columnName);
    boolean hasInvertedIndex = indexingConfigs.getInvertedIndexColumns() != null
        && indexingConfigs.getInvertedIndexColumns().contains(columnName);

    if (!hasDictionary || !hasInvertedIndex) {
      LOGGER.warn("Forward index has been disabled for column {}. Either dictionary ({}) and / or inverted index ({}) "
              + "has been disabled. If the forward index needs to be regenerated or another index added please refresh "
              + "or back-fill the forward index as it cannot be rebuilt without dictionary and inverted index.",
          columnName, hasDictionary ? "enabled" : "disabled", hasInvertedIndex ? "enabled" : "disabled");
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
    indexingConfig.setOnHeapDictionaryColumns(
        sanitizeListBasedIndexingColumns(indexingConfig.getOnHeapDictionaryColumns()));
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
   * TODO: After deprecating "replicasPerPartition", we can change this function's behavior to always overwrite
   * config to "replication" only.
   *
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
        requestReplication = tableConfig.getReplication();
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
      int replicasPerPartition;
      try {
        replicasPerPartition = tableConfig.getReplication();
        if (replicasPerPartition < defaultTableMinReplicas) {
          LOGGER.info(
              "Creating table with minimum replicasPerPartition of: {} instead of requested replicasPerPartition: {}",
              defaultTableMinReplicas, replicasPerPartition);
          segmentsConfig.setReplicasPerPartition(String.valueOf(defaultTableMinReplicas));
        }
      } catch (NumberFormatException e) {
        throw new IllegalStateException("Invalid replicasPerPartition number", e);
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
            throw new IllegalStateException(
                String.format("Invalid storage quota: %d, max allowed size: %d", quotaConfig.getStorageInBytes(),
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
    Preconditions.checkNotNull(offlineTableConfig,
        "Found null offline table config in hybrid table check for table: %s", rawTableName);
    Preconditions.checkNotNull(realtimeTableConfig,
        "Found null realtime table config in hybrid table check for table: %s", rawTableName);
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

  // enum of all the skip-able validation types.
  public enum ValidationType {
    ALL, TASK, UPSERT
  }

  /**
   * needsEmptySegmentPruner checks if EmptySegmentPruner is needed for a TableConfig.
   * @param tableConfig Input table config.
   */
  public static boolean needsEmptySegmentPruner(TableConfig tableConfig) {
    if (isKinesisConfigured(tableConfig)) {
      return true;
    }
    RoutingConfig routingConfig = tableConfig.getRoutingConfig();
    if (routingConfig == null) {
      return false;
    }
    List<String> segmentPrunerTypes = routingConfig.getSegmentPrunerTypes();
    if (segmentPrunerTypes == null || segmentPrunerTypes.isEmpty()) {
      return false;
    }
    for (String segmentPrunerType : segmentPrunerTypes) {
      if (RoutingConfig.EMPTY_SEGMENT_PRUNER_TYPE.equalsIgnoreCase(segmentPrunerType)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isKinesisConfigured(TableConfig tableConfig) {
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    if (indexingConfig != null) {
      Map<String, String> streamConfig = indexingConfig.getStreamConfigs();
      if (streamConfig != null && KINESIS_STREAM_TYPE.equals(streamConfig.get(StreamConfigProperties.STREAM_TYPE))) {
        return true;
      }
    }
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    if (ingestionConfig == null) {
      return false;
    }
    StreamIngestionConfig streamIngestionConfig = ingestionConfig.getStreamIngestionConfig();
    if (streamIngestionConfig == null) {
      return false;
    }
    for (Map<String, String> config : streamIngestionConfig.getStreamConfigMaps()) {
      if (config != null && KINESIS_STREAM_TYPE.equals(config.get(StreamConfigProperties.STREAM_TYPE))) {
        return true;
      }
    }
    return false;
  }

  private static boolean isRoutingStrategyAllowedForUpsert(@Nonnull RoutingConfig routingConfig) {
    String instanceSelectorType = routingConfig.getInstanceSelectorType();
    return UPSERT_DEDUP_ALLOWED_ROUTING_STRATEGIES.stream().anyMatch(x -> x.equalsIgnoreCase(instanceSelectorType));
  }

  /**
   * Helper method to extract TableConfig in updated syntax from current TableConfig.
   * <ul>
   *   <li>Moves all index configs to FieldConfig.indexes</li>
   *   <li>Clean up index related configs from IndexingConfig and FieldConfig.IndexTypes</li>
   * </ul>
   */
  public static TableConfig createTableConfigFromOldFormat(TableConfig tableConfig, Schema schema) {
    TableConfig clone = new TableConfig(tableConfig);
    for (IndexType<?, ?, ?> indexType : IndexService.getInstance().getAllIndexes()) {
      // get all the index data in new format
      indexType.convertToNewFormat(clone, schema);
    }
    // cleanup the indexTypes field from all FieldConfig items
    if (clone.getFieldConfigList() != null) {
      List<FieldConfig> cleanFieldConfigList = new ArrayList<>();
      for (FieldConfig fieldConfig : clone.getFieldConfigList()) {
        cleanFieldConfigList.add(new FieldConfig.Builder(fieldConfig)
            .withIndexTypes(null)
            .build());
      }
      clone.setFieldConfigList(cleanFieldConfigList);
    }
    return clone;
  }
}
