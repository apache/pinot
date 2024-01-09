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
import org.apache.pinot.segment.local.recordtransformer.SchemaConformingTransformer;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitSlicedRangeIndexCreator;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.StarTreeAggregationConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.SchemaConformingTransformerConfig;
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
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.quartz.CronScheduleBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.AggregationFunctionType.*;


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
  private static final String UPSERT_COMPACTION_TASK_TYPE = "UpsertCompactionTask";

  // this is duplicate with KinesisConfig.STREAM_TYPE, while instead of use KinesisConfig.STREAM_TYPE directly, we
  // hardcode the value here to avoid pulling the entire pinot-kinesis module as dependency.
  private static final String KINESIS_STREAM_TYPE = "kinesis";
  private static final EnumSet<AggregationFunctionType> SUPPORTED_INGESTION_AGGREGATIONS =
      EnumSet.of(SUM, MIN, MAX, COUNT, DISTINCTCOUNTHLL, SUMPRECISION, DISTINCTCOUNTHLLPLUS);

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
      validateTableSchemaConfig(tableConfig);
      validateValidationConfig(tableConfig, schema);

      StreamConfig streamConfig = null;
      validateIngestionConfig(tableConfig, schema, disableGroovy);
      // Only allow realtime tables with non-null stream.type and LLC consumer.type
      if (tableConfig.getTableType() == TableType.REALTIME) {
        Map<String, String> streamConfigMap = IngestionConfigUtils.getStreamConfigMap(tableConfig);
        try {
          // Validate that StreamConfig can be created
          streamConfig = new StreamConfig(tableConfig.getTableName(), streamConfigMap);
        } catch (Exception e) {
          throw new IllegalStateException("Could not create StreamConfig using the streamConfig map", e);
        }
        validateDecoder(streamConfig);
        // if segmentSizeBytes is specified, rows must be zero.
        if (streamConfigMap.containsKey(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_SEGMENT_SIZE)
            || streamConfigMap.containsKey(StreamConfigProperties.DEPRECATED_SEGMENT_FLUSH_DESIRED_SIZE)) {
          Preconditions.checkState(streamConfig.getFlushThresholdRows() == 0,
              String.format("Invalid config: %s=%d, it must be set to 0 for size based segment threshold to work.",
                  StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, streamConfig.getFlushThresholdRows()));
        }
      }
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
   * Validates the table name with the following rule:
   * - Schema name should either be null or match the raw table name
   */
  private static void validateTableSchemaConfig(TableConfig tableConfig) {
    // Ensure that table is not created if schema is not present
    String rawTableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());
    String schemaName = tableConfig.getValidationConfig().getSchemaName();
    if (schemaName != null && !schemaName.equals(rawTableName)) {
      throw new IllegalStateException("Schema name: " + schemaName + " does not match table name: " + rawTableName);
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

      if (tableConfig.getReplication() < 2) {
        throw new IllegalStateException("peerSegmentDownloadScheme can't be used when replication is < 2");
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
      // stream config map can either be in ingestion config or indexing config. cannot be in both places
      if (ingestionConfig.getStreamIngestionConfig() != null) {
        IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
        Preconditions.checkState(indexingConfig == null || MapUtils.isEmpty(indexingConfig.getStreamConfigs()),
            "Should not use indexingConfig#getStreamConfigs if ingestionConfig#StreamIngestionConfig is provided");
        List<Map<String, String>> streamConfigMaps = ingestionConfig.getStreamIngestionConfig().getStreamConfigMaps();
        Preconditions.checkState(streamConfigMaps.size() == 1, "Only 1 stream is supported in REALTIME table");
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

          FieldSpec fieldSpec = null;
          if (schema != null) {
            fieldSpec = schema.getFieldSpecFor(columnName);
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
          AggregationFunctionType functionType =
              AggregationFunctionType.getAggregationFunctionType(functionContext.getFunctionName());
          validateIngestionAggregation(functionType);

          List<ExpressionContext> arguments = functionContext.getArguments();
          int numArguments = arguments.size();
          if (functionType == DISTINCTCOUNTHLL) {
            Preconditions.checkState(numArguments >= 1 && numArguments <= 2,
                "DISTINCT_COUNT_HLL can have at most two arguments: %s", aggregationConfig);
            if (numArguments == 2) {
              ExpressionContext secondArgument = arguments.get(1);
              Preconditions.checkState(secondArgument.getType() == ExpressionContext.Type.LITERAL,
                  "Second argument of DISTINCT_COUNT_HLL must be literal: %s", aggregationConfig);
              String literal = secondArgument.getLiteral().getStringValue();
              Preconditions.checkState(StringUtils.isNumeric(literal),
                  "Second argument of DISTINCT_COUNT_HLL must be a number: %s", aggregationConfig);
            }
            if (fieldSpec != null) {
              DataType dataType = fieldSpec.getDataType();
              Preconditions.checkState(dataType == DataType.BYTES,
                  "Result type for DISTINCT_COUNT_HLL must be BYTES: %s", aggregationConfig);
            }
          } else if (functionType == DISTINCTCOUNTHLLPLUS) {
            Preconditions.checkState(numArguments >= 1 && numArguments <= 3,
                "DISTINCT_COUNT_HLL_PLUS can have at most three arguments: %s", aggregationConfig);
            if (numArguments == 2) {
              ExpressionContext secondArgument = arguments.get(1);
              Preconditions.checkState(secondArgument.getType() == ExpressionContext.Type.LITERAL,
                  "Second argument of DISTINCT_COUNT_HLL_PLUS must be literal: %s", aggregationConfig);
              String literal = secondArgument.getLiteral().getStringValue();
              Preconditions.checkState(StringUtils.isNumeric(literal),
                  "Second argument of DISTINCT_COUNT_HLL_PLUS must be a number: %s", aggregationConfig);
            }
            if (numArguments == 3) {
              ExpressionContext thirdArgument = arguments.get(2);
              Preconditions.checkState(thirdArgument.getType() == ExpressionContext.Type.LITERAL,
                  "Third argument of DISTINCT_COUNT_HLL_PLUS must be literal: %s", aggregationConfig);
              String literal = thirdArgument.getLiteral().getStringValue();
              Preconditions.checkState(StringUtils.isNumeric(literal),
                  "Third argument of DISTINCT_COUNT_HLL_PLUS must be a number: %s", aggregationConfig);
            }
            if (fieldSpec != null) {
              DataType dataType = fieldSpec.getDataType();
              Preconditions.checkState(dataType == DataType.BYTES,
                  "Result type for DISTINCT_COUNT_HLL_PLUS must be BYTES: %s", aggregationConfig);
            }
          } else if (functionType == SUMPRECISION) {
            Preconditions.checkState(numArguments >= 2 && numArguments <= 3,
                "SUM_PRECISION must specify precision (required), scale (optional): %s", aggregationConfig);
            ExpressionContext secondArgument = arguments.get(1);
            Preconditions.checkState(secondArgument.getType() == ExpressionContext.Type.LITERAL,
                "Second argument of SUM_PRECISION must be literal: %s", aggregationConfig);
            String literal = secondArgument.getLiteral().getStringValue();
            Preconditions.checkState(StringUtils.isNumeric(literal),
                "Second argument of SUM_PRECISION must be a number: %s", aggregationConfig);
            if (fieldSpec != null) {
              DataType dataType = fieldSpec.getDataType();
              Preconditions.checkState(dataType == DataType.BIG_DECIMAL || dataType == DataType.BYTES,
                  "Result type for DISTINCT_COUNT_HLL must be BIG_DECIMAL or BYTES: %s", aggregationConfig);
            }
          } else {
            Preconditions.checkState(numArguments == 1, "%s can only have one argument: %s", functionType,
                aggregationConfig);
          }
          ExpressionContext firstArgument = arguments.get(0);
          Preconditions.checkState(firstArgument.getType() == ExpressionContext.Type.IDENTIFIER,
              "First argument of aggregation function: %s must be identifier, got: %s", functionType,
              firstArgument.getType());

          aggregationSourceColumns.add(firstArgument.getIdentifier());
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

      SchemaConformingTransformerConfig schemaConformingTransformerConfig =
          ingestionConfig.getSchemaConformingTransformerConfig();
      if (null != schemaConformingTransformerConfig && null != schema) {
        SchemaConformingTransformer.validateSchema(schema, schemaConformingTransformerConfig);
      }
    }
  }

  public static void validateIngestionAggregation(AggregationFunctionType functionType) {
    Preconditions.checkState(SUPPORTED_INGESTION_AGGREGATIONS.contains(functionType),
        "Aggregation function: %s must be one of: %s", functionType, SUPPORTED_INGESTION_AGGREGATIONS);
  }

  @VisibleForTesting
  static void validateDecoder(StreamConfig streamConfig) {
    if (streamConfig.getDecoderClass().equals("org.apache.pinot.plugin.inputformat.protobuf.ProtoBufMessageDecoder")) {
      String descriptorFilePath = "descriptorFile";
      String protoClassName = "protoClassName";
      // check the existence of the needed decoder props
      if (!streamConfig.getDecoderProperties().containsKey(descriptorFilePath)) {
        throw new IllegalStateException("Missing property of descriptorFile for ProtoBufMessageDecoder");
      }
      if (!streamConfig.getDecoderProperties().containsKey(protoClassName)) {
        throw new IllegalStateException("Missing property of protoClassName for ProtoBufMessageDecoder");
      }
    }
  }

  public final static EnumSet<AggregationFunctionType> AVAILABLE_CORE_VALUE_AGGREGATORS =
      EnumSet.of(MIN, MAX, SUM, DISTINCTCOUNTHLL, DISTINCTCOUNTRAWHLL, DISTINCTCOUNTTHETASKETCH,
          DISTINCTCOUNTRAWTHETASKETCH, DISTINCTCOUNTTUPLESKETCH, DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH,
          SUMVALUESINTEGERSUMTUPLESKETCH, AVGVALUEINTEGERSUMTUPLESKETCH, DISTINCTCOUNTHLLPLUS, DISTINCTCOUNTRAWHLLPLUS,
          DISTINCTCOUNTCPCSKETCH, DISTINCTCOUNTRAWCPCSKETCH, DISTINCTCOUNTULL, DISTINCTCOUNTRAWULL);

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
              try {
                // check that it's a valid aggregation function type
                AggregationFunctionType aft = AggregationFunctionType.getAggregationFunctionType(entry.getValue());
                // check that a value aggregator is available
                if (!AVAILABLE_CORE_VALUE_AGGREGATORS.contains(aft)) {
                  throw new IllegalArgumentException("ValueAggregator not enabled for type: " + aft.toString());
                }
              } catch (IllegalArgumentException e) {
                String err =
                    String.format("Column \"%s\" has invalid aggregate type: %s", entry.getKey(), entry.getValue());
                throw new IllegalStateException(err);
              }
            }
          }
        } else if (taskTypeConfigName.equals(UPSERT_COMPACTION_TASK_TYPE)) {
          // check table is realtime
          Preconditions.checkState(tableConfig.getTableType() == TableType.REALTIME,
              "UpsertCompactionTask only supports realtime tables!");
          // check upsert enabled
          Preconditions.checkState(tableConfig.isUpsertEnabled(), "Upsert must be enabled for UpsertCompactionTask");

          // check no malformed period
          if (taskTypeConfig.containsKey("bufferTimePeriod")) {
            TimeUtils.convertPeriodToMillis(taskTypeConfig.get("bufferTimePeriod"));
          }
          // check maxNumRecordsPerSegment
          if (taskTypeConfig.containsKey("invalidRecordsThresholdPercent")) {
            Preconditions.checkState(Double.parseDouble(taskTypeConfig.get("invalidRecordsThresholdPercent")) > 0
                    && Double.parseDouble(taskTypeConfig.get("invalidRecordsThresholdPercent")) <= 100,
                "invalidRecordsThresholdPercent must be > 0 and <= 100");
          }
          // check invalidRecordsThresholdCount
          if (taskTypeConfig.containsKey("invalidRecordsThresholdCount")) {
            Preconditions.checkState(Long.parseLong(taskTypeConfig.get("invalidRecordsThresholdCount")) >= 1,
                "invalidRecordsThresholdCount must be >= 1");
          }
          // check that either invalidRecordsThresholdPercent or invalidRecordsThresholdCount was provided
          Preconditions.checkState(
              taskTypeConfig.containsKey("invalidRecordsThresholdPercent") || taskTypeConfig.containsKey(
                  "invalidRecordsThresholdCount"),
              "invalidRecordsThresholdPercent or invalidRecordsThresholdCount or both must be provided");
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
    // replica group is configured for routing
    Preconditions.checkState(
        tableConfig.getRoutingConfig() != null && isRoutingStrategyAllowedForUpsert(tableConfig.getRoutingConfig()),
        "Upsert/Dedup table must use strict replica-group (i.e. strictReplicaGroup) based routing");
    Preconditions.checkState(tableConfig.getTenantConfig().getTagOverrideConfig() == null,
        "Upsert/Dedup table cannot use tenant tag override");

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
            fieldSpec != null,
            "Invalid delete record column found");
        Preconditions.checkState(
            fieldSpec.isSingleValueField(),
            "The delete record column must be a single-valued column");
        DataType dataType = fieldSpec.getDataType();
        Preconditions.checkState(dataType == DataType.BOOLEAN || dataType == DataType.STRING
                || dataType.isNumeric(), "The delete record column must be of type: String / Boolean / Numeric");
      }

      String outOfOrderRecordColumn = upsertConfig.getOutOfOrderRecordColumn();
      Preconditions.checkState(
          outOfOrderRecordColumn == null || !upsertConfig.isDropOutOfOrderRecord(),
          "outOfOrderRecordColumn and dropOutOfOrderRecord shouldn't exist together for upsert table");

      if (outOfOrderRecordColumn != null) {
        FieldSpec fieldSpec = schema.getFieldSpecFor(outOfOrderRecordColumn);
        Preconditions.checkState(
            fieldSpec != null && fieldSpec.isSingleValueField() && fieldSpec.getDataType() == DataType.BOOLEAN,
            "The outOfOrderRecordColumn must be a single-valued BOOLEAN column");
      }
    }

    Preconditions.checkState(
        tableConfig.getInstanceAssignmentConfigMap() == null || !tableConfig.getInstanceAssignmentConfigMap()
            .containsKey(InstancePartitionsType.COMPLETED),
        "InstanceAssignmentConfig for COMPLETED is not allowed for upsert tables");
    validateAggregateMetricsForUpsertConfig(tableConfig);
    validateTTLForUpsertConfig(tableConfig, schema);
  }

  /**
   * Validates the upsert config related to TTL.
   */
  @VisibleForTesting
  static void validateTTLForUpsertConfig(TableConfig tableConfig, Schema schema) {
    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    if (upsertConfig == null || (upsertConfig.getMetadataTTL() == 0 && upsertConfig.getDeletedKeysTTL() == 0)) {
      return;
    }

    List<String> comparisonColumns = upsertConfig.getComparisonColumns();
    if (CollectionUtils.isNotEmpty(comparisonColumns)) {
      Preconditions.checkState(comparisonColumns.size() == 1,
          "MetadataTTL / DeletedKeysTTL does not work with multiple comparison columns");
      String comparisonColumn = comparisonColumns.get(0);
      DataType comparisonColumnDataType = schema.getFieldSpecFor(comparisonColumn).getDataType();
      Preconditions.checkState(comparisonColumnDataType.isNumeric(),
          "MetadataTTL / DeletedKeysTTL must have comparison column: %s in numeric type, found: %s",
          comparisonColumn, comparisonColumnDataType);
    }

    if (upsertConfig.getMetadataTTL() > 0) {
      Preconditions.checkState(upsertConfig.isEnableSnapshot(), "Upsert TTL must have snapshot enabled");
    }

    if (upsertConfig.getDeletedKeysTTL() > 0) {
      Preconditions.checkState(upsertConfig.getDeleteRecordColumn() != null,
          "Deleted Keys TTL can only be enabled with deleteRecordColumn set.");
    }
  }

  /**
   * Detects whether both InstanceAssignmentConfig and InstancePartitionsMap are set for a given
   * instance partitions type. Validation fails because the table would ignore InstanceAssignmentConfig
   * when the partitions are already set.
   */
  @VisibleForTesting
  static void validateInstancePartitionsTypeMapConfig(TableConfig tableConfig) {
    if (MapUtils.isEmpty(tableConfig.getInstancePartitionsMap()) || MapUtils.isEmpty(
        tableConfig.getInstanceAssignmentConfigMap())) {
      return;
    }

    for (InstancePartitionsType instancePartitionsType : InstancePartitionsType.values()) {
      if (tableConfig.getInstanceAssignmentConfigMap().containsKey(instancePartitionsType.toString())) {
        InstanceAssignmentConfig instanceAssignmentConfig =
            tableConfig.getInstanceAssignmentConfigMap().get(instancePartitionsType.toString());
        if (instanceAssignmentConfig.getPartitionSelector()
            == InstanceAssignmentConfig.PartitionSelector.MIRROR_SERVER_SET_PARTITION_SELECTOR) {
          Preconditions.checkState(
              tableConfig.getInstancePartitionsMap().containsKey(instancePartitionsType),
              String.format("Both InstanceAssignmentConfigMap and InstancePartitionsMap needed for %s, as "
                      + "MIRROR_SERVER_SET_PARTITION_SELECTOR is used",
                  instancePartitionsType));
        } else {
          Preconditions.checkState(
              !tableConfig.getInstancePartitionsMap().containsKey(instancePartitionsType),
              String.format("Both InstanceAssignmentConfigMap and InstancePartitionsMap set for %s",
                  instancePartitionsType));
        }
      }
    }
  }

  /**
   * Detects whether both replicaGroupStrategyConfig and replicaGroupPartitionConfig are set for a given
   * table. Validation fails because the table would ignore replicaGroupStrategyConfig
   * when the replicaGroupPartitionConfig is already set.
   */
  @VisibleForTesting
  static void validatePartitionedReplicaGroupInstance(TableConfig tableConfig) {
    if (tableConfig.getValidationConfig().getReplicaGroupStrategyConfig() == null || MapUtils.isEmpty(
        tableConfig.getInstanceAssignmentConfigMap())) {
      return;
    }
    for (Map.Entry<String, InstanceAssignmentConfig> entry : tableConfig.getInstanceAssignmentConfigMap().entrySet()) {
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
  private static void validateIndexingConfig(IndexingConfig indexingConfig, @Nullable Schema schema) {
    if (schema == null) {
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
        List<String> functionColumnPairs = starTreeIndexConfig.getFunctionColumnPairs();
        if (functionColumnPairs != null) {
          for (String functionColumnPair : functionColumnPairs) {
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
        }
        List<StarTreeAggregationConfig> aggregationConfigs = starTreeIndexConfig.getAggregationConfigs();
        if (aggregationConfigs != null) {
          for (StarTreeAggregationConfig aggregationConfig : aggregationConfigs) {
            AggregationFunctionColumnPair columnPair;
            try {
              columnPair = AggregationFunctionColumnPair.fromAggregationConfig(aggregationConfig);
            } catch (Exception e) {
              throw new IllegalStateException("Invalid StarTreeIndex config: " + aggregationConfig);
            }
            String columnName = columnPair.getColumn();
            if (!columnName.equals(AggregationFunctionColumnPair.STAR)) {
              columnNameToConfigMap.put(columnName, STAR_TREE_CONFIG_NAME);
            }
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
      IndexingConfig indexingConfig, @Nullable Schema schema) {
    if (fieldConfigList == null) {
      return;
    }
    assert indexingConfig != null;

    for (FieldConfig fieldConfig : fieldConfigList) {
      String columnName = fieldConfig.getName();
      EncodingType encodingType = fieldConfig.getEncodingType();
      Preconditions.checkArgument(encodingType != null, "Encoding type must be specified for column: %s", columnName);
      CompressionCodec compressionCodec = fieldConfig.getCompressionCodec();
      switch (encodingType) {
        case RAW:
          Preconditions.checkArgument(compressionCodec == null || compressionCodec.isApplicableToRawIndex(),
              "Compression codec: %s is not applicable to raw index", compressionCodec);
          break;
        case DICTIONARY:
          Preconditions.checkArgument(compressionCodec == null || compressionCodec.isApplicableToDictEncodedIndex(),
              "Compression codec: %s is not applicable to dictionary encoded index", compressionCodec);
          List<String> noDictionaryColumns = indexingConfig.getNoDictionaryColumns();
          Preconditions.checkArgument(noDictionaryColumns == null || !noDictionaryColumns.contains(columnName),
              "FieldConfig encoding type is different from indexingConfig for column: %s", columnName);
          Map<String, String> noDictionaryConfig = indexingConfig.getNoDictionaryConfig();
          Preconditions.checkArgument(noDictionaryConfig == null || !noDictionaryConfig.containsKey(columnName),
              "FieldConfig encoding type is different from indexingConfig for column: %s", columnName);
          break;
        default:
          break;
      }

      if (schema == null) {
        return;
      }
      FieldSpec fieldSpec = schema.getFieldSpecFor(columnName);
      Preconditions.checkState(fieldSpec != null,
          "Column: %s defined in field config list must be a valid column defined in the schema", columnName);

      // Validate the forward index disabled compatibility with other indexes if enabled for this column
      validateForwardIndexDisabledIndexCompatibility(columnName, fieldConfig, indexingConfig, schema);

      if (CollectionUtils.isNotEmpty(fieldConfig.getIndexTypes())) {
        for (FieldConfig.IndexType indexType : fieldConfig.getIndexTypes()) {
          switch (indexType) {
            case INVERTED:
              Preconditions.checkState(fieldConfig.getEncodingType() == EncodingType.DICTIONARY,
                  "Cannot create inverted index on column: %s, it can only be applied to dictionary encoded columns",
                  columnName);
              break;
            case TEXT:
              Preconditions.checkState(fieldSpec.getDataType().getStoredType() == DataType.STRING,
                  "Cannot create text index on column: %s, it can only be applied to string columns", columnName);
              break;
            case FST:
              Preconditions.checkState(
                  fieldConfig.getEncodingType() == EncodingType.DICTIONARY && fieldSpec.isSingleValueField()
                      && fieldSpec.getDataType().getStoredType() == DataType.STRING,
                  "Cannot create FST index on column: %s, it can only be applied to dictionary encoded single value "
                      + "string columns", columnName);
              break;
            case TIMESTAMP:
              Preconditions.checkState(fieldSpec.getDataType() == DataType.TIMESTAMP,
                  "Cannot create timestamp index on column: %s, it can only be applied to timestamp columns",
                  columnName);
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
      IndexingConfig indexingConfig, Schema schema) {
    Map<String, String> fieldConfigProperties = fieldConfig.getProperties();
    if (fieldConfigProperties == null) {
      return;
    }

    boolean forwardIndexDisabled = Boolean.parseBoolean(
        fieldConfigProperties.getOrDefault(FieldConfig.FORWARD_INDEX_DISABLED,
            FieldConfig.DEFAULT_FORWARD_INDEX_DISABLED));
    if (!forwardIndexDisabled) {
      return;
    }

    FieldSpec fieldSpec = schema.getFieldSpecFor(columnName);
    // Check for the range index since the index itself relies on the existence of the forward index to work.
    if (indexingConfig.getRangeIndexColumns() != null && indexingConfig.getRangeIndexColumns().contains(columnName)) {
      Preconditions.checkState(fieldSpec.isSingleValueField(), String.format("Feature not supported for multi-value "
          + "columns with range index. Cannot disable forward index for column %s. Disable range index on this "
          + "column to use this feature", columnName));
      Preconditions.checkState(indexingConfig.getRangeIndexVersion() == BitSlicedRangeIndexCreator.VERSION,
          String.format("Feature not supported for single-value columns with range index version < 2. Cannot disable "
              + "forward index for column %s. Either disable range index or create range index with"
              + " version >= 2 to use this feature", columnName));
    }

    Preconditions.checkState(!indexingConfig.isOptimizeDictionaryForMetrics() && !indexingConfig.isOptimizeDictionary(),
        String.format(
            "Dictionary override optimization options (OptimizeDictionary, optimizeDictionaryForMetrics)"
                + " not supported with forward index for column: %s, disabled", columnName));

    boolean hasDictionary = fieldConfig.getEncodingType() == EncodingType.DICTIONARY;
    boolean hasInvertedIndex =
        indexingConfig.getInvertedIndexColumns() != null && indexingConfig.getInvertedIndexColumns()
            .contains(columnName);

    if (!hasDictionary || !hasInvertedIndex) {
      LOGGER.warn("Forward index has been disabled for column {}. Either dictionary ({}) and / or inverted index ({}) "
              + "has been disabled. If the forward index needs to be regenerated or another index added please refresh "
              + "or back-fill the forward index as it cannot be rebuilt without dictionary and inverted index.",
          columnName,
          hasDictionary ? "enabled" : "disabled", hasInvertedIndex ? "enabled" : "disabled");
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
   * Ensure that the table config has the minimum number of replicas set as per cluster configs.
   */
  public static void ensureMinReplicas(TableConfig tableConfig, int defaultTableMinReplicas) {
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    int replication = tableConfig.getReplication();
    if (replication < defaultTableMinReplicas) {
      LOGGER.info("Creating table with minimum replication factor of: {} instead of requested replication: {}",
          defaultTableMinReplicas, replication);
      validationConfig.setReplicasPerPartition(null);
      validationConfig.setReplication(String.valueOf(defaultTableMinReplicas));
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
    TenantConfig offlineTenantConfig = offlineTableConfig.getTenantConfig();
    TenantConfig realtimeTenantConfig = realtimeTableConfig.getTenantConfig();
    String offlineBroker =
        offlineTenantConfig.getBroker() == null ? TagNameUtils.DEFAULT_TENANT_NAME : offlineTenantConfig.getBroker();
    String realtimeBroker =
        realtimeTenantConfig.getBroker() == null ? TagNameUtils.DEFAULT_TENANT_NAME : realtimeTenantConfig.getBroker();
    if (!offlineBroker.equals(realtimeBroker)) {
      throw new IllegalArgumentException(String.format(
          "Broker Tenants are different for table: %s! Offline broker tenant name: %s, Realtime broker tenant name: %s",
          rawTableName, offlineBroker, realtimeBroker));
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
        cleanFieldConfigList.add(new FieldConfig.Builder(fieldConfig).withIndexTypes(null).build());
      }
      clone.setFieldConfigList(cleanFieldConfigList);
    }
    return clone;
  }
}
