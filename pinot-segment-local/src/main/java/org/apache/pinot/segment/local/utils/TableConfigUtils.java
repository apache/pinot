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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregatorFactory;
import org.apache.pinot.segment.local.function.FunctionEvaluator;
import org.apache.pinot.segment.local.function.FunctionEvaluatorFactory;
import org.apache.pinot.segment.local.recordtransformer.SchemaConformingTransformer;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextMetadata;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.StarTreeAggregationConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.TimestampConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.config.table.ingestion.EnrichmentConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.SchemaConformingTransformerConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfig;
import org.apache.pinot.spi.recordtransformer.enricher.RecordEnricherRegistry;
import org.apache.pinot.spi.recordtransformer.enricher.RecordEnricherValidationConfig;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.AggregationFunctionType.DISTINCTCOUNTHLL;
import static org.apache.pinot.segment.spi.AggregationFunctionType.DISTINCTCOUNTHLLPLUS;
import static org.apache.pinot.segment.spi.AggregationFunctionType.SUMPRECISION;


/**
 * Utils related to table config operations
 */
public final class TableConfigUtils {
  private TableConfigUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TableConfigUtils.class);

  // supported TableTaskTypes, must be identical to the one return in the impl of {@link PinotTaskGenerator}.
  private static final String UPSERT_COMPACTION_TASK_TYPE = "UpsertCompactionTask";
  private static final String UPSERT_COMPACT_MERGE_TASK_TYPE = "UpsertCompactMergeTask";

  // this is duplicate with KinesisConfig.STREAM_TYPE, while instead of use KinesisConfig.STREAM_TYPE directly, we
  // hardcode the value here to avoid pulling the entire pinot-kinesis module as dependency.
  private static final String KINESIS_STREAM_TYPE = "kinesis";

  private static final Set<String> UPSERT_DEDUP_ALLOWED_ROUTING_STRATEGIES =
      ImmutableSet.of(RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE,
          RoutingConfig.MULTI_STAGE_REPLICA_GROUP_SELECTOR_TYPE);

  private static boolean _disableGroovy;

  private static boolean _enforcePoolBasedAssignment;

  public static void setDisableGroovy(boolean disableGroovy) {
    _disableGroovy = disableGroovy;
  }

  public static void setEnforcePoolBasedAssignment(boolean enforcePoolBasedAssignment) {
    _enforcePoolBasedAssignment = enforcePoolBasedAssignment;
  }

  /**
   * @see TableConfigUtils#validate(TableConfig, Schema, String)
   */
  public static void validate(TableConfig tableConfig, Schema schema) {
    validate(tableConfig, schema, null);
  }

  /**
   * Performs table config validations. Includes validations for the following:
   * 1. Validation config
   * 2. IngestionConfig
   * 3. TierConfigs
   * 4. Indexing config
   * 5. Field Config List
   * 6. Instance pool and replica group, if enabled
   *
   * TODO: Add more validations for each section (e.g. validate conditions are met for aggregateMetrics)
   */
  public static void validate(TableConfig tableConfig, Schema schema, @Nullable String typesToSkip) {
    Preconditions.checkArgument(schema != null, "Schema should not be null");
    Set<ValidationType> skipTypes = parseTypesToSkipString(typesToSkip);
    // Sanitize the table config before validation
    sanitize(tableConfig);

    if (skipTypes.contains(ValidationType.ALL)) {
      return;
    }

    validateValidationConfig(tableConfig, schema);
    validateIngestionConfig(tableConfig, schema);
    if (tableConfig.getTableType() == TableType.REALTIME) {
      validateStreamConfigMaps(tableConfig);
    }
    validateTierConfigList(tableConfig.getTierConfigsList());
    validateIndexingConfigAndFieldConfigList(tableConfig, schema);
    validateInstancePartitionsTypeMapConfig(tableConfig);
    validatePartitionedReplicaGroupInstance(tableConfig);
    validateInstanceAssignmentConfigs(tableConfig);

    if (!skipTypes.contains(ValidationType.UPSERT)) {
      validateUpsertAndDedupConfig(tableConfig, schema);
      validatePartialUpsertStrategies(tableConfig, schema);
    }

    if (_enforcePoolBasedAssignment) {
      validateInstancePoolsNReplicaGroups(tableConfig);
    }
  }

  /**
   * Validates the table config is using instance pool and replica group configuration.
   * @param tableConfig Table config to validate
   * @return true if the table config is using instance pool and replica group configuration, false otherwise
   */
  static boolean isTableUsingInstancePoolAndReplicaGroup(TableConfig tableConfig) {
    boolean status = true;
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = tableConfig.getInstanceAssignmentConfigMap();
    if (instanceAssignmentConfigMap != null) {
      for (InstanceAssignmentConfig instanceAssignmentConfig : instanceAssignmentConfigMap.values()) {
        if (instanceAssignmentConfig != null) {
          status &= (instanceAssignmentConfig.getTagPoolConfig().isPoolBased()
              && instanceAssignmentConfig.getReplicaGroupPartitionConfig().isReplicaGroupBased());
        } else {
          status = false;
        }
      }
    } else {
      status = false;
    }

    return status;
  }

  public static void validateInstancePoolsNReplicaGroups(TableConfig tableConfig) {
    Preconditions.checkState(isTableUsingInstancePoolAndReplicaGroup(tableConfig),
        "Instance pool and replica group configurations must be enabled");
  }

  private static Set<ValidationType> parseTypesToSkipString(@Nullable String typesToSkip) {
    return typesToSkip == null ? Collections.emptySet()
        : Arrays.stream(typesToSkip.split(",")).map(s -> ValidationType.valueOf(s.toUpperCase()))
            .collect(Collectors.toSet());
  }

  /**
   * Validates the table name with the following rules:
   * <ul>
   *   <li>Table name can have at most one dot in it.
   *   <li>Table name does not have whitespace.</li>
   * </ul>
   */
  public static void validateTableName(TableConfig tableConfig) {
    String tableName = tableConfig.getTableName();
    int dotCount = StringUtils.countMatches(tableName, '.');
    if (dotCount > 1) {
      throw new IllegalStateException("Table name: '" + tableName + "' containing more than one '.' is not allowed");
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
  private static void validateValidationConfig(TableConfig tableConfig, Schema schema) {
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    String timeColumnName = validationConfig.getTimeColumnName();
    if (tableConfig.getTableType() == TableType.REALTIME) {
      // For REALTIME table, must have a non-null timeColumnName
      Preconditions.checkState(timeColumnName != null, "'timeColumnName' cannot be null in REALTIME table config");
    }
    // timeColumnName can be null in OFFLINE table
    if (timeColumnName != null) {
      Preconditions.checkState(schema.getSpecForTimeColumn(timeColumnName) != null,
          "Cannot find valid fieldSpec for timeColumn: %s from the table config: %s, in the schema: %s", timeColumnName,
          tableConfig.getTableName(), schema.getSchemaName());
    }
    if (tableConfig.isDimTable()) {
      Preconditions.checkState(tableConfig.getTableType() == TableType.OFFLINE,
          "Dimension table must be of OFFLINE table type.");
      Preconditions.checkState(CollectionUtils.isNotEmpty(schema.getPrimaryKeyColumns()),
          "Dimension table must have primary key[s]");
    }

    String peerSegmentDownloadScheme = validationConfig.getPeerSegmentDownloadScheme();
    if (peerSegmentDownloadScheme != null) {
      if (!isValidPeerDownloadScheme(peerSegmentDownloadScheme)) {
        throw new IllegalStateException("Invalid value '" + peerSegmentDownloadScheme
            + "' for peerSegmentDownloadScheme. Must be one of http or https");
      }

      if (tableConfig.getReplication() < 2) {
        throw new IllegalStateException("peerSegmentDownloadScheme can't be used when replication is < 2");
      }
    }

    validateRetentionConfig(tableConfig);
  }

  private static boolean isValidPeerDownloadScheme(String peerSegmentDownloadScheme) {
    return CommonConstants.HTTP_PROTOCOL.equalsIgnoreCase(peerSegmentDownloadScheme)
        || CommonConstants.HTTPS_PROTOCOL.equalsIgnoreCase(peerSegmentDownloadScheme);
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
  public static void validateIngestionConfig(TableConfig tableConfig, Schema schema) {
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
        Preconditions.checkState(!streamConfigMaps.isEmpty(), "Must have at least 1 stream in REALTIME table");
        // TODO: for multiple stream configs, validate them

        boolean isPauselessEnabled = ingestionConfig.getStreamIngestionConfig().isPauselessConsumptionEnabled();
        if (isPauselessEnabled) {
          int replication = tableConfig.getReplication();
          // We are checking for this only when replication is greater than 1 because in test environments
          // users still prefer to create pauseless tables with replication 1
          if (replication > 1) {
            String peerSegmentDownloadScheme = tableConfig.getValidationConfig().getPeerSegmentDownloadScheme();
            Preconditions.checkState(StringUtils.isNotEmpty(peerSegmentDownloadScheme) && isValidPeerDownloadScheme(
                    peerSegmentDownloadScheme),
                "Must have a valid peerSegmentDownloadScheme set in validation config for pauseless consumption");
          } else {
            LOGGER.warn("It's not recommended to create pauseless tables with replication 1 for stability reasons.");
          }
        }
      }

      // Filter config
      FilterConfig filterConfig = ingestionConfig.getFilterConfig();
      if (filterConfig != null) {
        String filterFunction = filterConfig.getFilterFunction();
        if (filterFunction != null) {
          if (_disableGroovy && FunctionEvaluatorFactory.isGroovyExpression(filterFunction)) {
            throw new IllegalStateException(
                "Groovy filter functions are disabled for table config. Found '" + filterFunction + "'");
          }
          try {
            FunctionEvaluatorFactory.getExpressionEvaluator(filterFunction);
          } catch (Exception e) {
            throw new IllegalStateException(
                "Invalid filter function '" + filterFunction + "', exception: " + e.getMessage(), e);
          }
        }
      }

      // Aggregation configs
      List<AggregationConfig> aggregationConfigs = ingestionConfig.getAggregationConfigs();
      Set<String> aggregationSourceColumns = new HashSet<>();
      if (CollectionUtils.isNotEmpty(aggregationConfigs)) {
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

          FieldSpec fieldSpec = schema.getFieldSpecFor(columnName);
          Preconditions.checkState(fieldSpec != null,
              "The destination column '" + columnName + "' of the aggregation function must be present in the schema");
          Preconditions.checkState(fieldSpec.getFieldType() == FieldSpec.FieldType.METRIC,
              "The destination column '" + columnName + "' of the aggregation function must be a metric column");
          DataType dataType = fieldSpec.getDataType();

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
            Preconditions.checkState(dataType == DataType.BYTES, "Result type for DISTINCT_COUNT_HLL must be BYTES: %s",
                aggregationConfig);
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
            Preconditions.checkState(dataType == DataType.BYTES,
                "Result type for DISTINCT_COUNT_HLL_PLUS must be BYTES: %s", aggregationConfig);
          } else if (functionType == SUMPRECISION) {
            Preconditions.checkState(numArguments >= 2 && numArguments <= 3,
                "SUM_PRECISION must specify precision (required), scale (optional): %s", aggregationConfig);
            ExpressionContext secondArgument = arguments.get(1);
            Preconditions.checkState(secondArgument.getType() == ExpressionContext.Type.LITERAL,
                "Second argument of SUM_PRECISION must be literal: %s", aggregationConfig);
            String literal = secondArgument.getLiteral().getStringValue();
            Preconditions.checkState(StringUtils.isNumeric(literal),
                "Second argument of SUM_PRECISION must be a number: %s", aggregationConfig);
            Preconditions.checkState(dataType == DataType.BIG_DECIMAL || dataType == DataType.BYTES,
                "Result type for SUM_PRECISION must be BIG_DECIMAL or BYTES: %s", aggregationConfig);
          } else {
            Preconditions.checkState(numArguments == 1, "%s can only have one argument: %s", functionType,
                aggregationConfig);
          }
          ExpressionContext firstArgument = arguments.get(0);
          Preconditions.checkState(firstArgument.getType() == ExpressionContext.Type.IDENTIFIER,
              "First argument of aggregation function: %s must be identifier, got: %s", functionType,
              firstArgument.getType());
          // Create a ValueAggregator for the aggregation function and check if it is supported for ingestion (fixed
          // size aggregated value).
          ValueAggregator<?, ?> valueAggregator;
          try {
            valueAggregator =
                ValueAggregatorFactory.getValueAggregator(functionType, arguments.subList(1, numArguments));
          } catch (Exception e) {
            throw new IllegalStateException(
                "Caught exception while creating ValueAggregator for aggregation function: " + aggregationFunction, e);
          }
          Preconditions.checkState(valueAggregator.isAggregatedValueFixedSize(),
              "Aggregation function: %s must have fixed size aggregated value", aggregationFunction);

          aggregationSourceColumns.add(firstArgument.getIdentifier());
        }
        Preconditions.checkState(new HashSet<>(schema.getMetricNames()).equals(aggregationColumns),
            "all metric columns must be aggregated");

        // This is required by MutableSegmentImpl.enableMetricsAggregationIfPossible().
        // That code will disable ingestion aggregation if all metrics aren't noDictionaryColumns.
        // But if you do that after the table is already created, all future aggregations will
        // just be the default value.
        Map<String, DictionaryIndexConfig> configPerCol = StandardIndexes.dictionary().getConfig(tableConfig, schema);
        aggregationColumns.forEach(column -> {
          DictionaryIndexConfig dictConfig = configPerCol.get(column);
          Preconditions.checkState(dictConfig != null && dictConfig.isDisabled(),
              "Aggregated column: %s must be a no-dictionary column", column);
        });
      }

      // Enrichment configs
      List<EnrichmentConfig> enrichmentConfigs = ingestionConfig.getEnrichmentConfigs();
      if (enrichmentConfigs != null) {
        for (EnrichmentConfig enrichmentConfig : enrichmentConfigs) {
          RecordEnricherRegistry.validateEnrichmentConfig(enrichmentConfig,
              new RecordEnricherValidationConfig(_disableGroovy));
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
          Preconditions.checkState(schema.hasColumn(columnName) || aggregationSourceColumns.contains(columnName),
              "The destination column '" + columnName
                  + "' of the transform function must be present in the schema or as a source column for "
                  + "aggregations");
          FunctionEvaluator expressionEvaluator;
          if (_disableGroovy && FunctionEvaluatorFactory.isGroovyExpression(transformFunction)) {
            throw new IllegalStateException(
                "Groovy transform functions are disabled for table config. Found '" + transformFunction
                    + "' for column '" + columnName + "'");
          }
          try {
            expressionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(transformFunction);
          } catch (Exception e) {
            throw new IllegalStateException(
                "Invalid transform function '" + transformFunction + "' for column '" + columnName
                    + "', exception: " + e.getMessage(), e);
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
      if (complexTypeConfig != null) {
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
      if (schemaConformingTransformerConfig != null) {
        SchemaConformingTransformer.validateSchema(schema, schemaConformingTransformerConfig);
      }
    }
  }

  private static void validateStreamConfigMaps(TableConfig tableConfig) {
    List<Map<String, String>> streamConfigMaps = IngestionConfigUtils.getStreamConfigMaps(tableConfig);
    int numStreamConfigs = streamConfigMaps.size();
    List<StreamConfig> streamConfigs = new ArrayList<>(numStreamConfigs);
    for (Map<String, String> streamConfigMap : streamConfigMaps) {
      StreamConfig streamConfig;
      try {
        // Validate that StreamConfig can be created
        streamConfig = new StreamConfig(tableConfig.getTableName(), streamConfigMap);
      } catch (Exception e) {
        throw new IllegalStateException("Could not create StreamConfig using the streamConfig map", e);
      }
      validateStreamConfig(streamConfig);
      streamConfigs.add(streamConfig);
    }
    if (numStreamConfigs > 1) {
      Preconditions.checkState(!tableConfig.isUpsertEnabled(),
          "Multiple stream configs are not supported for upsert table");

      // Apply the following checks if there are multiple streamConfigs:
      // 1. Check if all streamConfigs have the same stream type.
      // 2. Ensure segment flush parameters consistent across all streamConfigs. We need this because Pinot is
      // predefining the values before fetching stream partition info from stream. At the construction time, we don't
      // know the value extracted from a streamConfig would be applied to which segment.
      // TODO: Remove these limitations
      StreamConfig firstStreamConfig = streamConfigs.get(0);
      String streamType = firstStreamConfig.getType();
      int flushThresholdRows = firstStreamConfig.getFlushThresholdRows();
      long flushThresholdTimeMillis = firstStreamConfig.getFlushThresholdTimeMillis();
      double flushThresholdVarianceFraction = firstStreamConfig.getFlushThresholdVarianceFraction();
      long flushThresholdSegmentSizeBytes = firstStreamConfig.getFlushThresholdSegmentSizeBytes();
      int flushThresholdSegmentRows = firstStreamConfig.getFlushThresholdSegmentRows();
      for (int i = 1; i < numStreamConfigs; i++) {
        StreamConfig streamConfig = streamConfigs.get(i);
        Preconditions.checkState(streamConfig.getType().equals(streamType),
            "All streamConfigs must have the same stream type");
        Preconditions.checkState(streamConfig.getFlushThresholdRows() == flushThresholdRows
                && streamConfig.getFlushThresholdTimeMillis() == flushThresholdTimeMillis
                && streamConfig.getFlushThresholdVarianceFraction() == flushThresholdVarianceFraction
                && streamConfig.getFlushThresholdSegmentSizeBytes() == flushThresholdSegmentSizeBytes
                && streamConfig.getFlushThresholdSegmentRows() == flushThresholdSegmentRows,
            "Segment flush parameters must be consistent across all streamConfigs");
      }
    }
  }

  @VisibleForTesting
  static void validateStreamConfig(StreamConfig streamConfig) {
    // Validate flush threshold
    Preconditions.checkState(streamConfig.getFlushThresholdTimeMillis() > 0, "Invalid flush threshold time: %s",
        streamConfig.getFlushThresholdTimeMillis());
    int flushThresholdRows = streamConfig.getFlushThresholdRows();
    int flushThresholdSegmentRows = streamConfig.getFlushThresholdSegmentRows();
    long flushThresholdSegmentSizeBytes = streamConfig.getFlushThresholdSegmentSizeBytes();
    int numFlushThresholdSet = 0;
    if (flushThresholdRows > 0) {
      numFlushThresholdSet++;
    }
    if (flushThresholdSegmentRows > 0) {
      numFlushThresholdSet++;
    }
    if (flushThresholdSegmentSizeBytes > 0) {
      numFlushThresholdSet++;
    }
    Preconditions.checkState(numFlushThresholdSet <= 1,
        "Only 1 of flush threshold (rows: %s, segment rows: %s, segment size: %s) can be set", flushThresholdRows,
        flushThresholdSegmentRows, flushThresholdSegmentSizeBytes);

    // Validate decoder
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
    // primary key columns are not of multi-value type
    for (String primaryKeyColumn : schema.getPrimaryKeyColumns()) {
      FieldSpec fieldSpec = schema.getFieldSpecFor(primaryKeyColumn);
      if (fieldSpec != null) {
        Preconditions.checkState(fieldSpec.isSingleValueField(),
            String.format("Upsert/Dedup primary key column: %s cannot be of multi-value type", primaryKeyColumn));
      }
    }
    // replica group is configured for routing
    Preconditions.checkState(
        tableConfig.getRoutingConfig() != null && isRoutingStrategyAllowedForUpsert(tableConfig.getRoutingConfig()),
        "Upsert/Dedup table must use strict replica-group (i.e. strictReplicaGroup) based routing");
    Preconditions.checkState(tableConfig.getTenantConfig().getTagOverrideConfig() == null || (
            tableConfig.getTenantConfig().getTagOverrideConfig().getRealtimeConsuming() == null
                && tableConfig.getTenantConfig().getTagOverrideConfig().getRealtimeCompleted() == null),
        "Invalid tenant tag override used for Upsert/Dedup table");

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
        Preconditions.checkState(fieldSpec != null,
            String.format("Column %s specified in deleteRecordColumn does not exist", deleteRecordColumn));
        Preconditions.checkState(fieldSpec.isSingleValueField(),
            String.format("The deleteRecordColumn - %s must be a single-valued column", deleteRecordColumn));
        DataType dataType = fieldSpec.getDataType();
        Preconditions.checkState(dataType == DataType.BOOLEAN || dataType == DataType.STRING || dataType.isNumeric(),
            String.format("The deleteRecordColumn - %s must be of type: String / Boolean / Numeric",
                deleteRecordColumn));
      }

      String outOfOrderRecordColumn = upsertConfig.getOutOfOrderRecordColumn();
      if (outOfOrderRecordColumn != null) {
        Preconditions.checkState(!Boolean.TRUE.equals(upsertConfig.isDropOutOfOrderRecord()),
            "outOfOrderRecordColumn and dropOutOfOrderRecord shouldn't exist together for upsert table");
        FieldSpec fieldSpec = schema.getFieldSpecFor(outOfOrderRecordColumn);
        Preconditions.checkState(
            fieldSpec != null && fieldSpec.isSingleValueField() && fieldSpec.getDataType() == DataType.BOOLEAN,
            "The outOfOrderRecordColumn must be a single-valued BOOLEAN column");
      }

      if (upsertConfig.isEnableDeletedKeysCompactionConsistency()) {
        // enableDeletedKeysCompactionConsistency shouldn't exist with metadataTTL
        Preconditions.checkState(upsertConfig.getMetadataTTL() == 0,
            "enableDeletedKeysCompactionConsistency and metadataTTL shouldn't exist together for upsert table");

        // enableDeletedKeysCompactionConsistency shouldn't exist with preload enabled
        Preconditions.checkState(upsertConfig.getPreload() != Enablement.ENABLE,
            "enableDeletedKeysCompactionConsistency and preload shouldn't exist together for upsert table");

        // enableDeletedKeysCompactionConsistency should exist with deletedKeysTTL
        Preconditions.checkState(upsertConfig.getDeletedKeysTTL() > 0,
            "enableDeletedKeysCompactionConsistency should exist with deletedKeysTTL for upsert table");

        // enableDeletedKeysCompactionConsistency should exist with snapshot enabled
        // NOTE: Allow snapshot to be DEFAULT because it might be enabled at server level.
        Preconditions.checkState(upsertConfig.getSnapshot() != Enablement.DISABLE,
            "enableDeletedKeysCompactionConsistency should exist with snapshot for upsert table");

        // enableDeletedKeysCompactionConsistency should exist with UpsertCompactionTask / UpsertCompactMergeTask
        TableTaskConfig taskConfig = tableConfig.getTaskConfig();
        Preconditions.checkState(taskConfig != null
                && (taskConfig.getTaskTypeConfigsMap().containsKey(UPSERT_COMPACTION_TASK_TYPE)
                || taskConfig.getTaskTypeConfigsMap().containsKey(UPSERT_COMPACT_MERGE_TASK_TYPE)),
            "enableDeletedKeysCompactionConsistency should exist with UpsertCompactionTask"
                + " / UpsertCompactMergeTask for upsert table");
      }

      if (upsertConfig.getConsistencyMode() != UpsertConfig.ConsistencyMode.NONE) {
        Preconditions.checkState(upsertConfig.getNewSegmentTrackingTimeMs() > 0,
            "Positive newSegmentTrackingTimeMs is required to enable consistency mode: "
                + upsertConfig.getConsistencyMode());
      }
    }

    Preconditions.checkState(
        tableConfig.getInstanceAssignmentConfigMap() == null || !tableConfig.getInstanceAssignmentConfigMap()
            .containsKey(InstancePartitionsType.COMPLETED),
        "InstanceAssignmentConfig for COMPLETED is not allowed for upsert tables");
    validateAggregateMetricsForUpsertConfig(tableConfig);
    validateTTLForUpsertConfig(tableConfig, schema);
    validateTTLForDedupConfig(tableConfig, schema);
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
          "MetadataTTL / DeletedKeysTTL must have comparison column: %s in numeric type, found: %s", comparisonColumn,
          comparisonColumnDataType);
    } else {
      String comparisonColumn = tableConfig.getValidationConfig().getTimeColumnName();
      DataType comparisonColumnDataType = schema.getFieldSpecFor(comparisonColumn).getDataType();
      Preconditions.checkState(comparisonColumnDataType.isNumeric(),
          "MetadataTTL / DeletedKeysTTL must have time column: %s in numeric type, found: %s", comparisonColumn,
          comparisonColumnDataType);
    }

    if (upsertConfig.getMetadataTTL() > 0) {
      // NOTE: Allow snapshot to be DEFAULT because it might be enabled at server level.
      Preconditions.checkState(upsertConfig.getSnapshot() != Enablement.DISABLE,
          "Upsert TTL must have snapshot enabled");
    }

    if (upsertConfig.getDeletedKeysTTL() > 0) {
      Preconditions.checkState(upsertConfig.getDeleteRecordColumn() != null,
          "Deleted Keys TTL can only be enabled with deleteRecordColumn set.");
    }
  }

  /**
   * Validates the dedup config related to TTL.
   */
  @VisibleForTesting
  static void validateTTLForDedupConfig(TableConfig tableConfig, Schema schema) {
    DedupConfig dedupConfig = tableConfig.getDedupConfig();
    if (dedupConfig == null || (dedupConfig.getMetadataTTL() <= 0)) {
      return;
    }

    String dedupTimeColumn = dedupConfig.getDedupTimeColumn();
    if (dedupTimeColumn != null && !dedupTimeColumn.isEmpty()) {
      DataType comparisonColumnDataType = schema.getFieldSpecFor(dedupTimeColumn).getDataType();
      Preconditions.checkState(comparisonColumnDataType.isNumeric(),
          "MetadataTTL must have dedupTimeColumn: %s in numeric type, found: %s", dedupTimeColumn,
          comparisonColumnDataType);
    } else {
      String timeColumn = tableConfig.getValidationConfig().getTimeColumnName();
      DataType timeColumnDataType = schema.getFieldSpecFor(timeColumn).getDataType();
      Preconditions.checkState(timeColumnDataType.isNumeric(),
          "MetadataTTL must have time column: %s in numeric type, found: %s", timeColumn, timeColumnDataType);
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
          Preconditions.checkState(tableConfig.getInstancePartitionsMap().containsKey(instancePartitionsType),
              String.format("Both InstanceAssignmentConfigMap and InstancePartitionsMap needed for %s, as "
                  + "MIRROR_SERVER_SET_PARTITION_SELECTOR is used", instancePartitionsType));
        } else {
          Preconditions.checkState(!tableConfig.getInstancePartitionsMap().containsKey(instancePartitionsType),
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

  @VisibleForTesting
  static void validateInstanceAssignmentConfigs(TableConfig tableConfig) {
    if (tableConfig.getInstanceAssignmentConfigMap() == null) {
      return;
    }
    for (Map.Entry<String, InstanceAssignmentConfig> instanceAssignmentConfigMapEntry
        : tableConfig.getInstanceAssignmentConfigMap().entrySet()) {
      String instancePartitionsType = instanceAssignmentConfigMapEntry.getKey();
      InstanceAssignmentConfig instanceAssignmentConfig = instanceAssignmentConfigMapEntry.getValue();
      if (instanceAssignmentConfig.getPartitionSelector()
          == InstanceAssignmentConfig.PartitionSelector.IMPLICIT_REALTIME_TABLE_PARTITION_SELECTOR) {
        Preconditions.checkState(tableConfig.getTableType() == TableType.REALTIME,
            "IMPLICIT_REALTIME_TABLE_PARTITION_SELECTOR can only be used for REALTIME tables");
        Preconditions.checkState(InstancePartitionsType.CONSUMING.name().equalsIgnoreCase(instancePartitionsType),
            "IMPLICIT_REALTIME_TABLE_PARTITION_SELECTOR can only be used for CONSUMING instance partitions type");
        Preconditions.checkState(instanceAssignmentConfig.getReplicaGroupPartitionConfig().isReplicaGroupBased(),
            "IMPLICIT_REALTIME_TABLE_PARTITION_SELECTOR can only be used with replica group based instance assignment");
        Preconditions.checkState(instanceAssignmentConfig.getReplicaGroupPartitionConfig().getNumPartitions() == 0,
            "numPartitions should not be explicitly set when using IMPLICIT_REALTIME_TABLE_PARTITION_SELECTOR");
        // Allow 0 because that's the default (unset) value.
        Preconditions.checkState(
            instanceAssignmentConfig.getReplicaGroupPartitionConfig().getNumInstancesPerPartition() == 0
                || instanceAssignmentConfig.getReplicaGroupPartitionConfig().getNumInstancesPerPartition() == 1,
            "numInstancesPerPartition must be 1 when using IMPLICIT_REALTIME_TABLE_PARTITION_SELECTOR");
      }
      // TODO: Add more validations for other partition selectors here
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

    Preconditions.checkState(
        schema.isEnableColumnBasedNullHandling() || tableConfig.getIndexingConfig().isNullHandlingEnabled(),
        "Null handling must be enabled for partial upsert tables");

    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    assert upsertConfig != null;
    Map<String, UpsertConfig.Strategy> partialUpsertStrategies = upsertConfig.getPartialUpsertStrategies();
    String partialUpsertMergerClass = upsertConfig.getPartialUpsertMergerClass();

    // check if partialUpsertMergerClass is provided then partialUpsertStrategies should be empty
    if (StringUtils.isNotBlank(partialUpsertMergerClass)) {
      Preconditions.checkState(MapUtils.isEmpty(partialUpsertStrategies),
          "If partialUpsertMergerClass is provided then partialUpsertStrategies should be empty");
    } else if (MapUtils.isNotEmpty(partialUpsertStrategies)) {
      List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
      // validate partial upsert column mergers
      for (Map.Entry<String, UpsertConfig.Strategy> entry : partialUpsertStrategies.entrySet()) {
        String column = entry.getKey();
        UpsertConfig.Strategy columnStrategy = entry.getValue();
        Preconditions.checkState(!primaryKeyColumns.contains(column),
            "Merger cannot be applied to primary key columns");

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
          Preconditions.checkState(!schema.getComplexNames().contains(column),
              "INCREMENT merger cannot be applied to complex column: %s", column);
        } else if (columnStrategy == UpsertConfig.Strategy.APPEND || columnStrategy == UpsertConfig.Strategy.UNION) {
          Preconditions.checkState(!fieldSpec.isSingleValueField(),
              "%s merger cannot be applied to single-value column: %s", columnStrategy.toString(), column);
        }
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

  /// Validates [IndexingConfig] and [FieldConfig]s, ensures that:
  /// - No conflicting index configs
  /// - All referenced columns exist in the schema
  /// - Proper dependency between index types (e.g. inverted index columns must have dictionary)
  private static void validateIndexingConfigAndFieldConfigList(TableConfig tableConfig, Schema schema) {
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    List<FieldConfig> fieldConfigs = tableConfig.getFieldConfigList();
    if (CollectionUtils.isNotEmpty(fieldConfigs)) {
      for (FieldConfig fieldConfig : fieldConfigs) {
        String column = fieldConfig.getName();
        Preconditions.checkState(schema.hasColumn(column), "Failed to find column: %s in schema", column);
      }
      validateIndexingConfigAndFieldConfigListCompatibility(indexingConfig, fieldConfigs);
    }

    Map<String, FieldIndexConfigs> indexConfigsMap;
    try {
      indexConfigsMap = FieldIndexConfigsUtil.createIndexConfigsByColName(tableConfig, schema);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create FieldIndexConfigs", e);
    }
    List<IndexType<?, ?, ?>> allIndexes = IndexService.getInstance().getAllIndexes();
    for (Map.Entry<String, FieldIndexConfigs> entry : indexConfigsMap.entrySet()) {
      String column = entry.getKey();
      FieldSpec fieldSpec = schema.getFieldSpecFor(column);
      Preconditions.checkState(fieldSpec != null, "Failed to find column: %s in schema", column);
      FieldIndexConfigs indexConfigs = entry.getValue();
      for (IndexType<?, ?, ?> indexType : allIndexes) {
        indexType.validate(indexConfigs, fieldSpec, tableConfig);
      }
    }

    validateMultiColumnTextIndex(indexingConfig.getMultiColumnTextIndexConfig());

    // Star-tree index config is not managed by FieldIndexConfigs, and we need to validate it separately.
    List<StarTreeIndexConfig> starTreeIndexConfigs = indexingConfig.getStarTreeIndexConfigs();
    if (CollectionUtils.isNotEmpty(starTreeIndexConfigs)) {
      validateStarTreeIndexConfigs(starTreeIndexConfigs, indexConfigsMap, schema);
    }

    // TIMESTAMP index is not managed by FieldIndexConfigs, and we need to validate it separately.
    if (CollectionUtils.isNotEmpty(fieldConfigs)) {
      for (FieldConfig fieldConfig : fieldConfigs) {
        TimestampConfig timestampConfig = fieldConfig.getTimestampConfig();
        if (timestampConfig != null) {
          String column = fieldConfig.getName();
          FieldSpec fieldSpec = schema.getFieldSpecFor(column);
          Preconditions.checkState(fieldSpec != null, "Failed to find column: %s in schema", column);
          Preconditions.checkState(fieldSpec.getDataType().getStoredType() == DataType.LONG,
              "Cannot create TIMESTAMP index on column: %s of stored type other than LONG", column);
          Preconditions.checkState(fieldSpec.isSingleValueField(),
              "Cannot create TIMESTAMP index on multi-value column: %s", column);
        }
      }
    }

    // Sorted column is not managed by FieldIndexConfigs, and we need to validate it separately.
    List<String> sortedColumns = indexingConfig.getSortedColumn();
    if (CollectionUtils.isNotEmpty(sortedColumns)) {
      for (String column : sortedColumns) {
        FieldSpec fieldSpec = schema.getFieldSpecFor(column);
        Preconditions.checkState(fieldSpec != null, "Failed to find sorted column: %s in schema", column);
        Preconditions.checkState(fieldSpec.isSingleValueField(), "Cannot sort on multi-value column: %s", column);
      }
    }

    // Partition column is not managed by FieldIndexConfigs, and we need to validate it separately.
    SegmentPartitionConfig segmentPartitionConfig = indexingConfig.getSegmentPartitionConfig();
    if (segmentPartitionConfig != null) {
      Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
      if (MapUtils.isNotEmpty(columnPartitionMap)) {
        for (String column : columnPartitionMap.keySet()) {
          FieldSpec fieldSpec = schema.getFieldSpecFor(column);
          Preconditions.checkState(fieldSpec != null, "Failed to find partition column: %s in schema", column);
          Preconditions.checkState(fieldSpec.isSingleValueField(), "Cannot partition on multi-value column: %s",
              column);
        }
      }
    }
  }

  private static void validateMultiColumnTextIndex(MultiColumnTextIndexConfig multiColTextIndex) {
    if (multiColTextIndex == null) {
      return;
    }

    Preconditions.checkState(multiColTextIndex.getColumns() != null && !multiColTextIndex.getColumns().isEmpty(),
        "Multi-column text index's list of columns can't be empty");

    checkForDuplicates(multiColTextIndex.getColumns());

    if (multiColTextIndex.getProperties() != null) {
      for (String key : multiColTextIndex.getProperties().keySet()) {
        Preconditions.checkState(MultiColumnTextMetadata.isValidSharedProperty(key),
            "Multi-column text index doesn't allow: %s as shared property", key);
      }
    }

    if (multiColTextIndex.getPerColumnProperties() != null) {
      for (String column : multiColTextIndex.getPerColumnProperties().keySet()) {
        Preconditions.checkState(multiColTextIndex.getColumns().contains(column),
            "Multi-column text index per-column property refers to unknown column: %s", column);

        for (String key : multiColTextIndex.getPerColumnProperties().get(column).keySet()) {
          Preconditions.checkState(MultiColumnTextMetadata.isValidPerColumnProperty(key),
              "Multi-column text index doesn't allow: %s as property for column: %s", key, column);
        }
      }
    }
  }

  /// Validates compatibility across [IndexingConfig] and [FieldConfig]s, ensures that:
  /// - Columns with DICTIONARY encoding type in [FieldConfig]s are not defined as no-dictionary in [IndexingConfig]
  private static void validateIndexingConfigAndFieldConfigListCompatibility(IndexingConfig indexingConfig,
      List<FieldConfig> fieldConfigs) {
    Set<String> noDictionaryColumnsFromIndexingConfig = new HashSet<>();
    if (indexingConfig.getNoDictionaryColumns() != null) {
      noDictionaryColumnsFromIndexingConfig.addAll(indexingConfig.getNoDictionaryColumns());
    }
    if (indexingConfig.getNoDictionaryConfig() != null) {
      noDictionaryColumnsFromIndexingConfig.addAll(indexingConfig.getNoDictionaryConfig().keySet());
    }
    if (!noDictionaryColumnsFromIndexingConfig.isEmpty()) {
      for (FieldConfig fieldConfig : fieldConfigs) {
        String column = fieldConfig.getName();
        EncodingType encodingType = fieldConfig.getEncodingType();
        if (encodingType == EncodingType.DICTIONARY) {
          Preconditions.checkState(!noDictionaryColumnsFromIndexingConfig.contains(column),
              "FieldConfig encoding type is different from indexingConfig for column: %s", column);
        }
      }
    }
  }

  /// Validates [StarTreeIndexConfig]s, ensures that:
  /// - Dimensions must be dictionary encoded
  /// - 'dimensionsSplitOrder' contains all dimensions in 'skipStarNodeCreationForDimensions'
  /// - Either functionColumnPairs or aggregationConfigs must be specified, but not both
  /// - All referenced columns exist in the schema and are single-valued
  private static void validateStarTreeIndexConfigs(List<StarTreeIndexConfig> starTreeIndexConfigs,
      Map<String, FieldIndexConfigs> indexConfigsMap, Schema schema) {
    Set<String> referencedColumns = new HashSet<>();
    for (StarTreeIndexConfig starTreeIndexConfig : starTreeIndexConfigs) {
      // Validate dimension columns are dictionary encoded
      List<String> dimensionsSplitOrder = starTreeIndexConfig.getDimensionsSplitOrder();
      assert CollectionUtils.isNotEmpty(dimensionsSplitOrder);
      for (String dimension : dimensionsSplitOrder) {
        FieldIndexConfigs indexConfigs = indexConfigsMap.get(dimension);
        Preconditions.checkState(indexConfigs != null,
            "Failed to find dimension column: %s specified in star-tree index config in schema", dimension);
        Preconditions.checkState(indexConfigs.getConfig(StandardIndexes.dictionary()).isEnabled(),
            "Cannot create star-tree index on dimension column: %s without dictionary", dimension);
        referencedColumns.add(dimension);
      }

      // Validate 'dimensionsSplitOrder' contains all dimensions in 'skipStarNodeCreationForDimensions'
      List<String> skipStarNodeCreationForDimensions = starTreeIndexConfig.getSkipStarNodeCreationForDimensions();
      if (CollectionUtils.isNotEmpty(skipStarNodeCreationForDimensions)) {
        //noinspection SlowListContainsAll
        Preconditions.checkState(dimensionsSplitOrder.containsAll(skipStarNodeCreationForDimensions),
            "Can not skip star-node creation for dimensions not in the split order, dimensionsSplitOrder: %s, "
                + "skipStarNodeCreationForDimensions: %s", dimensionsSplitOrder, skipStarNodeCreationForDimensions);
      }

      List<String> functionColumnPairs = starTreeIndexConfig.getFunctionColumnPairs();
      List<StarTreeAggregationConfig> aggregationConfigs = starTreeIndexConfig.getAggregationConfigs();
      Preconditions.checkState(
          (functionColumnPairs != null && aggregationConfigs == null) || (functionColumnPairs == null
              && aggregationConfigs != null),
          "Either 'functionColumnPairs' or 'aggregationConfigs' must be specified, but not both");
      Set<AggregationFunctionColumnPair> functionColumnPairsSet = new HashSet<>();
      Set<AggregationFunctionColumnPair> storedTypes = new HashSet<>();
      if (functionColumnPairs != null) {
        for (String functionColumnPair : functionColumnPairs) {
          AggregationFunctionColumnPair columnPair;
          try {
            columnPair = AggregationFunctionColumnPair.fromColumnName(functionColumnPair);
          } catch (Exception e) {
            throw new IllegalStateException("Invalid StarTreeIndex config: " + functionColumnPair + ". Must be"
                + "in the form <Aggregation function>__<Column name>");
          }

          if (!functionColumnPairsSet.add(columnPair)) {
            throw new IllegalStateException("Duplicate function column pair: " + functionColumnPair);
          }

          AggregationFunctionColumnPair storedType = AggregationFunctionColumnPair.resolveToStoredType(columnPair);
          if (!storedTypes.add(storedType)) {
            LOGGER.warn("StarTreeIndex config duplication: {} already matches existing function column pair: {}. ",
                columnPair, storedType);
          }
          String column = columnPair.getColumn();
          if (!column.equals(AggregationFunctionColumnPair.STAR)) {
            referencedColumns.add(column);
          }
        }
      }
      if (aggregationConfigs != null) {
        for (StarTreeAggregationConfig aggregationConfig : aggregationConfigs) {
          AggregationFunctionColumnPair columnPair;
          try {
            columnPair = AggregationFunctionColumnPair.fromAggregationConfig(aggregationConfig);
          } catch (Exception e) {
            throw new IllegalStateException("Invalid StarTreeIndex config: " + aggregationConfig);
          }

          if (!functionColumnPairsSet.add(columnPair)) {
            throw new IllegalStateException("Duplicate function column pair: " + columnPair + ". If you want multiple"
                + " pre-aggregations on the same column with the same aggregation function but with different"
                + " configuration parameters, specify them in separate star-tree index configurations");
          }

          AggregationFunctionColumnPair storedType = AggregationFunctionColumnPair.resolveToStoredType(columnPair);
          if (!storedTypes.add(storedType)) {
            LOGGER.warn("StarTreeIndex config duplication: {} already matches existing function column pair: {}. ",
                columnPair, storedType);
          }
          String column = columnPair.getColumn();
          if (!column.equals(AggregationFunctionColumnPair.STAR)) {
            referencedColumns.add(column);
          }
        }
      }

      // Validate all referenced columns exist in the schema and are single-valued
      for (String column : referencedColumns) {
        FieldSpec fieldSpec = schema.getFieldSpecFor(column);
        Preconditions.checkState(fieldSpec != null,
            "Failed to find column: %s specified in star-tree index config in schema", column);
        Preconditions.checkState(fieldSpec.isSingleValueField(),
            "Star-tree index can only be created on single-value columns, but found multi-value column: %s", column);
        Preconditions.checkState(fieldSpec.getDataType() != DataType.MAP,
            "Star-tree index cannot be created on MAP column: %s", column);
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
                String.format("Exceeded storage size for dimension table. Requested size: %d, Max allowed size: %d",
                    quotaConfig.getStorageInBytes(), maxAllowedSizeInBytes));
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

  public static void checkForDuplicates(List<String> columns) {
    Set<String> seen = new HashSet<>(columns.size());
    Set<String> duplicates = new HashSet<>(columns.size());

    for (String item : columns) {
      if (!seen.add(item)) {
        duplicates.add(item);
      }
    }

    if (!duplicates.isEmpty()) {
      throw new IllegalStateException(
          "Cannot create TEXT index on duplicate columns: " + duplicates);
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

  private static boolean isRoutingStrategyAllowedForUpsert(RoutingConfig routingConfig) {
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
            .withIndexTypes(null).withProperties(null).build());
      }
      clone.setFieldConfigList(cleanFieldConfigList);
    }
    return clone;
  }

  /**
   * Helper method to convert from legacy/deprecated configs into current version of TableConfig.
   * <ul>
   *   <li>Moves deprecated ingestion related configs into Ingestion Config.</li>
   *   <li>The conversion happens in-place, the specified tableConfig is mutated in-place.</li>
   * </ul>
   *
   * @param tableConfig Input table config.
   */
  @SuppressWarnings("deprecation")
  public static void convertFromLegacyTableConfig(TableConfig tableConfig) {
    // It is possible that indexing as well as ingestion configs exist, in which case we always honor ingestion config.
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    BatchIngestionConfig batchIngestionConfig =
        (ingestionConfig != null) ? ingestionConfig.getBatchIngestionConfig() : null;

    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    String segmentPushType = validationConfig.getSegmentPushType();
    String segmentPushFrequency = validationConfig.getSegmentPushFrequency();

    if (batchIngestionConfig == null) {
      // Only create the config if any of the deprecated config is not null.
      if (segmentPushType != null || segmentPushFrequency != null) {
        batchIngestionConfig = new BatchIngestionConfig(null, segmentPushType, segmentPushFrequency);
      }
    } else {
      // This should not happen typically, but since we are in repair mode, might as well cover this corner case.
      if (batchIngestionConfig.getSegmentIngestionType() == null) {
        batchIngestionConfig.setSegmentIngestionType(segmentPushType);
      }
      if (batchIngestionConfig.getSegmentIngestionFrequency() == null) {
        batchIngestionConfig.setSegmentIngestionFrequency(segmentPushFrequency);
      }
    }

    StreamIngestionConfig streamIngestionConfig =
        (ingestionConfig != null) ? ingestionConfig.getStreamIngestionConfig() : null;
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();

    if (streamIngestionConfig == null) {
      // Only set the new config if the deprecated one is set.
      Map<String, String> streamConfigs = indexingConfig.getStreamConfigs();
      if (MapUtils.isNotEmpty(streamConfigs)) {
        streamIngestionConfig = new StreamIngestionConfig(Collections.singletonList(streamConfigs));
      }
    }

    if (ingestionConfig == null) {
      if (batchIngestionConfig != null || streamIngestionConfig != null) {
        ingestionConfig = new IngestionConfig();
        ingestionConfig.setBatchIngestionConfig(batchIngestionConfig);
        ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
      }
    } else {
      ingestionConfig.setBatchIngestionConfig(batchIngestionConfig);
      ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
    }

    // Set the new config fields.
    tableConfig.setIngestionConfig(ingestionConfig);

    // Clear the deprecated ones.
    indexingConfig.setStreamConfigs(null);
    validationConfig.setSegmentPushFrequency(null);
    validationConfig.setSegmentPushType(null);
  }

  /**
   * Helper method to create a new TableConfig by overwriting the original TableConfig with tier specific configs, so
   * that the consumers of TableConfig don't have to handle tier overwrites themselves. To begin with, we only
   * consider to overwrite the index configs in `tableIndexConfig` and `fieldConfigList`, e.g.
   *
   * {
   *   "tableIndexConfig": {
   *     ... // configs allowed in IndexingConfig, for default tier
   *     "tierOverwrites": {
   *       "hotTier": {...}, // configs allowed in IndexingConfig, for hot tier
   *       "coldTier": {...} // configs allowed in IndexingConfig, for cold tier
   *     }
   *   }
   *   "fieldConfigList": [
   *     {
   *       ... // configs allowed in FieldConfig, for default tier
   *       "tierOverwrites": {
   *         "hotTier": {...}, // configs allowed in FieldConfig, for hot tier
   *         "coldTier": {...} // configs allowed in FieldConfig, for cold tier
   *       }
   *     },
   *     ...
   *   ]
   * }
   *
   * Overwriting is to extract tier specific configs from those `tierOverwrites` sections and replace the
   * corresponding configs set for default tier.
   *
   * TODO: Other tier specific configs like segment assignment policy may be handled in this helper method too, to
   *       keep tier overwrites transparent to consumers of TableConfig.
   *
   * @param tableConfig the input table config which is kept intact
   * @param tier        the target tier to overwrite the table config
   * @return a new table config overwritten for the tier, or the original table if overwriting doesn't happen.
   */
  public static TableConfig overwriteTableConfigForTier(TableConfig tableConfig, @Nullable String tier) {
    if (tier == null) {
      return tableConfig;
    }
    try {
      boolean updated = false;
      JsonNode tblCfgJson = tableConfig.toJsonNode();
      // Apply tier specific overwrites for `tableIndexConfig`
      JsonNode tblIdxCfgJson = tblCfgJson.get(TableConfig.INDEXING_CONFIG_KEY);
      if (tblIdxCfgJson != null && tblIdxCfgJson.has(TableConfig.TIER_OVERWRITES_KEY)) {
        JsonNode tierCfgJson = tblIdxCfgJson.get(TableConfig.TIER_OVERWRITES_KEY).get(tier);
        if (tierCfgJson != null) {
          LOGGER.debug("Got table index config overwrites: {} for tier: {}", tierCfgJson, tier);
          overwriteConfig(tblIdxCfgJson, tierCfgJson);
          updated = true;
        }
      }
      // Apply tier specific overwrites for `fieldConfigList`
      JsonNode fieldCfgListJson = tblCfgJson.get(TableConfig.FIELD_CONFIG_LIST_KEY);
      if (fieldCfgListJson != null && fieldCfgListJson.isArray()) {
        Iterator<JsonNode> fieldCfgListItr = fieldCfgListJson.elements();
        while (fieldCfgListItr.hasNext()) {
          JsonNode fieldCfgJson = fieldCfgListItr.next();
          if (!fieldCfgJson.has(TableConfig.TIER_OVERWRITES_KEY)) {
            continue;
          }
          JsonNode tierCfgJson = fieldCfgJson.get(TableConfig.TIER_OVERWRITES_KEY).get(tier);
          if (tierCfgJson != null) {
            LOGGER.debug("Got field index config overwrites: {} for tier: {}", tierCfgJson, tier);
            overwriteConfig(fieldCfgJson, tierCfgJson);
            updated = true;
          }
        }
      }
      if (updated) {
        LOGGER.debug("Got overwritten table config: {} for tier: {}", tblCfgJson, tier);
        return JsonUtils.jsonNodeToObject(tblCfgJson, TableConfig.class);
      } else {
        LOGGER.debug("No table config overwrites for tier: {}", tier);
        return tableConfig;
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to overwrite table config for tier: {} for table: {}", tier, tableConfig.getTableName(), e);
      return tableConfig;
    }
  }

  private static void overwriteConfig(JsonNode oldCfg, JsonNode newCfg) {
    Iterator<Map.Entry<String, JsonNode>> cfgItr = newCfg.fields();
    while (cfgItr.hasNext()) {
      Map.Entry<String, JsonNode> cfgEntry = cfgItr.next();
      ((ObjectNode) oldCfg).set(cfgEntry.getKey(), cfgEntry.getValue());
    }
  }

  /**
   * Get the partition column from tableConfig instance assignment config map.
   * @param tableConfig table config
   * @return partition column
   */
  public static String getPartitionColumn(TableConfig tableConfig) {
    // check InstanceAssignmentConfigMap is null or empty,
    if (!MapUtils.isEmpty(tableConfig.getInstanceAssignmentConfigMap())) {
      for (InstanceAssignmentConfig instanceAssignmentConfig : tableConfig.getInstanceAssignmentConfigMap().values()) {
        //check InstanceAssignmentConfig has the InstanceReplicaGroupPartitionConfig with non-empty partitionColumn
        if (StringUtils.isNotEmpty(instanceAssignmentConfig.getReplicaGroupPartitionConfig().getPartitionColumn())) {
          return instanceAssignmentConfig.getReplicaGroupPartitionConfig().getPartitionColumn();
        }
      }
    }

    // for backward-compatibility, If partitionColumn value isn't there in InstanceReplicaGroupPartitionConfig
    // check ReplicaGroupStrategyConfig for partitionColumn
    //noinspection deprecation
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    return replicaGroupStrategyConfig != null ? replicaGroupStrategyConfig.getPartitionColumn() : null;
  }

  public static Set<String> getRelevantTags(TableConfig tableConfig) {
    Set<String> relevantTags = new HashSet<>();
    String serverTenantName = tableConfig.getTenantConfig().getServer();
    if (serverTenantName != null) {
      String serverTenantTag =
          TagNameUtils.getServerTagForTenant(serverTenantName, tableConfig.getTableType());
      relevantTags.add(serverTenantTag);
    }
    TagOverrideConfig tagOverrideConfig = tableConfig.getTenantConfig().getTagOverrideConfig();
    if (tagOverrideConfig != null) {
      String completedTag = tagOverrideConfig.getRealtimeCompleted();
      String consumingTag = tagOverrideConfig.getRealtimeConsuming();
      if (completedTag != null) {
        relevantTags.add(completedTag);
      }
      if (consumingTag != null) {
        relevantTags.add(consumingTag);
      }
    }
    if (tableConfig.getInstanceAssignmentConfigMap() != null) {
      // for simplicity, including all segment types present in instanceAssignmentConfigMap
      tableConfig.getInstanceAssignmentConfigMap().values().forEach(instanceAssignmentConfig -> {
        String tag = instanceAssignmentConfig.getTagPoolConfig().getTag();
        relevantTags.add(tag);
      });
    }
    if (tableConfig.getTierConfigsList() != null) {
      tableConfig.getTierConfigsList().forEach(tierConfig -> {
        String tierTag = tierConfig.getServerTag();
        relevantTags.add(tierTag);
      });
    }
    return relevantTags;
  }

  public static boolean isRelevantToTenant(TableConfig tableConfig, String tenantName) {
    Set<String> relevantTenants =
        getRelevantTags(tableConfig).stream().map(TagNameUtils::getTenantFromTag).collect(
            Collectors.toSet());
    return relevantTenants.contains(tenantName);
  }
}
