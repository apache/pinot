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
package org.apache.pinot.segment.local.recordtransformer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.config.table.ingestion.EnrichmentConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.SourceFieldConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.apache.pinot.spi.recordtransformer.enricher.RecordEnricher;
import org.apache.pinot.spi.recordtransformer.enricher.RecordEnricherRegistry;
import org.apache.pinot.spi.utils.PinotDataType;


public class RecordTransformerUtils {
  private RecordTransformerUtils() {
  }

  /// Returns a list of [RecordTransformer]s based on the given [TableConfig] and [Schema].
  /// DO NOT CHANGE THE ORDER OF THE RECORD TRANSFORMERS.
  /// The transformers returned are:
  /// - (Optional) [DataTypeTransformer] to fix the data types of the source fields configured with
  /// `preComplexTypeTransform = true` in `IngestionConfig#getSourceFieldConfigs()`. It precedes the pre-complex-type
  /// [RecordEnricher]s and [ComplexTypeTransformer] so that they consume the source fields with the corrected types.
  /// - (Optional) [RecordEnricher]s to enrich the records before complex type transformation.
  /// - (Optional) [ComplexTypeTransformer] to flatten map/unnest list.
  /// - (Optional) Custom [RecordTransformer]s
  /// - (Optional) [DataTypeTransformer] to fix the data types of the source fields configured with
  /// `preComplexTypeTransform = false` in `IngestionConfig#getSourceFieldConfigs()`, plus (when
  /// `IngestionConfig#isConvertAggregationSourceTypes` is true) aggregation source columns that are not in the
  /// schema (see [#addAggregationSourceDataTypes]). It precedes the post-complex-type
  /// [RecordEnricher]s and [ExpressionTransformer] so that they consume the source fields with the corrected types.
  /// - (Optional) [RecordEnricher]s to enrich the records before other transformations.
  /// - (Optional) [ExpressionTransformer] to evaluate expressions and fill the values.
  /// - (Optional) [FilterTransformer] to filter records based on custom predicates.
  /// - (Optional) [SchemaConformingTransformer] to conform the records to the schema, keep or drop fields, and gain
  /// enhanced text search capabilities.
  /// - [DataTypeTransformer] to convert values to comply with the schema.
  /// - (Optional) [TimeValidationTransformer] to validate time values. It follows [DataTypeTransformer] so that time
  /// values are converted to the correct type.
  /// - (Optional) [SpecialValueTransformer] to handle special values. It follows [DataTypeTransformer] so that all
  /// values are converted to the correct type.
  /// - [NullValueTransformer] to handle null values. It follows [DataTypeTransformer] and [TimeValidationTransformer]
  /// because empty Collection/Map/Object[] and invalid values can be replaced with null.
  /// - (Optional) [SanitizationTransformer] to sanitize values. It follows [NullValueTransformer] so that before
  /// sanitization, all values are non-null and follow the data types defined in the schema.
  public static List<RecordTransformer> getTransformers(TableConfig tableConfig, @Nullable Schema schema,
      boolean skipPreComplexTypeTransformers, boolean skipComplexTypeTransformer,
      boolean skipPostComplexTypeTransformers, boolean skipFilterTransformer) {
    List<RecordTransformer> transformers = new ArrayList<>();
    if (!skipPreComplexTypeTransformers) {
      addSourceFieldDataTypeTransformer(tableConfig, schema, transformers, true);
      addRecordEnricherTransformers(tableConfig, transformers, true);
    }
    if (!skipComplexTypeTransformer) {
      addIfNotNoOp(transformers, ComplexTypeTransformer.create(tableConfig));
    }
    if (skipPostComplexTypeTransformers) {
      return transformers;
    }
    Preconditions.checkState(schema != null,
        "Schema must be provided when post complex type transformers are requested");
    addSourceFieldDataTypeTransformer(tableConfig, schema, transformers, false);
    addRecordEnricherTransformers(tableConfig, transformers, false);
    addIfNotNoOp(transformers, new ExpressionTransformer(tableConfig, schema));
    if (!skipFilterTransformer) {
      addIfNotNoOp(transformers, new FilterTransformer(tableConfig));
    }
    addIfNotNoOp(transformers, SchemaConformingTransformer.create(tableConfig, schema));
    addIfNotNoOp(transformers, new DataTypeTransformer(tableConfig, schema));
    addIfNotNoOp(transformers, new TimeValidationTransformer(tableConfig, schema));
    addIfNotNoOp(transformers, new SpecialValueTransformer(schema));
    addIfNotNoOp(transformers, new NullValueTransformer(tableConfig, schema));
    addIfNotNoOp(transformers, new SanitizationTransformer(schema));
    return transformers;
  }

  public static List<RecordTransformer> getDefaultTransformers(TableConfig tableConfig, Schema schema) {
    return getTransformers(tableConfig, schema, false, false, false, false);
  }

  /// Returns transformers to apply after a partial upsert merge. Only post-merge transform configs are honored to avoid
  /// re-running ingestion-time transforms. Derived columns must exist in the schema to be queryable.
  ///
  /// @param tableConfig The table configuration containing post-partial-upsert transform configs
  /// @param schema The table schema used for validation and type conversion
  /// @return List of transformers to apply after merge, or `null` if none configured
  @Nullable
  public static List<RecordTransformer> getPostPartialUpsertTransformers(TableConfig tableConfig, Schema schema) {
    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    if (upsertConfig == null) {
      return null;
    }
    List<TransformConfig> postPartialUpsertTransformConfigs = upsertConfig.getPostPartialUpsertTransformConfigs();
    if (CollectionUtils.isEmpty(postPartialUpsertTransformConfigs)) {
      return null;
    }
    List<RecordTransformer> transformers = new ArrayList<>();
    // TODO: Only re-apply transforms to columns touched by the partial upsert merge to avoid recomputing unrelated
    //       derived fields.
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    boolean continueOnError = ingestionConfig != null && ingestionConfig.isContinueOnError();
    addIfNotNoOp(transformers,
        new ExpressionTransformer(postPartialUpsertTransformConfigs, true /* overwriteExistingValues */,
            continueOnError));
    addIfNotNoOp(transformers, new DataTypeTransformer(tableConfig, schema));
    addIfNotNoOp(transformers, new TimeValidationTransformer(tableConfig, schema));
    addIfNotNoOp(transformers, new SpecialValueTransformer(schema));
    addIfNotNoOp(transformers, new NullValueTransformer(tableConfig, schema));
    addIfNotNoOp(transformers, new SanitizationTransformer(schema));
    return transformers;
  }

  private static void addIfNotNoOp(List<RecordTransformer> transformers, @Nullable RecordTransformer transformer) {
    if (transformer != null && !transformer.isNoOp()) {
      transformers.add(transformer);
    }
  }

  private static void addSourceFieldDataTypeTransformer(TableConfig tableConfig, @Nullable Schema schema,
      List<RecordTransformer> transformers, boolean preComplexTypeTransform) {
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    if (ingestionConfig == null) {
      return;
    }
    Map<String, PinotDataType> dataTypes = new HashMap<>();
    List<SourceFieldConfig> sourceFieldConfigs = ingestionConfig.getSourceFieldConfigs();
    if (CollectionUtils.isNotEmpty(sourceFieldConfigs)) {
      for (SourceFieldConfig sourceFieldConfig : sourceFieldConfigs) {
        // If pre-ComplexType transformers are requested, add only pre-ComplexType source fields. Similarly, if
        // non pre-ComplexType transformers are requested, add only non pre-ComplexType source fields.
        if (sourceFieldConfig.isPreComplexTypeTransform() == preComplexTypeTransform) {
          dataTypes.put(sourceFieldConfig.getName(), sourceFieldConfig.getDataType());
        }
      }
    }
    // Opt-in: convert aggregation source columns that are not in the schema (and not already covered by an explicit
    // SourceFieldConfig) so mistyped JSON/Avro string numbers are converted before MutableSegmentImpl indexes them.
    // Off by default; uses the stock DataTypeTransformer (no lazy compatibility short-circuit).
    if (!preComplexTypeTransform && schema != null && ingestionConfig.isConvertAggregationSourceTypes()) {
      addAggregationSourceDataTypes(tableConfig, schema, dataTypes);
    }
    if (!dataTypes.isEmpty()) {
      transformers.add(new DataTypeTransformer(tableConfig, dataTypes));
    }
  }

  /// Derives [PinotDataType]s for ingestion-aggregation source columns that are absent from the schema (and not already
  /// covered by an explicit [SourceFieldConfig] in either phase). Types are inferred from the aggregation function and
  /// destination metric. When one source feeds multiple aggregations, inferred numeric types are merged by keeping the
  /// wider type so config order cannot drop precision. Sketch/HLL/COUNT sources are left unconverted so offering
  /// semantics (e.g. hashing a string vs a number) are preserved.
  /// [org.apache.pinot.segment.local.aggregator.ValueAggregatorUtils#toDouble] remains a safety net.
  @VisibleForTesting
  static void addAggregationSourceDataTypes(TableConfig tableConfig, Schema schema,
      Map<String, PinotDataType> dataTypes) {
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    List<AggregationConfig> aggregationConfigs = ingestionConfig.getAggregationConfigs();
    if (CollectionUtils.isEmpty(aggregationConfigs)) {
      return;
    }
    // dataTypes only has this phase's SourceFieldConfigs. Pre-complex-type names are absent from the post-phase map
    // and must still skip inference so an explicit type is not overwritten.
    Set<String> explicitSourceFields = getExplicitSourceFieldNames(ingestionConfig);
    for (AggregationConfig aggregationConfig : aggregationConfigs) {
      String destColumn = aggregationConfig.getColumnName();
      String aggregationFunction = aggregationConfig.getAggregationFunction();
      ExpressionContext expressionContext;
      try {
        expressionContext = RequestContextUtils.getExpression(aggregationFunction);
      } catch (Exception e) {
        // Invalid configs are rejected at table-create validation time; skip here to keep transformer build resilient.
        continue;
      }
      if (expressionContext.getType() != ExpressionContext.Type.FUNCTION) {
        continue;
      }
      FunctionContext functionContext = expressionContext.getFunction();
      AggregationFunctionType functionType;
      try {
        functionType = AggregationFunctionType.getAggregationFunctionType(functionContext.getFunctionName());
      } catch (Exception e) {
        continue;
      }
      List<ExpressionContext> arguments = functionContext.getArguments();
      if (arguments.isEmpty()) {
        continue;
      }
      ExpressionContext firstArgument = arguments.get(0);
      if (firstArgument.getType() != ExpressionContext.Type.IDENTIFIER) {
        continue;
      }
      String sourceColumn = firstArgument.getIdentifier();
      if (AggregationFunctionColumnPair.STAR.equals(sourceColumn) || schema.hasColumn(sourceColumn)
          || explicitSourceFields.contains(sourceColumn)) {
        // Any explicit SourceFieldConfig (including pre-complex-type) or schema column already covers conversion;
        // COUNT(*) has no source value.
        continue;
      }
      FieldSpec destFieldSpec = schema.getFieldSpecFor(destColumn);
      PinotDataType inferredType = inferAggregationSourceDataType(functionType, destFieldSpec);
      if (inferredType != null) {
        PinotDataType existing = dataTypes.get(sourceColumn);
        dataTypes.put(sourceColumn,
            existing == null ? inferredType : mergeInferredAggregationSourceTypes(existing, inferredType));
      }
    }
  }

  /// Returns source field names configured in either transformer phase. Used so a pre-complex-type
  /// [SourceFieldConfig] is not overwritten by post-phase aggregation-source inference.
  private static Set<String> getExplicitSourceFieldNames(IngestionConfig ingestionConfig) {
    List<SourceFieldConfig> sourceFieldConfigs = ingestionConfig.getSourceFieldConfigs();
    if (CollectionUtils.isEmpty(sourceFieldConfigs)) {
      return Set.of();
    }
    Set<String> names = new HashSet<>(sourceFieldConfigs.size());
    for (SourceFieldConfig sourceFieldConfig : sourceFieldConfigs) {
      names.add(sourceFieldConfig.getName());
    }
    return names;
  }

  /// When one raw source feeds multiple aggregations, keep the type that loses less information. Numeric scalars
  /// widen `INT < LONG < FLOAT < DOUBLE < BIG_DECIMAL`. A scalar vs multi-value conflict keeps the first inferred
  /// shape so conversion does not rewrite an array to a scalar, or the reverse.
  static PinotDataType mergeInferredAggregationSourceTypes(PinotDataType existing, PinotDataType incoming) {
    if (existing == incoming) {
      return existing;
    }
    if (existing.isSingleValue() != incoming.isSingleValue()) {
      return existing;
    }
    int existingWidth = numericWidth(existing);
    int incomingWidth = numericWidth(incoming);
    if (existingWidth < 0 || incomingWidth < 0) {
      return existing;
    }
    return incomingWidth > existingWidth ? incoming : existing;
  }

  /// Width for inferred numeric conversion targets. Non-numeric types return `-1`.
  private static int numericWidth(PinotDataType type) {
    switch (type) {
      case INT:
        return 1;
      case LONG:
        return 2;
      case FLOAT:
        return 3;
      case DOUBLE:
        return 4;
      case BIG_DECIMAL:
        return 5;
      default:
        return -1;
    }
  }

  /// Returns the target type for converting an aggregation source column, or `null` when no conversion should be
  /// applied (COUNT, HLL, sketches: keep raw offering values).
  @Nullable
  static PinotDataType inferAggregationSourceDataType(AggregationFunctionType functionType,
      @Nullable FieldSpec destFieldSpec) {
    switch (functionType) {
      case SUM:
      case MIN:
      case MAX:
        if (destFieldSpec != null) {
          switch (destFieldSpec.getDataType().getStoredType()) {
            case INT:
              return PinotDataType.INT;
            case LONG:
              return PinotDataType.LONG;
            case FLOAT:
              return PinotDataType.FLOAT;
            case DOUBLE:
              return PinotDataType.DOUBLE;
            case BIG_DECIMAL:
              return PinotDataType.BIG_DECIMAL;
            default:
              return PinotDataType.DOUBLE;
          }
        }
        return PinotDataType.DOUBLE;
      case SUMMV:
      case AVGMV:
        // Multi-value sources must convert to an array type: a single-value target would make
        // DataTypeTransformerUtils.standardize throw on multi-element arrays. The MV aggregators sum/average the
        // elements through ValueAggregatorUtils.toDouble, so Double[] matches their expectations.
        return PinotDataType.DOUBLE_ARRAY;
      case AVG:
      case MINMAXRANGE:
      case PERCENTILEEST:
      case PERCENTILERAWEST:
      case PERCENTILETDIGEST:
      case PERCENTILERAWTDIGEST:
        return PinotDataType.DOUBLE;
      case SUMPRECISION:
        return PinotDataType.BIG_DECIMAL;
      default:
        // COUNT / HLL / sketches: do not auto-convert (preserves string hashing etc.)
        return null;
    }
  }

  private static void addRecordEnricherTransformers(TableConfig tableConfig, List<RecordTransformer> transformers,
      boolean preComplexTypeTransformers) {
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    if (ingestionConfig != null) {
      List<EnrichmentConfig> enrichmentConfigs = ingestionConfig.getEnrichmentConfigs();
      if (enrichmentConfigs != null) {
        for (EnrichmentConfig enrichmentConfig : enrichmentConfigs) {
          // if pre-ComplexType transformers are requested, add only pre-ComplexType transformers. Similarly, if
          // non pre-ComplexType transformers are requested, add only non pre-ComplexType transformers.
          if (preComplexTypeTransformers != enrichmentConfig.isPreComplexTypeTransform()) {
            continue;
          }
          RecordEnricher recordEnricher;
          try {
            recordEnricher = RecordEnricherRegistry.createRecordEnricher(enrichmentConfig);
          } catch (IOException e) {
            throw new RuntimeException("Failed to instantiate record enricher " + enrichmentConfig.getEnricherType(),
                e);
          }
          addIfNotNoOp(transformers, recordEnricher);
        }
      }
    }
  }
}
