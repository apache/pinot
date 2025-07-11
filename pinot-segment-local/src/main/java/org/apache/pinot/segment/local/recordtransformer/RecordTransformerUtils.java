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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.EnrichmentConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.apache.pinot.spi.recordtransformer.enricher.RecordEnricher;
import org.apache.pinot.spi.recordtransformer.enricher.RecordEnricherRegistry;


public class RecordTransformerUtils {
  private RecordTransformerUtils() {
  }

  /// Returns a list of [RecordTransformer]s based on the given [TableConfig] and [Schema].
  /// DO NOT CHANGE THE ORDER OF THE RECORD TRANSFORMERS.
  /// The transformers returned are:
  /// - (Optional) [RecordEnricher]s to enrich the records before complex type transformation.
  /// - (Optional) [ComplexTypeTransformer] to flatten map/unnest list.
  /// - (Optional) Custom [RecordTransformer]s
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

  private static void addIfNotNoOp(List<RecordTransformer> transformers, @Nullable RecordTransformer transformer) {
    if (transformer != null && !transformer.isNoOp()) {
      transformers.add(transformer);
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
