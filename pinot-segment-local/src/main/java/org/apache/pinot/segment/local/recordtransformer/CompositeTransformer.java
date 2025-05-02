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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.EnrichmentConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.apache.pinot.spi.recordtransformer.enricher.RecordEnricher;
import org.apache.pinot.spi.recordtransformer.enricher.RecordEnricherRegistry;


/**
 * The {@code CompositeTransformer} class performs multiple transforms based on the inner {@link RecordTransformer}s.
 */
public class CompositeTransformer implements RecordTransformer {
  // TODO: Integrate ComplexTypeTransformer into the CompositeTransformer.
  private final List<RecordTransformer> _transformers;

  /**
   * Returns a list of record transformers that perform enrichment of the record before the record is passed onto the
   * ComplexType transformer. The transform pipeline order is as follows:
   * <ol>
   *  <li> pre-complex type transformers</li>
   *  <li> complex type transformers</li>
   *  <li> plain record transformers</li>
   * </ol>
   */
  public static List<RecordTransformer> getPreComplexTypeTransformers(TableConfig tableConfig) {
    List<RecordTransformer> preComplexTypeTransformers = new ArrayList<>();
    addRecordEnricherTransformers(tableConfig, preComplexTypeTransformers, true);
    return preComplexTypeTransformers;
  }

  /**
   * Returns a record transformer that performs null value handling, time/expression/data-type transformation and record
   * sanitization. Note that the list of transformers returned from this method does not include the pre-complex type
   * and complex type transformers.
   * <p>NOTE: DO NOT CHANGE THE ORDER OF THE RECORD TRANSFORMERS
   * <ul>
   *   <li>
   *     Optional list of {@link RecordEnricher}s to enrich the record before any other transformation.
   *   </li>
   *   <li>
   *     Optional {@link ExpressionTransformer} after enrichers, so that we get the real columns for other transformers
   *     to work on
   *   </li>
   *   <li>
   *     Optional {@link FilterTransformer} after {@link ExpressionTransformer}, so that we have source as well as
   *     destination columns
   *   </li>
   *   <li>
   *     Optional {@link SchemaConformingTransformer} after {@link FilterTransformer}, so that we can transform input
   *     records that have varying fields to a fixed schema and keep or drop other fields by configuration. We
   *     could also gain enhanced text search capabilities from it.
   *   </li>
   *   <li>
   *     {@link DataTypeTransformer} after {@link SchemaConformingTransformer}
   *     to convert values to comply with the schema
   *   </li>
   *   <li>
   *     Optional {@link TimeValidationTransformer} after {@link DataTypeTransformer} so that time value is converted to
   *     the correct type
   *   </li>
   *   <li>
   *     {@link NullValueTransformer} after {@link DataTypeTransformer} and {@link TimeValidationTransformer} because
   *     empty Collection/Map/Object[] and invalid values can be replaced with null
   *   </li>
   *   <li>
   *     Optional {@link SanitizationTransformer} after {@link NullValueTransformer} so that before sanitation, all
   *     values are non-null and follow the data types defined in the schema
   *   </li>
   *   <li>
   *     {@link SpecialValueTransformer} after {@link DataTypeTransformer} so that we already have the values complying
   *      with the schema before handling special values and before {@link NullValueTransformer} so that it transforms
   *      all the null values properly
   *   </li>
   * </ul>
   */
  public static List<RecordTransformer> getDefaultTransformers(TableConfig tableConfig, Schema schema) {
    List<RecordTransformer> transformers = new ArrayList<>();
    addRecordEnricherTransformers(tableConfig, transformers, false);
    addIfNotNoOp(transformers, new ExpressionTransformer(tableConfig, schema));
    addIfNotNoOp(transformers, new FilterTransformer(tableConfig));
    addIfNotNoOp(transformers, new SchemaConformingTransformer(tableConfig, schema));
    addIfNotNoOp(transformers, new DataTypeTransformer(tableConfig, schema));
    addIfNotNoOp(transformers, new TimeValidationTransformer(tableConfig, schema));
    addIfNotNoOp(transformers, new SpecialValueTransformer(schema));
    addIfNotNoOp(transformers, new NullValueTransformer(tableConfig, schema));
    addIfNotNoOp(transformers, new SanitizationTransformer(schema));
    return transformers;
  }

  private static void addIfNotNoOp(List<RecordTransformer> transformers, RecordTransformer transformer) {
    if (!transformer.isNoOp()) {
      transformers.add(transformer);
    }
  }

  private static void addRecordEnricherTransformers(TableConfig tableConfig,
      List<RecordTransformer> transformers, boolean preComplexTypeTransformers) {
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
          try {
            addIfNotNoOp(transformers, RecordEnricherRegistry.createRecordEnricher(enrichmentConfig));
          } catch (IOException e) {
            throw new RuntimeException("Failed to instantiate record enricher " + enrichmentConfig.getEnricherType(),
                e);
          }
        }
      }
    }
  }

  public static CompositeTransformer getDefaultTransformer(TableConfig tableConfig, Schema schema, @Nullable
      SegmentZKMetadata segmentZKMetadata) {
    return new CompositeTransformer(getDefaultTransformers(tableConfig, schema));
  }

  public static CompositeTransformer getDefaultTransformer(TableConfig tableConfig, Schema schema) {
    return new CompositeTransformer(getDefaultTransformers(tableConfig, schema));
  }

  /**
   * Includes custom and default transformers.
   */
  public static CompositeTransformer composeAllTransformers(List<RecordTransformer> customTransformers,
      TableConfig tableConfig, Schema schema) {
    List<RecordTransformer> allTransformers = new ArrayList<>(customTransformers);
    allTransformers.addAll(getDefaultTransformers(tableConfig, schema));
    return new CompositeTransformer(allTransformers);
  }

  /**
   * Returns a pass through record transformer that does not transform the record.
   */
  public static CompositeTransformer getPassThroughTransformer() {
    return new CompositeTransformer(List.of());
  }

  public CompositeTransformer(List<RecordTransformer> transformers) {
    _transformers = transformers;
  }

  @Override
  public Set<String> getInputColumns() {
    Set<String> inputColumns = new HashSet<>();
    for (RecordTransformer transformer : _transformers) {
      inputColumns.addAll(transformer.getInputColumns());
    }
    return inputColumns;
  }

  @Nullable
  @Override
  public GenericRow transform(GenericRow record) {
    for (RecordTransformer transformer : _transformers) {
      if (!IngestionUtils.shouldIngestRow(record)) {
        return record;
      }
      record = transformer.transform(record);
      if (record == null) {
        return null;
      }
    }
    return record;
  }
}
