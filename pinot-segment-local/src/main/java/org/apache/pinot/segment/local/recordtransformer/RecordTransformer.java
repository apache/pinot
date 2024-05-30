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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.plugin.record.enricher.RecordEnricherRegistry;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.EnrichmentConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The record transformer which takes a {@link GenericRow} and transform it based on some custom rules.
 */
public interface RecordTransformer extends Serializable {
  final boolean GROOVY_DISABLED = false;

  static final Logger LOGGER = LoggerFactory.getLogger(RecordTransformer.class);
  Map<String, RecordTransformer> RECORD_ENRICHER_FACTORY_MAP = RecordEnricherRegistry.getRecordEnricherFactoryMap();
  final List<RecordTransformer> ENRICHERS = new ArrayList<>();
  final Set<String> COLUMNS_TO_EXTRACT = new HashSet<>();
  static final String NONE_TYPE = "";

  static void validateEnrichmentConfig(EnrichmentConfig enrichmentConfig, boolean config) {
    if (!RECORD_ENRICHER_FACTORY_MAP.containsKey(enrichmentConfig.getEnricherType())) {
      throw new IllegalArgumentException("No record enricher found for type: " + enrichmentConfig.getEnricherType());
    }
    RECORD_ENRICHER_FACTORY_MAP.get(enrichmentConfig.getEnricherType())
        .validateEnrichmentConfig(enrichmentConfig.getProperties(), config);
  }

  static RecordTransformer createRecordEnricher(EnrichmentConfig enrichmentConfig)
      throws IOException {
    if (!RECORD_ENRICHER_FACTORY_MAP.containsKey(enrichmentConfig.getEnricherType())) {
      throw new IllegalArgumentException("No record enricher found for type: " + enrichmentConfig.getEnricherType());
    }
    return RECORD_ENRICHER_FACTORY_MAP.get(enrichmentConfig.getEnricherType())
        .createEnricher(enrichmentConfig.getProperties());
  }

  static RecordTransformer getPassThroughPipeline() {
    return new RecordTransformer() {
    };
  }

  static RecordTransformer fromIngestionConfig(IngestionConfig ingestionConfig) {
    RecordTransformer pipeline = new RecordTransformer() {
    };
    if (null == ingestionConfig || null == ingestionConfig.getEnrichmentConfigs()) {
      return pipeline;
    }
    List<EnrichmentConfig> enrichmentConfigs = ingestionConfig.getEnrichmentConfigs();
    for (EnrichmentConfig enrichmentConfig : enrichmentConfigs) {
      try {
        RecordTransformer enricher = RecordTransformer.createRecordEnricher(enrichmentConfig);
        pipeline.add(enricher);
      } catch (IOException e) {
        throw new RuntimeException("Failed to instantiate record enricher " + enrichmentConfig.getEnricherType(), e);
      }
    }
    return pipeline;
  }

  static RecordTransformer fromTableConfig(TableConfig tableConfig) {
    return fromIngestionConfig(tableConfig.getIngestionConfig());
  }

  /**
   * Returns {@code true} if the transformer is no-op (can be skipped), {@code false} otherwise.
   */
  default boolean isNoOp() {
    return false;
  }

  /**
   * Transforms a record based on some custom rules.
   *
   * @param record Record to transform
   * @return Transformed record, or {@code null} if the record does not follow certain rules.
   */
  @Nullable
  default GenericRow transform(GenericRow record) {
    return null;
  }

  /**
   * Returns the list of input columns required for enriching the record.
   * This is used to make sure the required input fields are extracted.
   */
  default List<String> getInputColumns() {
    return new ArrayList<>();
  }

  default String getEnricherType() {
    return NONE_TYPE;
  }

  default RecordTransformer createEnricher(JsonNode enricherProps)
      throws IOException {
    return null;
  }

  default void validateEnrichmentConfig(JsonNode enricherProps, boolean validationConfig) {
  }

  default boolean isGroovyDisabled() {
    return GROOVY_DISABLED;
  }

  default Set<String> getColumnsToExtract() {
    return COLUMNS_TO_EXTRACT;
  }

  default void add(RecordTransformer enricher) {
    ENRICHERS.add(enricher);
    COLUMNS_TO_EXTRACT.addAll(enricher.getInputColumns());
  }

  default void run(GenericRow record) {
    for (RecordTransformer enricher : ENRICHERS) {
      enricher.transform(record);
    }
  }
}
