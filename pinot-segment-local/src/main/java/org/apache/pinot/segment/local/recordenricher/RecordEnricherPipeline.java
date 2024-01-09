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
package org.apache.pinot.segment.local.recordenricher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.EnrichmentConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.readers.GenericRow;


public class RecordEnricherPipeline {
  private final List<RecordEnricher> _enrichers = new ArrayList<>();
  private final Set<String> _columnsToExtract = new HashSet<>();

  public static RecordEnricherPipeline getPassThroughPipeline() {
    return new RecordEnricherPipeline();
  }

  public static RecordEnricherPipeline fromIngestionConfig(IngestionConfig ingestionConfig) {
    RecordEnricherPipeline pipeline = new RecordEnricherPipeline();
    if (null == ingestionConfig || null == ingestionConfig.getEnrichmentConfigs()) {
      return pipeline;
    }
    List<EnrichmentConfig> enrichmentConfigs = ingestionConfig.getEnrichmentConfigs();
    for (EnrichmentConfig enrichmentConfig : enrichmentConfigs) {
      try {
        RecordEnricher enricher = (RecordEnricher) Class.forName(enrichmentConfig.getEnricherClassName()).newInstance();
        enricher.init(enrichmentConfig.getProperties());
        pipeline.add(enricher);
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        throw new RuntimeException("Failed to instantiate record enricher" + enrichmentConfig.getEnricherClassName(),
            e);
      }
    }
    return pipeline;
  }

  public static RecordEnricherPipeline fromTableConfig(TableConfig tableConfig) {
    return fromIngestionConfig(tableConfig.getIngestionConfig());
  }

  public Set<String> getColumnsToExtract() {
    return _columnsToExtract;
  }

  public void add(RecordEnricher enricher) {
    _enrichers.add(enricher);
    _columnsToExtract.addAll(enricher.getInputColumns());
  }

  public void run(GenericRow record) {
    for (RecordEnricher enricher : _enrichers) {
      enricher.enrich(record);
    }
  }
}
