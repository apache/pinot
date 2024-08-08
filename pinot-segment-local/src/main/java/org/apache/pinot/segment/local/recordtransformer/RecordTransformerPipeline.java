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
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.readers.GenericRow;


public class RecordTransformerPipeline {
  private final List<RecordTransformer> _enrichers = new ArrayList<>();
  private final Set<String> _columnsToExtract = new HashSet<>();

  public static RecordTransformerPipeline getPassThroughPipeline() {
    return new RecordTransformerPipeline();
  }

  public static RecordTransformerPipeline fromIngestionConfig(IngestionConfig ingestionConfig) {
    RecordTransformerPipeline pipeline = new RecordTransformerPipeline();
    if (null == ingestionConfig || null == ingestionConfig.getTransformConfigs()) {
      return pipeline;
    }
    List<TransformConfig> enrichmentConfigs = ingestionConfig.getTransformConfigs();
    for (TransformConfig transformConfig : enrichmentConfigs) {
      try {
        if (transformConfig.getEnricherType() != null) {
          RecordTransformer enricher = RecordTransformerRegistry.createRecordTransformer(transformConfig);
          pipeline.add(enricher);
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to instantiate record enricher " + transformConfig.getEnricherType(), e);
      }
    }
    return pipeline;
  }

  public static RecordTransformerPipeline fromTableConfig(TableConfig tableConfig) {
    return fromIngestionConfig(tableConfig.getIngestionConfig());
  }

  public Set<String> getColumnsToExtract() {
    return _columnsToExtract;
  }

  public void add(RecordTransformer enricher) {
    _enrichers.add(enricher);
    _columnsToExtract.addAll(enricher.getInputColumns());
  }

  public void run(GenericRow record) {
    for (RecordTransformer enricher : _enrichers) {
      enricher.transform(record);
    }
  }
}
