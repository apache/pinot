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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.EnrichmentConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


public class CompositeTransformerTest {
  @Test
  public void testGetPreComplexTypeTransformers()
      throws JsonProcessingException {
    String tableName = "myTable_OFFLINE";
    IngestionConfig ingestionConfig = new IngestionConfig();
    EnrichmentConfig erc1 = new EnrichmentConfig("generateColumn",
        new ObjectMapper().readTree("{\"fieldToFunctionMap\": {}}"), true);
    EnrichmentConfig erc2 = new EnrichmentConfig("noOp",
        new ObjectMapper().readTree("{}"), false);
    List<EnrichmentConfig> enrichmentConfigs = Arrays.asList(erc1, erc2);
    ingestionConfig.setEnrichmentConfigs(enrichmentConfigs);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName)
        .setIngestionConfig(ingestionConfig).build();
    List<RecordTransformer> recordTransformers = CompositeTransformer.getPreComplexTypeTransformers(tableConfig);
    assertEquals(recordTransformers.size(), 1);
  }

  @Test
  public void testGetDefaultTransformers()
      throws JsonProcessingException {
    String tableName = "myTable_OFFLINE";
    IngestionConfig ingestionConfig = new IngestionConfig();
    EnrichmentConfig erc1 = new EnrichmentConfig("generateColumn",
        new ObjectMapper().readTree("{\"fieldToFunctionMap\": {}}"), true);
    List<EnrichmentConfig> enrichmentConfigs = List.of(erc1);
    ingestionConfig.setEnrichmentConfigs(enrichmentConfigs);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName)
        .setIngestionConfig(ingestionConfig).build();
    Schema schema = new Schema();
    schema.setSchemaName(tableName);
    schema.addField(new DimensionFieldSpec("id", FieldSpec.DataType.STRING, true));
    List<RecordTransformer> recordTransformers = CompositeTransformer.getDefaultTransformers(tableConfig, schema);
    assertEquals(recordTransformers.size(), 3);
    Set<String> recordTransformerNames = new HashSet<>();
    for (RecordTransformer recordTransformer : recordTransformers) {
      recordTransformerNames.add(recordTransformer.getClass().getSimpleName());
    }
    assertFalse(recordTransformerNames.contains("CustomFunctionEnricher"));
  }
}
