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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test that verifies inverted indexes can be built for raw-forward columns (no-dictionary forward index)
 * and that queries can leverage those indexes.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class RawForwardIndexInvertedIndexTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "RawForwardIndexInvertedIndexTest";
  private static final String RAW_DIMENSION = "rawDim";
  private static final String METRIC_COLUMN = "metric";
  private static final int ROW_COUNT = 500;
  private static final int UNIQUE_DIMENSION_VALUES = 10;
  private static final String FILTER_VALUE = RAW_DIMENSION + "-3";

  @Override
  protected long getCountStarResult() {
    return ROW_COUNT;
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName()).setSortedColumn(getSortedColumn())
        .setInvertedIndexColumns(getInvertedIndexColumns())
        .setCreateInvertedIndexDuringSegmentGeneration(isCreateInvertedIndexDuringSegmentGeneration())
        .setNoDictionaryColumns(getNoDictionaryColumns()).setRangeIndexColumns(getRangeIndexColumns())
        .setBloomFilterColumns(getBloomFilterColumns()).setFieldConfigList(getFieldConfigs())
        .setNumReplicas(getNumReplicas()).setSegmentVersion(getSegmentVersion()).setLoadMode(getLoadMode())
        .setTaskConfig(getTaskConfig()).setBrokerTenant(getBrokerTenant()).setServerTenant(getServerTenant())
        .setIngestionConfig(getIngestionConfig()).setQueryConfig(getQueryConfig())
        .setNullHandlingEnabled(getNullHandlingEnabled()).setSegmentPartitionConfig(getSegmentPartitionConfig())
        .setOptimizeNoDictStatsCollection(true).build();
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(RAW_DIMENSION, FieldSpec.DataType.STRING)
        .addMetric(METRIC_COLUMN, FieldSpec.DataType.LONG)
        .addDateTime(TIMESTAMP_FIELD_NAME, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("RawForwardIndexRecord").fields()
        .requiredString(RAW_DIMENSION)
        .requiredLong(METRIC_COLUMN)
        .requiredLong(TIMESTAMP_FIELD_NAME)
        .endRecord();

    File avroFile = new File(_tempDir, "raw-forward-inverted-index.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      Random random = new Random(1234);
      long currentTimeMillis = System.currentTimeMillis();
      for (int i = 0; i < ROW_COUNT; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(RAW_DIMENSION, RAW_DIMENSION + "-" + (i % UNIQUE_DIMENSION_VALUES));
        record.put(METRIC_COLUMN, random.nextInt(10_000));
        record.put(TIMESTAMP_FIELD_NAME, currentTimeMillis + i);
        fileWriter.append(record);
      }
    }
    return List.of(avroFile);
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    return List.of();
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("dictionary", JsonUtils.newObjectNode());
    FieldConfig fieldConfig =
        new FieldConfig(RAW_DIMENSION, FieldConfig.EncodingType.RAW, null, null, null, null, indexes, null, null);
    return List.of(fieldConfig);
  }

  @Override
  protected List<String> getInvertedIndexColumns() {
    return List.of(RAW_DIMENSION);
  }

  @Override
  protected boolean isCreateInvertedIndexDuringSegmentGeneration() {
    return true;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testInvertedIndexOnRawForwardColumn(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT COUNT(*) FROM %s WHERE %s = '%s'", getTableName(), RAW_DIMENSION, FILTER_VALUE);
    JsonNode response = postQuery(query);
    long matchedCount = response.get("resultTable").get("rows").get(0).get(0).asLong();
    assertEquals(matchedCount, ROW_COUNT / UNIQUE_DIMENSION_VALUES);
    assertEquals(response.get("numEntriesScannedInFilter").asLong(), 0L,
        "Inverted index should avoid scanning filters even for raw forward index");
    JsonNode explainPlan = postQuery("SET useMultistageEngine=false; EXPLAIN PLAN FOR " + query);
    assertTrue(explainPlan.toString().contains("FILTER_INVERTED_INDEX"),
        "Explain plan should confirm inverted index usage: " + explainPlan);
  }

  @Test
  public void testSegmentMetadataForRawForwardColumn()
      throws IOException {
    JsonNode segmentsMetadata = JsonUtils.stringToJsonNode(sendGetRequest(
        _controllerRequestURLBuilder.forSegmentsMetadataFromServer(getTableName(), List.of(RAW_DIMENSION))));
    assertTrue(segmentsMetadata.size() > 0);
    JsonNode rawColumnMetadata = null;
    for (JsonNode segmentMetadata : segmentsMetadata) {
      JsonNode columnsMetadata = segmentMetadata.get("columns");
      if (columnsMetadata == null) {
        continue;
      }
      for (JsonNode columnMetadata : columnsMetadata) {
        if (RAW_DIMENSION.equals(columnMetadata.get("columnName").asText())) {
          rawColumnMetadata = columnMetadata;
          break;
        }
      }
      if (rawColumnMetadata != null) {
        break;
      }
    }
    assertNotNull(rawColumnMetadata);
    assertTrue(rawColumnMetadata.get("hasDictionary").asBoolean(),
        "Dictionary should be built for inverted index even when forward index is raw");
    TableConfig offlineTableConfig = getOfflineTableConfig();
    List<FieldConfig> fieldConfigs = offlineTableConfig.getFieldConfigList();
    assertNotNull(fieldConfigs, "Field configs should be configured for raw forward index");
    FieldConfig rawFieldConfig = null;
    for (FieldConfig fieldConfig : fieldConfigs) {
      if (RAW_DIMENSION.equals(fieldConfig.getName())) {
        rawFieldConfig = fieldConfig;
        break;
      }
    }
    assertNotNull(rawFieldConfig, "Raw forward index field config should be present");
    assertEquals(rawFieldConfig.getEncodingType(), FieldConfig.EncodingType.RAW,
        "Forward index should remain raw via FieldConfig");
    assertTrue(rawFieldConfig.getIndexes() != null && rawFieldConfig.getIndexes().has("dictionary"),
        "Dictionary should be explicitly configured for raw forward index");
  }
}
