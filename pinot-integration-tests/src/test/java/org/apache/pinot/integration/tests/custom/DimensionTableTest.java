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
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.core.data.manager.offline.DimensionTableDataManager;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.spi.config.table.DimensionTableConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Integration test for dimension table functionality.
 * Tests that dimension tables can be loaded, queried, and properly cleaned up on delete.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class DimensionTableTest extends CustomDataQueryClusterIntegrationTest {

  private static final String TABLE_NAME = "DimensionTableTest";
  private static final String LONG_COL = "longCol";
  private static final String INT_COL = "intCol";

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return 1000;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(getTableName())
        .addSingleValueDimension(LONG_COL, FieldSpec.DataType.LONG)
        .addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
        .setPrimaryKeyColumns(Collections.singletonList(LONG_COL))
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(LONG_COL,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null, null),
        new org.apache.avro.Schema.Field(INT_COL,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null)));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < getCountStarResult(); i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(LONG_COL, (long) i);
        record.put(INT_COL, i);
        writers.get(i % writers.size()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setDimensionTableConfig(new DimensionTableConfig(false, false))
        .setIsDimTable(true)
        .build();
  }

  /**
   * Override to use buildSegmentsFromAvro (plural) which handles segment naming properly
   * for dimension tables that have no time column.
   */
  @Override
  protected void setUpTable()
      throws Exception {
    Schema schema = createSchema();
    addSchema(schema);

    List<File> avroFiles = createAvroFiles();
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Dimension tables are loaded into memory; wait specifically for non-zero docs
    waitForNonZeroDocsLoaded(60_000L, true, getTableName());
  }

  @Override
  protected void waitForAllDocsLoaded(long timeoutMs)
      throws Exception {
    // Already waited in setUpTable; just verify
    long count = getCurrentCountStarResult();
    Assert.assertEquals(count, getCountStarResult(), "Dimension table doc count mismatch");
  }

  @Test
  public void testQueryDimensionTable()
      throws Exception {
    JsonNode node = postQuery("SELECT COUNT(*) FROM " + getTableName());
    assertNoError(node);
    Assert.assertEquals(node.get("resultTable").get("rows").get(0).get(0).asInt(), (int) getCountStarResult());
  }

  @Test(dependsOnMethods = "testQueryDimensionTable")
  public void testDeleteDimensionTable()
      throws Exception {
    dropOfflineTable(getTableName(), "-1d");
    waitForEVToDisappear(TableNameBuilder.OFFLINE.tableNameWithType(getTableName()));
    Assert.assertNull(
        DimensionTableDataManager.getInstanceByTableName(TableNameBuilder.OFFLINE.tableNameWithType(getTableName())));
  }

  @Override
  public void tearDown()
      throws IOException {
    // Table may already be dropped by testDeleteDimensionTable
    synchronized (SUITE_MUTATION_LOCK) {
      LOGGER.warn("Tearing down integration test class: {}", getClass().getSimpleName());
      try {
        dropOfflineTable(getTableName());
      } catch (Exception e) {
        // Expected if testDeleteDimensionTable already dropped it
      }
      LOGGER.warn("Finished tearing down integration test class: {}", getClass().getSimpleName());
    }
  }
}
