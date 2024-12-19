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

package org.apache.pinot.segment.local.indexsegment.mutable;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.dedup.TableDedupMetadataManager;
import org.apache.pinot.segment.local.dedup.TableDedupMetadataManagerFactory;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MutableSegmentDedupeTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), MutableSegmentDedupeTest.class.getSimpleName());
  private static final String SCHEMA_FILE_PATH = "data/test_dedup_schema.json";
  private static final String DATA_FILE_PATH = "data/test_dedup_data.json";
  private MutableSegmentImpl _mutableSegmentImpl;

  private void setup(boolean dedupEnabled, double metadataTTL, String dedupTimeColumn)
      throws Exception {
    URL schemaResourceUrl = this.getClass().getClassLoader().getResource(SCHEMA_FILE_PATH);
    URL dataResourceUrl = this.getClass().getClassLoader().getResource(DATA_FILE_PATH);
    Schema schema = Schema.fromFile(new File(schemaResourceUrl.getFile()));
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
        .setDedupConfig(new DedupConfig(dedupEnabled, HashFunction.NONE)).build();
    CompositeTransformer recordTransformer = CompositeTransformer.getDefaultTransformer(tableConfig, schema);
    File jsonFile = new File(dataResourceUrl.getFile());
    DedupConfig dedupConfig = new DedupConfig(true, HashFunction.NONE, null, null, metadataTTL, dedupTimeColumn, false);
    PartitionDedupMetadataManager partitionDedupMetadataManager =
        (dedupEnabled) ? getTableDedupMetadataManager(schema, dedupConfig).getOrCreatePartitionManager(0) : null;
    _mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Collections.emptySet(), Collections.emptySet(),
            Collections.emptySet(), false, true, null, "secondsSinceEpoch", null, dedupConfig,
            partitionDedupMetadataManager);
    GenericRow reuse = new GenericRow();
    try (RecordReader recordReader = RecordReaderFactory.getRecordReader(FileFormat.JSON, jsonFile,
        schema.getColumnNames(), null)) {
      while (recordReader.hasNext()) {
        recordReader.next(reuse);
        GenericRow transformedRow = recordTransformer.transform(reuse);
        _mutableSegmentImpl.index(transformedRow, null);
        if (dedupEnabled) {
          partitionDedupMetadataManager.removeExpiredPrimaryKeys();
        }
        reuse.clear();
      }
    }
  }

  private static TableDedupMetadataManager getTableDedupMetadataManager(Schema schema, DedupConfig dedupConfig) {
    TableConfig tableConfig = Mockito.mock(TableConfig.class);
    Mockito.when(tableConfig.getTableName()).thenReturn("testTable_REALTIME");
    Mockito.when(tableConfig.getDedupConfig()).thenReturn(dedupConfig);
    SegmentsValidationAndRetentionConfig segmentsValidationAndRetentionConfig =
        Mockito.mock(SegmentsValidationAndRetentionConfig.class);
    Mockito.when(tableConfig.getValidationConfig()).thenReturn(segmentsValidationAndRetentionConfig);
    Mockito.when(segmentsValidationAndRetentionConfig.getTimeColumnName()).thenReturn("secondsSinceEpoch");
    TableDataManager tableDataManager = Mockito.mock(TableDataManager.class);
    Mockito.when(tableDataManager.getTableDataDir()).thenReturn(TEMP_DIR);
    return TableDedupMetadataManagerFactory.create(tableConfig, schema, tableDataManager,
        Mockito.mock(ServerMetrics.class), null);
  }

  public List<Map<String, String>> loadJsonFile(String filePath)
      throws IOException {
    URL resourceUrl = this.getClass().getClassLoader().getResource(filePath);
    if (resourceUrl == null) {
      throw new IllegalArgumentException("File not found: " + filePath);
    }
    File jsonFile = new File(resourceUrl.getFile());
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(jsonFile, List.class);
  }

  @Test
  public void testDedupeEnabled()
      throws Exception {
    setup(true, 0, null);
    Assert.assertEquals(_mutableSegmentImpl.getNumDocsIndexed(), 2);
    List<Map<String, String>> rawData = loadJsonFile(DATA_FILE_PATH);
    for (int i = 0; i < 2; i++) {
      verifyGeneratedSegmentDataAgainstRawData(i, i, rawData);
    }
  }

  @Test
  public void testDedupeDisabled()
      throws Exception {
    setup(false, 0, null);
    Assert.assertEquals(_mutableSegmentImpl.getNumDocsIndexed(), 4);
    List<Map<String, String>> rawData = loadJsonFile(DATA_FILE_PATH);
    for (int i = 0; i < 4; i++) {
      verifyGeneratedSegmentDataAgainstRawData(i, i, rawData);
    }
  }

  @Test
  public void testDedupWithMetadataTTLWithoutDedupTimeColumn()
      throws Exception {
    setup(true, 1000, null);
    checkGeneratedSegmentDataWhenTableTimeColumnIsUsedAsDedupTimeColumn();
  }

  @Test
  public void testDedupWithMetadataTTLWithTableTimeColumn()
      throws Exception {
    setup(true, 1000, "secondsSinceEpoch");
    checkGeneratedSegmentDataWhenTableTimeColumnIsUsedAsDedupTimeColumn();
  }

  private void checkGeneratedSegmentDataWhenTableTimeColumnIsUsedAsDedupTimeColumn()
      throws IOException {
    Assert.assertEquals(_mutableSegmentImpl.getNumDocsIndexed(), 3);
    List<Map<String, String>> rawData = loadJsonFile(DATA_FILE_PATH);
    for (int i = 0; i < 2; i++) {
      verifyGeneratedSegmentDataAgainstRawData(i, i, rawData);
    }
    verifyGeneratedSegmentDataAgainstRawData(2, 3, rawData);
  }

  @Test
  public void testDedupWithMetadataTTLWithDedupTimeColumn()
      throws Exception {
    setup(true, 1000, "dedupTime");
    Assert.assertEquals(_mutableSegmentImpl.getNumDocsIndexed(), 2);
    List<Map<String, String>> rawData = loadJsonFile(DATA_FILE_PATH);
    for (int i = 0; i < 2; i++) {
      verifyGeneratedSegmentDataAgainstRawData(i, i, rawData);
    }
  }

  private void verifyGeneratedSegmentDataAgainstRawData(int docId, int rawDataIndex,
      List<Map<String, String>> rawData) {
    for (String columnName : rawData.get(0).keySet()) {
      Assert.assertEquals(String.valueOf(_mutableSegmentImpl.getValue(docId, columnName)),
          String.valueOf(rawData.get(rawDataIndex).get(columnName)));
    }
  }
}
