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
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.dedup.TableDedupMetadataManager;
import org.apache.pinot.segment.local.dedup.TableDedupMetadataManagerFactory;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class MutableSegmentDedupTest implements PinotBuffersAfterMethodCheckRule {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), MutableSegmentDedupTest.class.getSimpleName());
  private static final String SCHEMA_FILE_PATH = "data/test_dedup_schema.json";
  private static final String DATA_FILE_PATH = "data/test_dedup_data.json";
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String TIME_COLUMN = "secondsSinceEpoch";

  private MutableSegmentImpl _mutableSegmentImpl;

  private void setup(boolean dedupEnabled, double metadataTTL, @Nullable String dedupTimeColumn)
      throws Exception {
    FileUtils.forceMkdir(TEMP_DIR);
    URL schemaResourceUrl = this.getClass().getClassLoader().getResource(SCHEMA_FILE_PATH);
    URL dataResourceUrl = this.getClass().getClassLoader().getResource(DATA_FILE_PATH);
    Schema schema = Schema.fromFile(new File(schemaResourceUrl.getFile()));
    DedupConfig dedupConfig = new DedupConfig();
    dedupConfig.setMetadataTTL(metadataTTL);
    dedupConfig.setDedupTimeColumn(dedupTimeColumn);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setDedupConfig(dedupConfig)
        .build();
    CompositeTransformer recordTransformer = CompositeTransformer.getDefaultTransformer(tableConfig, schema);
    File jsonFile = new File(dataResourceUrl.getFile());
    PartitionDedupMetadataManager partitionDedupMetadataManager =
        (dedupEnabled) ? getTableDedupMetadataManager(schema, dedupConfig).getOrCreatePartitionManager(0) : null;
    try {
      _mutableSegmentImpl = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, true, TIME_COLUMN, null,
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
    } finally {
      if (partitionDedupMetadataManager != null) {
        partitionDedupMetadataManager.stop();
        partitionDedupMetadataManager.close();
      }
    }
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    if (_mutableSegmentImpl != null) {
      _mutableSegmentImpl.destroy();
      _mutableSegmentImpl = null;
    }
    FileUtils.forceDelete(TEMP_DIR);
  }

  private static TableDedupMetadataManager getTableDedupMetadataManager(Schema schema, DedupConfig dedupConfig) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setDedupConfig(dedupConfig)
        .build();
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(TEMP_DIR);
    return TableDedupMetadataManagerFactory.create(new PinotConfiguration(), tableConfig, schema, tableDataManager);
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
  public void testDedupEnabled()
      throws Exception {
    setup(true, 0, null);
    Assert.assertEquals(_mutableSegmentImpl.getNumDocsIndexed(), 2);
    List<Map<String, String>> rawData = loadJsonFile(DATA_FILE_PATH);
    for (int i = 0; i < 2; i++) {
      verifyGeneratedSegmentDataAgainstRawData(i, i, rawData);
    }
  }

  @Test
  public void testDedupDisabled()
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
    setup(true, 1000, TIME_COLUMN);
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
