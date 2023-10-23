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

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManagerFactory;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class MutableSegmentImplUpsertTest {
  private static final String SCHEMA_FILE_PATH = "data/test_upsert_schema.json";
  private static final String DATA_FILE_PATH = "data/test_upsert_data.json";
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);

  private TableDataManager _tableDataManager;
  private TableConfig _tableConfig;
  private Schema _schema;
  private CompositeTransformer _recordTransformer;
  private MutableSegmentImpl _mutableSegmentImpl;
  private PartitionUpsertMetadataManager _partitionUpsertMetadataManager;

  @BeforeClass
  public void setUp() {
    ServerMetrics.register(mock(ServerMetrics.class));
    _tableDataManager = mock(TableDataManager.class);
    when(_tableDataManager.getTableDataDir()).thenReturn(new File(REALTIME_TABLE_NAME));
  }

  private UpsertConfig createPartialUpsertConfig(HashFunction hashFunction) {
    UpsertConfig upsertConfigWithHash = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfigWithHash.setPartialUpsertStrategies(new HashMap<>());
    upsertConfigWithHash.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.OVERWRITE);
    upsertConfigWithHash.setComparisonColumns(Arrays.asList("secondsSinceEpoch", "otherComparisonColumn"));
    upsertConfigWithHash.setHashFunction(hashFunction);
    return upsertConfigWithHash;
  }

  private UpsertConfig createFullUpsertConfig(HashFunction hashFunction) {
    UpsertConfig upsertConfigWithHash = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithHash.setHashFunction(hashFunction);
    return upsertConfigWithHash;
  }

  private void setup(UpsertConfig upsertConfigWithHash)
      throws Exception {
    URL schemaResourceUrl = this.getClass().getClassLoader().getResource(SCHEMA_FILE_PATH);
    URL dataResourceUrl = this.getClass().getClassLoader().getResource(DATA_FILE_PATH);
    _tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setUpsertConfig(upsertConfigWithHash)
            .setNullHandlingEnabled(true).build();
    _schema = Schema.fromFile(new File(schemaResourceUrl.getFile()));
    _recordTransformer = CompositeTransformer.getDefaultTransformer(_tableConfig, _schema);
    File jsonFile = new File(dataResourceUrl.getFile());
    TableUpsertMetadataManager tableUpsertMetadataManager = TableUpsertMetadataManagerFactory.create(_tableConfig, null);
    tableUpsertMetadataManager.init(_tableConfig, _schema, _tableDataManager, mock(HelixManager.class),
        mock(ExecutorService.class));
    _partitionUpsertMetadataManager = tableUpsertMetadataManager.getOrCreatePartitionManager(0);
    _mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(_schema, Collections.emptySet(), Collections.emptySet(),
            Collections.emptySet(), false, true, upsertConfigWithHash, "secondsSinceEpoch",
            _partitionUpsertMetadataManager, null);

    GenericRow reuse = new GenericRow();
    try (RecordReader recordReader = RecordReaderFactory.getRecordReader(FileFormat.JSON, jsonFile,
        _schema.getColumnNames(), null)) {
      while (recordReader.hasNext()) {
        recordReader.next(reuse);
        GenericRow transformedRow = _recordTransformer.transform(reuse);
        _mutableSegmentImpl.index(transformedRow, null);
        reuse.clear();
      }
    }
  }

  @Test
  public void testHashFunctions()
      throws Exception {
    testUpsertIngestion(createFullUpsertConfig(HashFunction.NONE));
    testUpsertIngestion(createFullUpsertConfig(HashFunction.MD5));
    testUpsertIngestion(createFullUpsertConfig(HashFunction.MURMUR3));
  }

  @Test
  public void testMultipleComparisonColumns()
      throws Exception {
    testUpsertIngestion(createPartialUpsertConfig(HashFunction.NONE));
    testUpsertIngestion(createPartialUpsertConfig(HashFunction.MD5));
    testUpsertIngestion(createPartialUpsertConfig(HashFunction.MURMUR3));
  }

  private void testUpsertIngestion(UpsertConfig upsertConfig)
      throws Exception {
    setup(upsertConfig);
    ImmutableRoaringBitmap bitmap = _mutableSegmentImpl.getValidDocIds().getMutableRoaringBitmap();
    if (upsertConfig.getComparisonColumns() == null) {
      // aa
      Assert.assertFalse(bitmap.contains(0));
      Assert.assertTrue(bitmap.contains(1));
      Assert.assertFalse(bitmap.contains(2));
      Assert.assertFalse(bitmap.contains(3));
      // bb
      Assert.assertFalse(bitmap.contains(4));
      Assert.assertTrue(bitmap.contains(5));
      Assert.assertFalse(bitmap.contains(6));
    } else {
      // aa
      Assert.assertFalse(bitmap.contains(0));
      Assert.assertFalse(bitmap.contains(1));
      Assert.assertTrue(bitmap.contains(2));
      Assert.assertFalse(bitmap.contains(3));
      // Confirm that both comparison column values have made it into the persisted upserted doc
      Assert.assertEquals(1567205397L, _mutableSegmentImpl.getValue(2, "secondsSinceEpoch"));
      Assert.assertEquals(1567205395L, _mutableSegmentImpl.getValue(2, "otherComparisonColumn"));
      Assert.assertFalse(_mutableSegmentImpl.getDataSource("secondsSinceEpoch").getNullValueVector().isNull(2));
      Assert.assertFalse(_mutableSegmentImpl.getDataSource("otherComparisonColumn").getNullValueVector().isNull(2));

      // bb
      Assert.assertFalse(bitmap.contains(4));
      Assert.assertTrue(bitmap.contains(5));
      Assert.assertFalse(bitmap.contains(6));
      // Confirm that comparison column values have made it into the persisted upserted doc
      Assert.assertEquals(1567205396L, _mutableSegmentImpl.getValue(5, "secondsSinceEpoch"));
      Assert.assertEquals(Long.MIN_VALUE, _mutableSegmentImpl.getValue(5, "otherComparisonColumn"));
      Assert.assertTrue(_mutableSegmentImpl.getDataSource("otherComparisonColumn").getNullValueVector().isNull(5));
      Assert.assertFalse(_mutableSegmentImpl.getDataSource("secondsSinceEpoch").getNullValueVector().isNull(5));
    }
  }
}
