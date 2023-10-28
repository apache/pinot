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
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManagerFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


public class MutableSegmentImplUpsertComparisonColTest {
  private static final String SCHEMA_FILE_PATH = "data/test_upsert_comparison_col_schema.json";
  private static final String DATA_FILE_PATH = "data/test_upsert_comparison_col_data.json";
  private static CompositeTransformer _recordTransformer;
  private static Schema _schema;
  private static TableConfig _tableConfig;
  private static MutableSegmentImpl _mutableSegmentImpl;
  private static PartitionUpsertMetadataManager _partitionUpsertMetadataManager;

  @BeforeClass
  public void setup()
      throws Exception {
    URL schemaResourceUrl = this.getClass().getClassLoader().getResource(SCHEMA_FILE_PATH);
    URL dataResourceUrl = this.getClass().getClassLoader().getResource(DATA_FILE_PATH);
    _schema = Schema.fromFile(new File(schemaResourceUrl.getFile()));
    UpsertConfig offsetUpsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    offsetUpsertConfig.setComparisonColumn("offset");
    _tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("testTable").setUpsertConfig(offsetUpsertConfig)
            .build();
    _recordTransformer = CompositeTransformer.getDefaultTransformer(_tableConfig, _schema);
    File jsonFile = new File(dataResourceUrl.getFile());
    TableUpsertMetadataManager tableUpsertMetadataManager = TableUpsertMetadataManagerFactory.create(_tableConfig);
    tableUpsertMetadataManager.init(_tableConfig, _schema, mock(TableDataManager.class), mock(ServerMetrics.class),
        mock(HelixManager.class), mock(ExecutorService.class));
    _partitionUpsertMetadataManager = tableUpsertMetadataManager.getOrCreatePartitionManager(0);
    _mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(_schema, Collections.emptySet(), Collections.emptySet(),
            Collections.emptySet(), false, true, offsetUpsertConfig, "secondsSinceEpoch",
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
  public void testUpsertIngestion() {
    ImmutableRoaringBitmap bitmap = _mutableSegmentImpl.getValidDocIds().getMutableRoaringBitmap();
    // note offset column is used for determining sequence but not time column
    Assert.assertFalse(bitmap.contains(0));
    Assert.assertTrue(bitmap.contains(1));
    Assert.assertTrue(bitmap.contains(2));
    Assert.assertFalse(bitmap.contains(3));
  }
}
