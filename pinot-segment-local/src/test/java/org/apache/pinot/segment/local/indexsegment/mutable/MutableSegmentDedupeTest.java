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
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.dedup.TableDedupMetadataManager;
import org.apache.pinot.segment.local.dedup.TableDedupMetadataManagerFactory;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.HashFunction;
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
  private static final String SCHEMA_FILE_PATH = "data/test_dedup_schema.json";
  private static final String DATA_FILE_PATH = "data/test_dedup_data.json";
  private MutableSegmentImpl _mutableSegmentImpl;

  private void setup(boolean dedupEnabled)
      throws Exception {
    URL schemaResourceUrl = this.getClass().getClassLoader().getResource(SCHEMA_FILE_PATH);
    URL dataResourceUrl = this.getClass().getClassLoader().getResource(DATA_FILE_PATH);
    Schema schema = Schema.fromFile(new File(schemaResourceUrl.getFile()));
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
        .setDedupConfig(new DedupConfig(dedupEnabled, HashFunction.NONE)).build();
    CompositeTransformer recordTransformer = CompositeTransformer.getDefaultTransformer(tableConfig, schema);
    File jsonFile = new File(dataResourceUrl.getFile());
    PartitionDedupMetadataManager partitionDedupMetadataManager =
        (dedupEnabled) ? getTableDedupMetadataManager(schema).getOrCreatePartitionManager(0) : null;
    _mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Collections.emptySet(), Collections.emptySet(),
            Collections.emptySet(), false, true, null, "secondsSinceEpoch", null, partitionDedupMetadataManager);
    GenericRow reuse = new GenericRow();
    try (RecordReader recordReader = RecordReaderFactory.getRecordReader(FileFormat.JSON, jsonFile,
        schema.getColumnNames(), null)) {
      while (recordReader.hasNext()) {
        recordReader.next(reuse);
        GenericRow transformedRow = recordTransformer.transform(reuse);
        _mutableSegmentImpl.index(transformedRow, null);
        reuse.clear();
      }
    }
  }

  private static TableDedupMetadataManager getTableDedupMetadataManager(Schema schema) {
    TableConfig tableConfig = Mockito.mock(TableConfig.class);
    Mockito.when(tableConfig.getTableName()).thenReturn("testTable_REALTIME");
    Mockito.when(tableConfig.getDedupConfig()).thenReturn(new DedupConfig(true, HashFunction.NONE));
    return TableDedupMetadataManagerFactory.create(tableConfig, schema, Mockito.mock(TableDataManager.class),
        Mockito.mock(ServerMetrics.class));
  }

  @Test
  public void testDedupeEnabled()
      throws Exception {
    setup(true);
    Assert.assertEquals(_mutableSegmentImpl.getNumDocsIndexed(), 2);
  }

  @Test
  public void testDedupeDisabled()
      throws Exception {
    setup(false);
    Assert.assertEquals(_mutableSegmentImpl.getNumDocsIndexed(), 4);
  }
}
