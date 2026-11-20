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
package org.apache.pinot.segment.local.segment.index.loader.invertedindex;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.creator.impl.inv.RangeIndexCreator;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.RangeIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Tests [RangeIndexHandler] on a multi-value column configured with range index version 2.
///
/// The BitSliced (v2) creator only supports single-value columns, so a multi-value column always gets a v1 index.
/// The handler must treat that v1 index as up to date, otherwise every reload sees a "version change" and rebuilds it.
public class RangeIndexHandlerMultiValueTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), RangeIndexHandlerMultiValueTest.class.getSimpleName());
  private static final String SEGMENT_NAME = "testSegment";
  private static final String MV_COLUMN = "mvIntCol";
  private static final Map<String, FieldIndexConfigs> RANGE_V2_CONFIGS = Map.of(MV_COLUMN,
      new FieldIndexConfigs.Builder().add(StandardIndexes.range(), new RangeIndexConfig(2)).build());

  @Test
  public void testNoRebuildAfterReloadForMultiValueColumnWithVersion2()
      throws Exception {
    File indexDir = buildSegment();

    assertTrue(needUpdateIndices(indexDir), "Range index is absent on disk, so the handler is expected to build it");
    updateIndices(indexDir);

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, segmentMetadata, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      assertEquals(reader.getIndexFor(MV_COLUMN, StandardIndexes.range()).getInt(0), RangeIndexCreator.VERSION,
          "Multi-value columns are expected to get a v1 range index even when v2 is configured");
    }

    assertFalse(needUpdateIndices(indexDir), "The v1 index is what v2 config builds for a multi-value column, "
        + "so a second reload must not rebuild it");
  }

  private static boolean needUpdateIndices(File indexDir)
      throws Exception {
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, segmentMetadata, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      return new RangeIndexHandler(segmentDirectory, RANGE_V2_CONFIGS, createTableConfig(), createSchema())
          .needUpdateIndices(reader);
    }
  }

  private static void updateIndices(File indexDir)
      throws Exception {
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, segmentMetadata, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      new RangeIndexHandler(segmentDirectory, RANGE_V2_CONFIGS, createTableConfig(), createSchema())
          .updateIndices(writer);
      writer.save();
    }
  }

  private static Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addMultiValueDimension(MV_COLUMN, DataType.INT)
        .build();
  }

  private static TableConfig createTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
  }

  private static File buildSegment()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(createTableConfig(), createSchema());
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);

    List<GenericRow> rows = new ArrayList<>();
    int[][] values = {{3, 1}, {7}, {2, 9, 4}, {5, 5}};
    for (int[] docValues : values) {
      GenericRow row = new GenericRow();
      Object[] boxed = new Object[docValues.length];
      for (int i = 0; i < docValues.length; i++) {
        boxed[i] = docValues[i];
      }
      row.putValue(MV_COLUMN, boxed);
      rows.add(row);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return new File(TEMP_DIR, SEGMENT_NAME);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }
}
