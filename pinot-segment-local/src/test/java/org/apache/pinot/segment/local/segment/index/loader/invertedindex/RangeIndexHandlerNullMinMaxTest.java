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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.readers.BitSlicedRangeIndexReader;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.RangeIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Tests [RangeIndexHandler] building a BitSliced (v2) range index on a no-dictionary column whose metadata carries
/// no min/max value.
///
/// Ingestion-aggregated metric columns are forced no-dictionary and skip min/max tracking while consuming, so
/// segments committed before that domain is recovered at seal time report null min/max. The BitSliced creator
/// subtracts the column min for INT/LONG, so without recovery it dereferences a null and the load fails. The handler
/// recovers the domain by scanning the forward index; these tests pin that the index is built and answers correctly.
///
/// The segment is built normally and then stripped of its min/max metadata, which is what such a segment looks like
/// on disk.
public class RangeIndexHandlerNullMinMaxTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), RangeIndexHandlerNullMinMaxTest.class.getSimpleName());
  private static final String SEGMENT_NAME = "testSegment";
  private static final String INT_COLUMN = "intCol";
  private static final String LONG_COLUMN = "longCol";

  // Deliberately unsorted, and negative so the subtract-min in the creator is actually exercised.
  private static final int[] INT_VALUES = {40, -10, 25, -10, 100};
  private static final long[] LONG_VALUES = {4000L, -1000L, 2500L, -1000L, 10000L};

  @Test
  public void testBuildsRangeIndexWhenMetadataMinMaxMissing()
      throws Exception {
    File indexDir = buildSegmentWithoutMinMax();

    updateRangeIndices(indexDir);

    SegmentMetadataImpl reloaded = new SegmentMetadataImpl(indexDir);
    // The recovery happens inside the handler and is not written back to metadata, so the column still reports null.
    assertNull(reloaded.getColumnMetadataFor(INT_COLUMN).getMinValue());
    assertNull(reloaded.getColumnMetadataFor(INT_COLUMN).getMaxValue());

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, reloaded, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      assertNotNull(reader);

      PinotDataBuffer intBuffer = reader.getIndexFor(INT_COLUMN, StandardIndexes.range());
      assertNotNull(intBuffer, "Range index expected to be built for the INT column");
      ColumnMetadata intMetadata = reloaded.getColumnMetadataFor(INT_COLUMN);
      BitSlicedRangeIndexReader intReader = new BitSlicedRangeIndexReader(intBuffer, intMetadata);

      // Range fully inside the recovered domain.
      assertDocIds(intReader.getMatchingDocIds(-10, 25), 1, 2, 3);
      // Single point at the recovered minimum.
      assertDocIds(intReader.getMatchingDocIds(-10, -10), 1, 3);
      // Range covering the whole column.
      assertDocIds(intReader.getMatchingDocIds(-10, 100), 0, 1, 2, 3, 4);
      // Range entirely below the recovered minimum matches nothing.
      assertDocIds(intReader.getMatchingDocIds(-100, -50));

      PinotDataBuffer longBuffer = reader.getIndexFor(LONG_COLUMN, StandardIndexes.range());
      assertNotNull(longBuffer, "Range index expected to be built for the LONG column");
      BitSlicedRangeIndexReader longReader =
          new BitSlicedRangeIndexReader(longBuffer, reloaded.getColumnMetadataFor(LONG_COLUMN));

      assertDocIds(longReader.getMatchingDocIds(-1000L, 2500L), 1, 2, 3);
      assertDocIds(longReader.getMatchingDocIds(2500L, 10000L), 0, 2, 4);
      assertDocIds(longReader.getMatchingDocIds(-5000L, -2000L));
    }
  }

  @Test
  public void testOpenEndedQueryIsCorrectWithoutMetadataMax()
      throws Exception {
    // The reader takes the column max from metadata, which is still null here, so it falls back to Long.MAX_VALUE.
    // The RangeBitmap domain is self-contained, so results must stay correct even though max pruning is weaker.
    File indexDir = buildSegmentWithoutMinMax();

    updateRangeIndices(indexDir);

    SegmentMetadataImpl reloaded = new SegmentMetadataImpl(indexDir);
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, reloaded, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      BitSlicedRangeIndexReader intReader =
          new BitSlicedRangeIndexReader(reader.getIndexFor(INT_COLUMN, StandardIndexes.range()),
              reloaded.getColumnMetadataFor(INT_COLUMN));

      // Upper bound far beyond the real max: every doc at or above the lower bound must still match.
      assertDocIds(intReader.getMatchingDocIds(25, Integer.MAX_VALUE), 0, 2, 4);
      assertDocIds(intReader.getMatchingDocIds(Integer.MIN_VALUE, Integer.MAX_VALUE), 0, 1, 2, 3, 4);
    }
  }

  private static void assertDocIds(ImmutableRoaringBitmap actual, int... expected) {
    assertNotNull(actual);
    int[] actualDocIds = actual.toArray();
    assertEquals(actualDocIds.length, expected.length,
        "Unexpected match count, got " + Arrays.toString(actualDocIds));
    for (int i = 0; i < expected.length; i++) {
      assertEquals(actualDocIds[i], expected[i]);
    }
  }

  /// Runs the handler over both columns with a v2 range index configured, which is the path that needs the value
  /// domain up front.
  private static void updateRangeIndices(File indexDir)
      throws Exception {
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    FieldIndexConfigs rangeV2 =
        new FieldIndexConfigs.Builder().add(StandardIndexes.range(), new RangeIndexConfig(2)).build();
    Map<String, FieldIndexConfigs> configs = Map.of(INT_COLUMN, rangeV2, LONG_COLUMN, rangeV2);
    // A directory hands out either a reader or a writer, not both, so check and build in separate opens.
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, segmentMetadata, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      RangeIndexHandler handler =
          new RangeIndexHandler(segmentDirectory, configs, createTableConfig(), createSchema());
      assertTrue(handler.needUpdateIndices(reader),
          "Range index is absent on disk, so the handler is expected to build it");
    }
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, segmentMetadata, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      RangeIndexHandler handler =
          new RangeIndexHandler(segmentDirectory, configs, createTableConfig(), createSchema());
      handler.updateIndices(writer);
    }
  }

  private static Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addSingleValueDimension(INT_COLUMN, DataType.INT)
        .addSingleValueDimension(LONG_COLUMN, DataType.LONG)
        .build();
  }

  private static TableConfig createTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setNoDictionaryColumns(List.of(INT_COLUMN, LONG_COLUMN))
        .build();
  }

  private static File buildSegmentWithoutMinMax()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(createTableConfig(), createSchema());
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(createRows()));
    driver.build();

    File indexDir = new File(TEMP_DIR, SEGMENT_NAME);
    removeMinMaxValuesFromMetadataFile(indexDir);
    return indexDir;
  }

  private static List<GenericRow> createRows() {
    List<GenericRow> rows = new ArrayList<>();
    for (int i = 0; i < INT_VALUES.length; i++) {
      GenericRow row = new GenericRow();
      row.putValue(INT_COLUMN, INT_VALUES[i]);
      row.putValue(LONG_COLUMN, LONG_VALUES[i]);
      rows.add(row);
    }
    return rows;
  }

  /// Strips min/max from the segment metadata, reproducing what a segment committed with untracked min/max looks
  /// like on disk.
  private static void removeMinMaxValuesFromMetadataFile(File indexDir)
      throws Exception {
    PropertiesConfiguration configuration = SegmentMetadataUtils.getPropertiesConfiguration(indexDir);
    Iterator<String> keys = configuration.getKeys();
    List<String> keysToClear = new ArrayList<>();
    while (keys.hasNext()) {
      String key = keys.next();
      if (key.endsWith(V1Constants.MetadataKeys.Column.MIN_VALUE)
          || key.endsWith(V1Constants.MetadataKeys.Column.MAX_VALUE)
          || key.endsWith(V1Constants.MetadataKeys.Column.MIN_MAX_VALUE_INVALID)) {
        keysToClear.add(key);
      }
    }
    for (String key : keysToClear) {
      configuration.clearProperty(key);
    }
    SegmentMetadataUtils.savePropertiesConfiguration(configuration, indexDir);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }
}
