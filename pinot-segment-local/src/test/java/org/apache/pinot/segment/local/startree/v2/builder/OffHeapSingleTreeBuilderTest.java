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
package org.apache.pinot.segment.local.startree.v2.builder;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.startree.StarTreeBuilderUtils;
import org.apache.pinot.segment.local.startree.v2.builder.OffHeapSingleTreeBuilder.FixedSizeRecordOffsets;
import org.apache.pinot.segment.local.startree.v2.builder.OffHeapSingleTreeBuilder.RecordOffsets;
import org.apache.pinot.segment.local.startree.v2.builder.OffHeapSingleTreeBuilder.VariableSizeRecordOffsets;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


public class OffHeapSingleTreeBuilderTest {

  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "OffHeapSingleTreeBuilderTest");
  private static final File INDEX_DIR = new File(TEMP_DIR, "testSegment");
  private static final String SEGMENT_RECORD_FILE_NAME = "segment.record";

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    FileUtils.forceMkdir(TEMP_DIR);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @Test
  public void testFixedSizeRecordOffsets() {
    RecordOffsets offsets = new FixedSizeRecordOffsets(1 << 30);
    for (int i = 0; i < 4; i++) {
      offsets.addRecord(1 << 30);
    }
    assertEquals(offsets.getStartOffset(0), 0L);
    assertEquals(offsets.getStartOffset(1), 1L << 30);
    assertEquals(offsets.getStartOffset(3), 3L << 30);
    assertEquals(offsets.getEndOffset(), 1L << 32);
  }

  @Test
  public void testVariableSizeRecordOffsets() {
    RecordOffsets offsets = new VariableSizeRecordOffsets();
    offsets.addRecord(123);
    offsets.addRecord(Integer.MAX_VALUE - 123);
    offsets.addRecord(456);
    offsets.addRecord(789);
    assertEquals(offsets.getStartOffset(0), 0L);
    assertEquals(offsets.getStartOffset(1), 123L);
    assertEquals(offsets.getStartOffset(2), Integer.MAX_VALUE);
    assertEquals(offsets.getStartOffset(3), Integer.MAX_VALUE + 456L);
    assertEquals(offsets.getEndOffset(), Integer.MAX_VALUE + 456L + 789L);
  }

  /// Builds a star-tree with the off-heap builder and asserts the intermediate segment record file
  /// is cleaned up after build. Exercises the full sortAndAggregateSegmentRecords → iterator → close
  /// lifecycle, including buffer allocation, sequential fill in Sub-phase A, sort in Sub-phase B,
  /// and dim-from-buffer reads in Sub-phase C.
  @Test
  public void testBuildCleansUpSegmentRecordFile()
      throws Exception {
    buildTestSegment();

    List<StarTreeV2BuilderConfig> builderConfigs = createBuilderConfigs();
    File segmentDir = INDEX_DIR.listFiles()[0];

    try (MultipleTreesBuilder builder = new MultipleTreesBuilder(builderConfigs, segmentDir,
        MultipleTreesBuilder.BuildMode.OFF_HEAP)) {
      builder.build();
    }

    // OffHeapSingleTreeBuilder writes an intermediate segment.record file during Sub-phase A/B/C.
    // The dim-buffer optimization keeps that file alive through iterator exhaustion; close() must
    // release the buffer and delete the file.
    File segmentRecordFile = findSegmentRecordFile(segmentDir);
    assertFalse(segmentRecordFile != null && segmentRecordFile.exists(),
        "segment.record file should be deleted after build: " + segmentRecordFile);
  }

  /// close() before build() must not throw and must leave no leftover segment record file.
  @Test
  public void testCloseWithoutBuildDoesNotThrow()
      throws Exception {
    buildTestSegment();

    List<StarTreeV2BuilderConfig> builderConfigs = createBuilderConfigs();
    File segmentDir = INDEX_DIR.listFiles()[0];

    // Construct and immediately close — no build().
    MultipleTreesBuilder builder = new MultipleTreesBuilder(builderConfigs, segmentDir,
        MultipleTreesBuilder.BuildMode.OFF_HEAP);
    builder.close();

    File segmentRecordFile = findSegmentRecordFile(segmentDir);
    assertFalse(segmentRecordFile != null && segmentRecordFile.exists(),
        "segment.record file should not exist when build() was never called: " + segmentRecordFile);
  }

  /// Builds the same segment twice — once with OFF_HEAP and once with ON_HEAP — then compares the
  /// resulting star-trees by grouping star-tree records by dim tuple and summing the aggregate
  /// column. Directly regressions-tests dim-read parity: if the buffer read returned wrong bytes,
  /// OFF_HEAP would aggregate metrics under the wrong dim tuples and the maps would diverge.
  @Test
  public void testOffHeapProducesSameStarTreeAsOnHeap()
      throws Exception {
    buildTestSegment();
    File sourceSegmentDir = INDEX_DIR.listFiles()[0];
    List<StarTreeV2BuilderConfig> builderConfigs = createBuilderConfigs();

    File offHeapDir = new File(TEMP_DIR, "offHeapCopy");
    File onHeapDir = new File(TEMP_DIR, "onHeapCopy");
    FileUtils.copyDirectory(sourceSegmentDir, offHeapDir);
    FileUtils.copyDirectory(sourceSegmentDir, onHeapDir);

    try (MultipleTreesBuilder builder = new MultipleTreesBuilder(builderConfigs, offHeapDir,
        MultipleTreesBuilder.BuildMode.OFF_HEAP)) {
      builder.build();
    }
    try (MultipleTreesBuilder builder = new MultipleTreesBuilder(builderConfigs, onHeapDir,
        MultipleTreesBuilder.BuildMode.ON_HEAP)) {
      builder.build();
    }

    Map<String, Long> offHeapByDim = readStarTreeAggregatedByDim(offHeapDir);
    Map<String, Long> onHeapByDim = readStarTreeAggregatedByDim(onHeapDir);
    assertEquals(offHeapByDim, onHeapByDim,
        "OFF_HEAP and ON_HEAP star-trees disagree on aggregate-by-dim: "
            + "off=" + offHeapByDim + ", on=" + onHeapByDim);
  }

  /// Iterates every star-tree record, resolves dim dict-ids to values, and returns a
  /// `(stringCol|intCol) -> sum(longCol)` map. Star nodes (dim value = STAR) fold into their own
  /// bucket, so the map is a canonical view of the tree independent of internal doc order.
  private Map<String, Long> readStarTreeAggregatedByDim(File segmentDir)
      throws Exception {
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap);
    try {
      StarTreeV2 tree = segment.getStarTrees().get(0);
      int numDocs = tree.getMetadata().getNumDocs();

      ForwardIndexReader<ForwardIndexReaderContext> stringReader =
          (ForwardIndexReader<ForwardIndexReaderContext>) tree.getDataSource("stringCol").getForwardIndex();
      Dictionary stringDict = tree.getDataSource("stringCol").getDictionary();
      ForwardIndexReader<ForwardIndexReaderContext> intReader =
          (ForwardIndexReader<ForwardIndexReaderContext>) tree.getDataSource("intCol").getForwardIndex();
      Dictionary intDict = tree.getDataSource("intCol").getDictionary();
      // Aggregate columns are keyed by AggregationFunctionColumnPair.toColumnName() ("sum__longCol").
      ForwardIndexReader<ForwardIndexReaderContext> aggReader =
          (ForwardIndexReader<ForwardIndexReaderContext>) tree.getDataSource("sum__longCol").getForwardIndex();

      Map<String, Long> byDim = new TreeMap<>();
      try (ForwardIndexReaderContext stringCtx = stringReader.createContext();
          ForwardIndexReaderContext intCtx = intReader.createContext();
          ForwardIndexReaderContext aggCtx = aggReader.createContext()) {
        for (int docId = 0; docId < numDocs; docId++) {
          int stringDictId = stringReader.getDictId(docId, stringCtx);
          int intDictId = intReader.getDictId(docId, intCtx);
          String stringVal = stringDictId == -1 ? "*" : stringDict.getStringValue(stringDictId);
          String intVal = intDictId == -1 ? "*" : String.valueOf(intDict.getIntValue(intDictId));
          byDim.merge(stringVal + "|" + intVal, aggReader.getLong(docId, aggCtx), Long::sum);
        }
      }
      return byDim;
    } finally {
      segment.destroy();
    }
  }

  private void buildTestSegment()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("stringCol", FieldSpec.DataType.STRING)
        .addSingleValueDimension("intCol", FieldSpec.DataType.INT)
        .addMetric("longCol", FieldSpec.DataType.LONG)
        .build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName("testSegment");

    // Produce a few rows with distinct dim tuples so Sub-phase C's sortedDocIds order genuinely
    // differs from segment docId order — exercises the "reads dims from buffer under sorted docId
    // access" path.
    List<GenericRow> rows = Arrays.asList(
        createRow("A", 1, 10L),
        createRow("B", 2, 20L),
        createRow("A", 2, 30L),
        createRow("B", 1, 40L),
        createRow("C", 3, 50L)
    );

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
  }

  private GenericRow createRow(String stringValue, int intValue, long longValue) {
    GenericRow row = new GenericRow();
    row.putValue("stringCol", stringValue);
    row.putValue("intCol", intValue);
    row.putValue("longCol", longValue);
    return row;
  }

  private List<StarTreeV2BuilderConfig> createBuilderConfigs()
      throws Exception {
    StarTreeIndexConfig starTreeConfig = new StarTreeIndexConfig(
        Arrays.asList("stringCol", "intCol"),
        null,
        Arrays.asList("SUM__longCol"),
        null,
        1000);

    File segmentDir = INDEX_DIR.listFiles()[0];
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap);
    try {
      return StarTreeBuilderUtils.generateBuilderConfigs(
          Arrays.asList(starTreeConfig),
          false,
          segment.getSegmentMetadata());
    } finally {
      segment.destroy();
    }
  }

  private File findSegmentRecordFile(File segmentDir) {
    // segment.record lives under the star-tree output directory created by MultipleTreesBuilder;
    // walk the segment dir tree to locate any leftover.
    return findByName(segmentDir, SEGMENT_RECORD_FILE_NAME);
  }

  private File findByName(File dir, String name) {
    if (!dir.isDirectory()) {
      return null;
    }
    File[] children = dir.listFiles();
    if (children == null) {
      return null;
    }
    for (File child : children) {
      if (child.getName().equals(name)) {
        return child;
      }
      if (child.isDirectory()) {
        File found = findByName(child, name);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }
}
