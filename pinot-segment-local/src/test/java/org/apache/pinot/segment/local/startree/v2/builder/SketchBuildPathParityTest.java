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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.StarTreeAggregationConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Proves the star-tree {@code BaseSingleTreeBuilder} build path is correct under the new buffer
 * wiring. Builds two segments containing the same serialized theta sketch records and a star-tree
 * index over them — one with the BYTES source column compressed as {@code PASS_THROUGH} (which
 * triggers the zero-copy {@code getValueAsBuffer} path in {@code getSegmentRecord} +
 * {@code applyRawValueFromBuffer} dispatch in {@code mergeSegmentRecord}), the other with
 * {@code LZ4} (which falls back to {@code getValue} + {@code applyRawValue}). The pre-aggregated
 * star-tree theta metric must produce identical cardinality estimates regardless of which path
 * the builder took.
 */
public class SketchBuildPathParityTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "SketchBuildPathParityTest");
  private static final String TABLE_NAME = "testTable";
  private static final String DIM_COLUMN = "dim";
  private static final String THETA_COLUMN = "thetaColumn";
  private static final String FUNCTION_COLUMN_PAIR = "distinctCountThetaSketch__" + THETA_COLUMN;
  private static final int NUM_RECORDS = 1000;
  private static final int DIM_CARDINALITY = 100;
  private static final int DISTINCTS_PER_ROW = 3;

  private ImmutableSegment _passThroughSegment;
  private ImmutableSegment _lz4Segment;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(DIM_COLUMN, DataType.INT)
        .addMetric(THETA_COLUMN, DataType.BYTES)
        .build();

    UpdateSketchBuilder sketchBuilder = new UpdateSketchBuilder();
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      record.putValue(DIM_COLUMN, i % DIM_CARDINALITY);
      UpdateSketch theta = sketchBuilder.build();
      int base = i * DISTINCTS_PER_ROW;
      for (int d = 0; d < DISTINCTS_PER_ROW; d++) {
        theta.update(base + d);
      }
      record.putValue(THETA_COLUMN, theta.compact().toByteArray());
      records.add(record);
    }

    _passThroughSegment = buildSegmentWithStarTree("passThrough", FieldConfig.CompressionCodec.PASS_THROUGH,
        schema, records);
    _lz4Segment = buildSegmentWithStarTree("lz4", FieldConfig.CompressionCodec.LZ4, schema, records);
  }

  private ImmutableSegment buildSegmentWithStarTree(String segmentName, FieldConfig.CompressionCodec codec,
      Schema schema, List<GenericRow> records)
      throws Exception {
    // Per-column compression for the BYTES source column. The star-tree builder reads this column
    // through PinotSegmentColumnReader: for PASS_THROUGH the reader reports stable views and the
    // builder takes the new buffer path; for LZ4 the flag is false and the builder uses the
    // existing byte[] path.
    List<FieldConfig> fieldConfigs = Collections.singletonList(
        new FieldConfig(THETA_COLUMN, FieldConfig.EncodingType.RAW, Collections.emptyList(), codec, null));
    StarTreeIndexConfig starTreeIndexConfig = new StarTreeIndexConfig(
        Collections.singletonList(DIM_COLUMN), null, null,
        Collections.singletonList(new StarTreeAggregationConfig(THETA_COLUMN, "DISTINCTCOUNTTHETASKETCH", null,
            FieldConfig.CompressionCodec.PASS_THROUGH, true, null, null, null)),
        100);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Collections.singletonList(THETA_COLUMN))
        .setFieldConfigList(fieldConfigs)
        .setStarTreeIndexConfigs(Collections.singletonList(starTreeIndexConfig))
        .build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);
    config.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(records));
    driver.build();

    return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), ReadMode.mmap);
  }

  /**
   * Sanity check: the two segments report stability correctly. This confirms the parity assertions
   * below are actually comparing the buffer path against the byte[] path (not two byte[] runs).
   */
  @Test
  public void buildPathsAreDistinguishedByStabilityFlag() {
    assertTrue(_passThroughSegment.getDataSource(THETA_COLUMN).getForwardIndex().isBufferViewStableAcrossReads(),
        "PASS_THROUGH source should report stable views (build buffer path)");
    assertFalse(_lz4Segment.getDataSource(THETA_COLUMN).getForwardIndex().isBufferViewStableAcrossReads(),
        "LZ4 source should report unstable views (build byte[] path)");
  }

  /**
   * Core correctness assertion: the pre-aggregated star-tree theta metric produces byte-identical
   * cardinality estimates in both segments. Any divergence indicates the buffer dispatch is
   * producing different aggregated sketches than the byte[] path.
   */
  @Test
  public void starTreeBuildProducesIdenticalAggregatesForBothPaths() {
    List<StarTreeV2> passThroughTrees = _passThroughSegment.getStarTrees();
    List<StarTreeV2> lz4Trees = _lz4Segment.getStarTrees();
    assertNotNull(passThroughTrees, "PASS_THROUGH segment must have a star-tree");
    assertNotNull(lz4Trees, "LZ4 segment must have a star-tree");
    assertEquals(passThroughTrees.size(), 1);
    assertEquals(lz4Trees.size(), 1);

    StarTreeV2 passThroughTree = passThroughTrees.get(0);
    StarTreeV2 lz4Tree = lz4Trees.get(0);
    int passThroughDocs = passThroughTree.getMetadata().getNumDocs();
    int lz4Docs = lz4Tree.getMetadata().getNumDocs();
    assertEquals(passThroughDocs, lz4Docs, "star-trees must have the same number of aggregated docs");

    // Read each pre-aggregated theta sketch from both star-trees and compare.
    var passReader = passThroughTree.getDataSource(FUNCTION_COLUMN_PAIR).getForwardIndex();
    var lz4Reader = lz4Tree.getDataSource(FUNCTION_COLUMN_PAIR).getForwardIndex();
    try (var passContext = passReader.createContext(); var lz4Context = lz4Reader.createContext()) {
      for (int docId = 0; docId < passThroughDocs; docId++) {
        @SuppressWarnings("unchecked")
        byte[] passBytes = ((org.apache.pinot.segment.spi.index.reader.ForwardIndexReader) passReader)
            .getBytes(docId, passContext);
        @SuppressWarnings("unchecked")
        byte[] lz4Bytes = ((org.apache.pinot.segment.spi.index.reader.ForwardIndexReader) lz4Reader)
            .getBytes(docId, lz4Context);
        Sketch passSketch = Sketch.wrap(org.apache.datasketches.memory.Memory.wrap(passBytes));
        Sketch lz4Sketch = Sketch.wrap(org.apache.datasketches.memory.Memory.wrap(lz4Bytes));
        assertEquals(passSketch.getEstimate(), lz4Sketch.getEstimate(),
            "aggregated theta sketch estimate diverged at star-tree docId " + docId);
      }
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    if (_passThroughSegment != null) {
      _passThroughSegment.destroy();
    }
    if (_lz4Segment != null) {
      _lz4Segment.destroy();
    }
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
