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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.startree.v2.builder.MultipleTreesBuilder.BuildMode;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Builds star-trees over a segment whose metric is null for a whole group.
///
/// The group with no non-null input is the case a null-aware star-tree exists to represent, and it is the case the
/// regular star-tree cannot distinguish from one that genuinely aggregated the column's default null value.
///
/// The build mode is a parameter rather than a random choice, because the two builders represent an all-null group
/// differently: the on-heap one keeps the aggregated value in memory where `null` needs no encoding, while the
/// off-heap one serializes every record to a temporary store and has to carry the nullness beside it.
public class NullAwareStarTreeBuilderTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "NullAwareStarTreeBuilderTest");
  private static final String TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String DIMENSION = "d";
  private static final String METRIC = "m";

  /// One record per leaf, so every parent node has to aggregate rather than pass a single record through.
  private static final int MAX_LEAF_RECORDS = 1;

  /// `d = 0` holds only null metrics, `d = 1` holds only non-null ones. The default null value of an `INT` metric is
  /// `0`, which is below both real values, so a regular star-tree reports `0` as the minimum where a null-aware one
  /// reports `10`.
  private static final int[] DIMENSION_VALUES = {0, 0, 1, 1};
  private static final Integer[] METRIC_VALUES = {null, null, 10, 20};

  private static final String MIN_COLUMN =
      new AggregationFunctionColumnPair(AggregationFunctionType.MIN, METRIC).toColumnName();

  @AfterMethod
  public void cleanUp()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @DataProvider(name = "buildModes")
  public Object[][] buildModes() {
    return new Object[][]{{BuildMode.ON_HEAP}, {BuildMode.OFF_HEAP}};
  }

  /// A group with no non-null input has no aggregated value to store, and the off-heap builder used to have nowhere
  /// to put that: it serialized every metric unconditionally and dereferenced the missing value.
  @Test(dataProvider = "buildModes")
  public void anAllNullGroupSurvivesTheBuild(BuildMode buildMode)
      throws Exception {
    File indexDir = createSegment();
    buildStarTrees(indexDir, buildMode, starTreeConfig(true));

    ImmutableSegment segment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
    try {
      StarTreeV2 starTree = segment.getStarTrees().get(0);
      assertTrue(starTree.getMetadata().isNullHandlingEnabled());

      // The all-null group is recorded in the metric's null vector rather than as an aggregated value
      ImmutableRoaringBitmap nullBitmap = nullBitmap(starTree, MIN_COLUMN);
      assertNotNull(nullBitmap, "A null-aware star-tree must write a null vector for a metric with an all-null group");
      assertFalse(nullBitmap.isEmpty());

      // Every group that did aggregate something reports a real minimum, never the column's default null value
      List<Double> minimums = nonNullValues(starTree, MIN_COLUMN, nullBitmap);
      assertFalse(minimums.isEmpty(), "Some group must have aggregated a value, or the check below proves nothing");
      for (double minimum : minimums) {
        assertEquals(minimum, 10.0, "A null row must not be aggregated as the column default");
      }
    } finally {
      segment.destroy();
    }
  }

  /// The two variants answer differently, so a segment has to be able to hold both: the builder config's identity
  /// includes the flag, which is what stops one being reused for the other.
  @Test(dataProvider = "buildModes")
  public void bothVariantsCoexistAndDisagreeOnNulls(BuildMode buildMode)
      throws Exception {
    File indexDir = createSegment();
    buildStarTrees(indexDir, buildMode, starTreeConfig(false), starTreeConfig(true));

    ImmutableSegment segment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
    try {
      List<StarTreeV2> starTrees = segment.getStarTrees();
      assertEquals(starTrees.size(), 2, "The two variants differ only by the flag, and must not be deduplicated");

      StarTreeV2 regular = variant(starTrees, false);
      StarTreeV2 nullAware = variant(starTrees, true);

      // Same shape: the flag is the only difference between the two configs
      assertEquals(regular.getMetadata().getDimensionsSplitOrder(), nullAware.getMetadata().getDimensionsSplitOrder());
      assertEquals(regular.getMetadata().getFunctionColumnPairs(), nullAware.getMetadata().getFunctionColumnPairs());

      // The regular tree folds nulls into the column default, so it has no null vector and reports that default
      assertNull(nullBitmap(regular, MIN_COLUMN), "A regular star-tree does not write a null vector");
      assertTrue(nonNullValues(regular, MIN_COLUMN, null).contains(0.0),
          "A regular star-tree aggregates a null row as the column's default null value");

      // The null-aware tree excludes them, so the default never appears as a minimum
      ImmutableRoaringBitmap nullBitmap = nullBitmap(nullAware, MIN_COLUMN);
      assertNotNull(nullBitmap);
      assertFalse(nonNullValues(nullAware, MIN_COLUMN, nullBitmap).contains(0.0),
          "A null-aware star-tree excludes null rows from the pre-aggregation");
    } finally {
      segment.destroy();
    }
  }

  private static StarTreeV2 variant(List<StarTreeV2> starTrees, boolean nullHandlingEnabled) {
    for (StarTreeV2 starTree : starTrees) {
      if (starTree.getMetadata().isNullHandlingEnabled() == nullHandlingEnabled) {
        return starTree;
      }
    }
    throw new AssertionError("No star-tree with nullHandlingEnabled=" + nullHandlingEnabled);
  }

  private static ImmutableRoaringBitmap nullBitmap(StarTreeV2 starTree, String column) {
    NullValueVectorReader nullValueVector = starTree.getDataSource(column).getNullValueVector();
    return nullValueVector != null ? nullValueVector.getNullBitmap() : null;
  }

  /// Returns the pre-aggregated values of every doc the null vector does not mark, so that a placeholder left behind
  /// for a null group is never read as data.
  private static List<Double> nonNullValues(StarTreeV2 starTree, String column, ImmutableRoaringBitmap nullBitmap)
      throws IOException {
    List<Double> values = new ArrayList<>();
    ForwardIndexReader reader = starTree.getDataSource(column).getForwardIndex();
    assertNotNull(reader);
    try (ForwardIndexReaderContext context = reader.createContext()) {
      for (int docId = 0; docId < starTree.getMetadata().getNumDocs(); docId++) {
        if (nullBitmap == null || !nullBitmap.contains(docId)) {
          values.add(reader.getDouble(docId, context));
        }
      }
    }
    return values;
  }

  private static StarTreeIndexConfig starTreeConfig(boolean nullHandlingEnabled) {
    return new StarTreeIndexConfig(List.of(DIMENSION), null,
        List.of(new AggregationFunctionColumnPair(AggregationFunctionType.MIN, METRIC).toColumnName()), null,
        MAX_LEAF_RECORDS, nullHandlingEnabled);
  }

  private static void buildStarTrees(File indexDir, BuildMode buildMode, StarTreeIndexConfig... configs)
      throws Exception {
    try (MultipleTreesBuilder builder = new MultipleTreesBuilder(List.of(configs), false, indexDir, buildMode)) {
      builder.build();
    }
  }

  private static File createSegment()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(DIMENSION, DataType.INT)
        .addMetric(METRIC, DataType.INT)
        .build();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNullHandlingEnabled(true).build();

    List<GenericRow> rows = new ArrayList<>(DIMENSION_VALUES.length);
    for (int i = 0; i < DIMENSION_VALUES.length; i++) {
      GenericRow row = new GenericRow();
      row.putValue(DIMENSION, DIMENSION_VALUES[i]);
      if (METRIC_VALUES[i] != null) {
        row.putValue(METRIC, METRIC_VALUES[i]);
      } else {
        // Ingestion stores the column default and records the row in the null vector
        row.putDefaultNullValue(METRIC, schema.getFieldSpecFor(METRIC).getDefaultNullValue());
      }
      rows.add(row);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setOutDir(TEMP_DIR.getPath());
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(rows));
    driver.build();
    return new File(TEMP_DIR, SEGMENT_NAME);
  }
}
