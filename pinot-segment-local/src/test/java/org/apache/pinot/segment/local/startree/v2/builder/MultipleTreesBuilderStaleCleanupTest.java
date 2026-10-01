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
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.startree.StarTreeBuilderUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;
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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Unit test for {@link MultipleTreesBuilder#build()} verifying that a stale on-disk `star_tree_index`
/// (leftover from a previous build attempt that was killed before completing) is cleaned up on the next
/// build instead of triggering the `Star-tree index file already exists` IllegalStateException from
/// {@link StarTreeIndexCombiner}.
public class MultipleTreesBuilderStaleCleanupTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), "MultipleTreesBuilderStaleCleanupTest");
  private static final File INDEX_DIR = new File(TEMP_DIR, "testSegment");

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    FileUtils.forceMkdir(TEMP_DIR);
    buildBaseSegment();
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  /// A previous build that was hard-killed (JVM crash / SIGKILL / OOM) leaves behind a partial
  /// `star_tree_index` at the segment root without a matching `STAR_TREE_COUNT` in segment metadata.
  /// The next build must clean it up and succeed rather than fail with `Star-tree index file already exists`.
  @Test
  public void staleIndexFileFromKilledBuildIsCleanedUp()
      throws Exception {
    File segmentDir = INDEX_DIR.listFiles()[0];
    File v3Dir = findV3Dir(segmentDir);
    File staleIndex = new File(v3Dir, StarTreeV2Constants.INDEX_FILE_NAME);
    File staleIndexMap = new File(v3Dir, StarTreeV2Constants.INDEX_MAP_FILE_NAME);
    // Simulate the killed-build leftover: a file at the segment root that segment metadata does not know about.
    FileUtils.writeStringToFile(staleIndex, "stale bytes from a prior killed build", StandardCharsets.UTF_8);
    assertTrue(staleIndex.exists());

    // Kick off a fresh build. Without the fix this throws `IllegalStateException: Star-tree index file already exists`.
    List<StarTreeV2BuilderConfig> builderConfigs = createBuilderConfigs(segmentDir);
    try (MultipleTreesBuilder builder = new MultipleTreesBuilder(builderConfigs, segmentDir,
        MultipleTreesBuilder.BuildMode.OFF_HEAP)) {
      builder.build();
    }

    // Post-build: the stale bytes are gone, a real index file and map file exist.
    assertTrue(staleIndex.isFile(), "star_tree_index should exist after a successful build");
    assertTrue(staleIndexMap.isFile(), "star_tree_index_map should exist after a successful build");
    assertTrue(staleIndex.length() > "stale bytes from a prior killed build".length(),
        "star_tree_index should be a real serialized tree, not the stale contents");
  }

  /// A lingering EXISTING_STAR_TREE_TEMP_DIR from a killed incremental build (previous run set files aside
  /// but never restored them) is also cleaned up on the next fresh build.
  @Test
  public void staleSeparatorTempDirFromKilledIncrementalIsCleanedUp()
      throws Exception {
    File segmentDir = INDEX_DIR.listFiles()[0];
    File v3Dir = findV3Dir(segmentDir);
    File staleSeparatorDir = new File(v3Dir, StarTreeV2Constants.EXISTING_STAR_TREE_TEMP_DIR);
    FileUtils.forceMkdir(staleSeparatorDir);
    FileUtils.writeStringToFile(new File(staleSeparatorDir, "leftover.bin"), "old", StandardCharsets.UTF_8);
    assertTrue(staleSeparatorDir.isDirectory());

    List<StarTreeV2BuilderConfig> builderConfigs = createBuilderConfigs(segmentDir);
    try (MultipleTreesBuilder builder = new MultipleTreesBuilder(builderConfigs, segmentDir,
        MultipleTreesBuilder.BuildMode.OFF_HEAP)) {
      builder.build();
    }

    // The temp dir has been cleaned up (either by our pre-build cleanup, or by the normal build flow).
    assertFalse(staleSeparatorDir.exists(),
        "stale EXISTING_STAR_TREE_TEMP_DIR should have been removed by the fresh build");
  }

  private void buildBaseSegment()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("stringCol", FieldSpec.DataType.STRING)
        .addMetric("longCol", FieldSpec.DataType.LONG)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName("testSegment");
    List<GenericRow> rows = List.of(makeRow("A", 1L), makeRow("B", 2L), makeRow("C", 3L));
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
  }

  private GenericRow makeRow(String s, long l) {
    GenericRow row = new GenericRow();
    row.putValue("stringCol", s);
    row.putValue("longCol", l);
    return row;
  }

  private List<StarTreeV2BuilderConfig> createBuilderConfigs(File segmentDir)
      throws Exception {
    StarTreeIndexConfig starTreeConfig =
        new StarTreeIndexConfig(List.of("stringCol"), null, List.of("SUM__longCol"), null, 1000);
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap);
    try {
      return StarTreeBuilderUtils.generateBuilderConfigs(List.of(starTreeConfig), false,
          segment.getSegmentMetadata());
    } finally {
      segment.destroy();
    }
  }

  private static File findV3Dir(File segmentDir) {
    File v3 = new File(segmentDir, "v3");
    return v3.isDirectory() ? v3 : segmentDir;
  }
}
