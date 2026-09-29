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
package org.apache.pinot.segment.local.segment.index.openstruct;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.OpenStructNaming;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// A key's index settings must be applied by a reload, not only when the segment was written.
///
/// Per-key indexes used to be built solely by `OpenStructColumnSplitter` while it wrote the child columns, so they
/// were frozen at creation: adding a range index to a key and reloading did nothing, while the same change on an
/// ordinary column has always worked.
public class OpenStructPerKeyIndexReloadTest {
  private static final File TMP_DIR =
      new File(FileUtils.getTempDirectory(), OpenStructPerKeyIndexReloadTest.class.getSimpleName());
  private static final String TABLE = "openStructPerKeyIndex";
  private static final String COLUMN = "props";
  private static final String KEY = "region";
  private static final String CHILD = OpenStructNaming.materializedColumnName(COLUMN, KEY);
  private static final String ORDINARY_COLUMN = "id";
  private static final int NUM_DOCS = 64;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TMP_DIR);
    FileUtils.forceMkdir(TMP_DIR);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TMP_DIR);
  }

  /// The regression: build with the key un-indexed, then reload with an inverted index configured for it.
  @Test
  public void testReloadBuildsAPerKeyIndexThatWasNotThereAtCreation()
      throws Exception {
    File segmentDir = buildSegment("reloadAddsIndex", withoutPerKeyIndex());

    assertFalse(hasIndex(segmentDir, CHILD, StandardIndexes.inverted()),
        "precondition: the key must start without an inverted index");

    reload(segmentDir, withPerKeyInvertedIndex());

    assertTrue(hasIndex(segmentDir, CHILD, StandardIndexes.inverted()),
        "a reload must apply the key's index settings, the way it does for any other column");
  }

  /// A segment whose keys already carry what the config asks for must need no reprocessing, and a reload of one
  /// must leave the indexes alone -- otherwise every reload rewrites indexes that are already correct.
  @Test
  public void testReloadIsANoOpWhenTheKeyIndexIsAlreadyPresent()
      throws Exception {
    File segmentDir = buildSegment("reloadNoOp", withPerKeyInvertedIndex());
    assertTrue(hasIndex(segmentDir, CHILD, StandardIndexes.inverted()),
        "precondition: creation already built the index");

    try (SegmentDirectory directory = openDirectory(segmentDir);
        SegmentPreProcessor preProcessor =
            new SegmentPreProcessor(directory, indexLoadingConfig(withPerKeyInvertedIndex()))) {
      assertFalse(preProcessor.needProcess(), "a key whose index already matches needs no reprocessing");
    }

    reload(segmentDir, withPerKeyInvertedIndex());
    assertTrue(hasIndex(segmentDir, CHILD, StandardIndexes.inverted()), "and the index must survive a reload");
  }

  /// The sparse blob holds every unmaterialized key in one column, so a per-key setting cannot mean anything for
  /// it. It must be left alone rather than indexed as if it were a key.
  @Test
  public void testSparseBlobColumnIsNotTreatedAsAKey()
      throws Exception {
    File segmentDir = buildSegment("sparseUntouched", withPerKeyInvertedIndex());
    String sparse = OpenStructNaming.sparseColumnName(COLUMN);
    reload(segmentDir, withPerKeyInvertedIndex());

    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);
    if (metadata.getColumnMetadataMap().containsKey(sparse)) {
      assertFalse(hasIndex(segmentDir, sparse, StandardIndexes.inverted()),
          "the shared blob column must not get a per-key inverted index");
    }
  }

  /// An ordinary column's indexes must survive a reload of a table that also has OPEN_STRUCT keys.
  @Test
  public void testReloadKeepsIndexesOnOrdinaryColumns()
      throws Exception {
    File segmentDir = buildSegment("ordinaryUntouched", withPerKeyInvertedIndex());
    assertTrue(hasIndex(segmentDir, ORDINARY_COLUMN, StandardIndexes.inverted()),
        "precondition: creation built the ordinary column's inverted index");

    reload(segmentDir, withPerKeyInvertedIndex());

    assertTrue(hasIndex(segmentDir, ORDINARY_COLUMN, StandardIndexes.inverted()),
        "an ordinary column's inverted index must survive the reload");
  }

  // ---------------------------------------------------------------- fixtures

  private static OpenStructIndexConfig withoutPerKeyIndex() {
    // Every key raw and un-indexed, so the child starts with nothing for the reload to find.
    FieldConfig raw = new FieldConfig.Builder("default").withEncodingType(FieldConfig.EncodingType.RAW).build();
    // First arg is `disabled`, not `enabled`.
    return new OpenStructIndexConfig(false, raw, -1, null, 0.0, null);
  }

  private static OpenStructIndexConfig withPerKeyInvertedIndex() {
    FieldConfig raw = new FieldConfig.Builder("default").withEncodingType(FieldConfig.EncodingType.RAW).build();
    ObjectNode inverted = JsonUtils.newObjectNode();
    inverted.set("inverted", JsonUtils.newObjectNode());
    FieldConfig keyConfig = new FieldConfig.Builder(KEY)
        .withEncodingType(FieldConfig.EncodingType.DICTIONARY)
        .withIndexes(inverted)
        .build();
    return new OpenStructIndexConfig(false, raw, -1, Set.of(KEY), 0.0, List.of(keyConfig));
  }

  private static Schema schema() {
    return new Schema.SchemaBuilder().setSchemaName(TABLE)
        .addField(new ComplexFieldSpec(COLUMN, FieldSpec.DataType.OPEN_STRUCT, true, Map.of()))
        .addSingleValueDimension(ORDINARY_COLUMN, FieldSpec.DataType.STRING)
        .build();
  }

  private static TableConfig tableConfig(OpenStructIndexConfig osConfig) {
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("open_struct", JsonUtils.objectToJsonNode(osConfig));
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE)
        .setFieldConfigList(List.of(new FieldConfig.Builder(COLUMN).withIndexes(indexes).build()))
        .setInvertedIndexColumns(List.of(ORDINARY_COLUMN))
        .setNullHandlingEnabled(true)
        .build();
  }

  private static File buildSegment(String segmentName, OpenStructIndexConfig osConfig)
      throws Exception {
    List<GenericRow> rows = new ArrayList<>(NUM_DOCS);
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      Map<String, Object> props = new HashMap<>();
      props.put(KEY, docId % 2 == 0 ? "us" : "eu");
      GenericRow row = new GenericRow();
      row.putValue(COLUMN, props);
      row.putValue(ORDINARY_COLUMN, "id-" + (docId % 8));
      rows.add(row);
    }
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig(osConfig), schema());
    config.setOutDir(new File(TMP_DIR, segmentName).getAbsolutePath());
    config.setSegmentName(segmentName);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return driver.getOutputDirectory();
  }

  private static IndexLoadingConfig indexLoadingConfig(OpenStructIndexConfig osConfig) {
    return new IndexLoadingConfig(tableConfig(osConfig), schema());
  }

  private static SegmentDirectory openDirectory(File segmentDir)
      throws Exception {
    return new SegmentLocalFSDirectory(segmentDir, new SegmentMetadataImpl(segmentDir), ReadMode.mmap);
  }

  private static void reload(File segmentDir, OpenStructIndexConfig osConfig)
      throws Exception {
    try (SegmentDirectory directory = openDirectory(segmentDir);
        SegmentPreProcessor preProcessor = new SegmentPreProcessor(directory, indexLoadingConfig(osConfig))) {
      preProcessor.process(null);
    }
  }

  private static boolean hasIndex(File segmentDir, String column, IndexType<?, ?, ?> indexType)
      throws Exception {
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);
    ColumnMetadata columnMetadata = metadata.getColumnMetadataFor(column);
    if (columnMetadata == null) {
      return false;
    }
    try (SegmentDirectory directory =
        new SegmentLocalFSDirectory(segmentDir, metadata, ReadMode.mmap);
        SegmentDirectory.Reader reader = directory.createReader()) {
      return reader.hasIndexFor(column, indexType);
    }
  }

  static {
    // Keep the V1 constants class loaded so segment metadata reads resolve the same way the loader does.
    assert V1Constants.MetadataKeys.Column.PARENT_COLUMN != null;
  }
}
