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
import org.apache.pinot.spi.data.DimensionFieldSpec;
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

  /// The per-key index matrix the end-to-end ingestion test uses: one key inverted, one dictionary-only, one raw.
  /// A reload must leave all three as configured -- a raw key in particular must not be dragged into a dictionary.
  @Test
  public void testReloadOfAMixedPerKeyIndexMatrix()
      throws Exception {
    File segmentDir = buildSegment("mixedMatrix", withMixedMatrix());
    reload(segmentDir, withMixedMatrix());

    String views = OpenStructNaming.materializedColumnName(COLUMN, "views");
    String cpu = OpenStructNaming.materializedColumnName(COLUMN, "cpu");
    String host = OpenStructNaming.materializedColumnName(COLUMN, "host");
    assertTrue(hasIndex(segmentDir, views, StandardIndexes.inverted()), "the inverted key keeps its index");
    assertTrue(hasIndex(segmentDir, cpu, StandardIndexes.dictionary()), "the dictionary key keeps its dictionary");
    assertFalse(hasIndex(segmentDir, host, StandardIndexes.dictionary()), "the raw key must stay raw");
  }

  private static List<FieldConfig> mixedMatrixKeys() {
    FieldConfig views = new FieldConfig.Builder("views")
        .withIndexes(JsonUtils.objectToJsonNode(Map.of("inverted", Map.of())))
        .build();
    FieldConfig cpu = new FieldConfig.Builder("cpu")
        .withEncodingType(FieldConfig.EncodingType.DICTIONARY)
        .build();
    FieldConfig host = new FieldConfig.Builder("host")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .build();
    return List.of(views, cpu, host);
  }

  private static OpenStructIndexConfig withMixedMatrix() {
    FieldConfig views = new FieldConfig.Builder("views")
        .withIndexes(JsonUtils.objectToJsonNode(Map.of("inverted", Map.of())))
        .build();
    FieldConfig cpu = new FieldConfig.Builder("cpu")
        .withEncodingType(FieldConfig.EncodingType.DICTIONARY)
        .build();
    FieldConfig host = new FieldConfig.Builder("host")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .build();
    return new OpenStructIndexConfig(false, null, 3, Set.of("views", "cpu", "host"), 0.5,
        List.of(views, cpu, host));
  }

  /// The spelling every other kind of column uses: RAW encoding, with the dictionary asked for under `indexes`.
  ///
  /// An External Table requires RAW on a column's own field config, so this is the shape a reader who has
  /// configured one before will reach for. An enabled inverted index already forced a dictionary on its own, so
  /// this combination worked before `indexes.dictionary` was honoured; it is here to keep it working.
  @Test
  public void testADictionaryAskedForUnderIndexesIsBuilt()
      throws Exception {
    File segmentDir = buildSegment("dictionaryViaIndexes", withoutPerKeyIndex());
    reload(segmentDir, rawKeyWithDictionaryUnderIndexes());

    assertTrue(hasIndex(segmentDir, CHILD, StandardIndexes.dictionary()),
        "indexes.dictionary must enable the dictionary on a RAW key");
    assertTrue(hasIndex(segmentDir, CHILD, StandardIndexes.inverted()),
        "and the inverted index built on top of it must exist");
  }

  /// The same spelling on a key whose only index is a range one, and the case that was actually broken.
  ///
  /// Unlike an inverted index, a range index needs a dictionary without forcing one, so nothing rescued a key
  /// written this way: the dictionary entry was ignored, the key stayed raw, and the range index was built over
  /// no dictionary at all. Reverting the fix fails this test and not the one above it.
  @Test
  public void testARangeOnlyKeyCanAskForADictionaryUnderIndexes()
      throws Exception {
    File segmentDir = buildSegment("rangeViaIndexes", withoutPerKeyIndex());
    reload(segmentDir, rawRangeKeyWithDictionaryUnderIndexes());

    assertTrue(hasIndex(segmentDir, OpenStructNaming.materializedColumnName(COLUMN, "views"),
        StandardIndexes.dictionary()), "a range-only key may ask for a dictionary the same way");
  }

  private static OpenStructIndexConfig rawKeyWithDictionaryUnderIndexes() {
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("dictionary", JsonUtils.newObjectNode());
    indexes.set("inverted", JsonUtils.newObjectNode());
    FieldConfig keyConfig = new FieldConfig.Builder(KEY)
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withIndexes(indexes)
        .build();
    return new OpenStructIndexConfig(false, null, -1, Set.of(KEY), 0.0, List.of(keyConfig));
  }

  private static OpenStructIndexConfig rawRangeKeyWithDictionaryUnderIndexes() {
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("dictionary", JsonUtils.newObjectNode());
    indexes.set("range", JsonUtils.newObjectNode());
    FieldConfig keyConfig = new FieldConfig.Builder("views")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withIndexes(indexes)
        .build();
    return new OpenStructIndexConfig(false, null, -1, Set.of("views"), 0.0, List.of(keyConfig));
  }

  /// The shared blob is an ordinary single-value STRING column holding each row's document, so an index that
  /// reads JSON out of a STRING column applies to it. Without one, every predicate on an unshredded key scans.
  @Test
  public void testAJsonIndexCanBeBuiltOnTheSparseBlob()
      throws Exception {
    // The blob only exists when some key is left out of the dense set, so the segment is built with a budget.
    File segmentDir = buildSegment("blobJsonIndex", withMixedMatrix());
    String sparse = OpenStructNaming.sparseColumnName(COLUMN);
    assertTrue(new SegmentMetadataImpl(segmentDir).getColumnMetadataMap().containsKey(sparse),
        "precondition: a key was left unmaterialized, so the blob column exists");
    assertFalse(hasIndex(segmentDir, sparse, StandardIndexes.json()),
        "precondition: the blob starts with no index");

    reload(segmentDir, withSparseJsonIndex());

    assertTrue(hasIndex(segmentDir, sparse, StandardIndexes.json()),
        "sparseJsonIndex must build a JSON index over the blob");
    assertFalse(hasIndex(segmentDir, sparse, StandardIndexes.dictionary()),
        "and the blob must never get a dictionary -- its values are whole documents");
  }

  /// `sparseFieldConfig` names the index directly, so any registered index type that accepts a STRING column can
  /// be asked for on the blob rather than only the one `sparseJsonIndex` hard-codes.
  @Test
  public void testTheBlobIndexCanBeNamedDirectly()
      throws Exception {
    File segmentDir = buildSegment("blobNamedIndex", withMixedMatrix());
    reload(segmentDir, withSparseFieldConfig("json"));

    assertTrue(hasIndex(segmentDir, OpenStructNaming.sparseColumnName(COLUMN), StandardIndexes.json()),
        "an index named under sparseFieldConfig.indexes must be built on the blob");
  }

  private static OpenStructIndexConfig withSparseJsonIndex() {
    return new OpenStructIndexConfig(false, null, 3, Set.of("views", "cpu", "host"), 0.5,
        mixedMatrixKeys(), true);
  }

  private static OpenStructIndexConfig withSparseFieldConfig(String indexName) {
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set(indexName, JsonUtils.newObjectNode());
    FieldConfig blob = new FieldConfig.Builder(OpenStructNaming.sparseColumnName(COLUMN))
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withIndexes(indexes)
        .build();
    return new OpenStructIndexConfig(false, null, 3, Set.of("views", "cpu", "host"), 0.5,
        mixedMatrixKeys(), null, null, null, null, blob);
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
        .addField(new ComplexFieldSpec(COLUMN, FieldSpec.DataType.OPEN_STRUCT, true, Map.of(
            "views", new DimensionFieldSpec("views", FieldSpec.DataType.LONG, true),
            "cpu", new DimensionFieldSpec("cpu", FieldSpec.DataType.DOUBLE, true),
            "host", new DimensionFieldSpec("host", FieldSpec.DataType.STRING, true))))
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
      props.put("views", (long) docId);
      props.put("cpu", docId * 0.5);
      props.put("host", "host-" + (docId % 5));
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
