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
package org.apache.pinot.segment.local.segment.index;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Tests that `indexSizeStatsEnabled` controls whether per-index-type sizes are persisted to
/// `metadata.properties` at seal time, and that the sizes written match the `v3/index_map` they are derived from.
public class IndexSizeStatsTest {
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "IndexSizeStatsTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String INDEX_SIZE_KEY_INFIX =
      "." + V1Constants.MetadataKeys.Column.INDEX_SIZE_IN_BYTES + ".";

  @BeforeMethod
  public void setUp() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  /// `indexSizeStatsEnabled` of `null` leaves the flag unset so the config default is exercised.
  private SegmentGeneratorConfig createSegmentConfig(Boolean indexSizeStatsEnabled) {
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    String filePath = TestUtils.getFileFromResourceUrl(resource);
    TableConfigBuilder tableConfigBuilder = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setInvertedIndexColumns(List.of("column3"))
        .setNoDictionaryColumns(List.of("column4"));
    if (indexSizeStatsEnabled != null) {
      tableConfigBuilder.setIndexSizeStatsEnabled(indexSizeStatsEnabled);
    }
    TableConfig tableConfig = tableConfigBuilder.build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("column3", DataType.STRING)
        .addSingleValueDimension("column4", DataType.STRING)
        .addMultiValueDimension("column6", DataType.INT)
        .addMultiValueDimension("column7", DataType.INT)
        .addDateTime("daysSinceEpoch", DataType.INT, "EPOCH|HOURS", "1:HOURS")
        .build();
    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGeneratorConfig(new File(filePath), FileFormat.AVRO, INDEX_DIR, RAW_TABLE_NAME,
            tableConfig, schema);
    config.setSegmentNamePostfix("1");
    return config;
  }

  private File buildSegment(Boolean indexSizeStatsEnabled)
      throws Exception {
    return buildSegment(indexSizeStatsEnabled, null);
  }

  private File buildSegment(Boolean indexSizeStatsEnabled, @Nullable SegmentVersion segmentVersion)
      throws Exception {
    SegmentGeneratorConfig config = createSegmentConfig(indexSizeStatsEnabled);
    if (segmentVersion != null) {
      config.setSegmentVersion(segmentVersion);
    }
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();
    return segmentDirectory();
  }

  /// The creation driver also leaves a `tmp-<uuid>` working directory behind, so pick the real segment directory by
  /// name rather than taking the first entry.
  private static File segmentDirectory() {
    File[] candidates = INDEX_DIR.listFiles(f -> f.isDirectory() && !f.getName().startsWith("tmp-"));
    assertNotNull(candidates);
    assertEquals(candidates.length, 1, "Expected exactly one segment directory under " + INDEX_DIR);
    return candidates[0];
  }

  private static List<String> indexSizeKeys(PropertiesConfiguration properties) {
    List<String> keys = new ArrayList<>();
    for (String key : CommonsConfigurationUtils.getKeys(properties)) {
      if (key.contains(INDEX_SIZE_KEY_INFIX)) {
        keys.add(key);
      }
    }
    return keys;
  }

  private static PropertiesConfiguration loadMetadata(File segmentDir)
      throws ConfigurationException {
    File metadataFile = SegmentDirectoryPaths.findMetadataFile(segmentDir);
    assertNotNull(metadataFile, "metadata.properties should exist under " + segmentDir);
    return CommonsConfigurationUtils.fromFile(metadataFile);
  }

  /// The flag defaults to off, so a segment built without setting it at all must carry no `indexSize` keys.
  @Test
  public void testDisabledByDefaultWritesNoIndexSizeKeys()
      throws Exception {
    File segmentDir = buildSegment(null);
    List<String> keys = indexSizeKeys(loadMetadata(segmentDir));
    assertTrue(keys.isEmpty(), "Expected no indexSize keys when the flag is left at its default, found: " + keys);
  }

  /// With the flag on, every sized entry in `v3/index_map` must have a persisted key whose value equals that entry
  /// exactly.
  ///
  /// Sizes are collected by statting the V1 index files before format conversion, so a persisted value is the index
  /// payload in bytes. `SingleFileIndexDirectory` prefixes every entry it packs into `columns.psf` with an 8-byte
  /// magic marker and records `payload + 8` as the entry size (`entry._size = size + MAGIC_MARKER_SIZE_BYTES`), so
  /// segment creation adds the same constant for V3 file-backed indexes. This assertion therefore pins both the
  /// collection logic and that format detail.
  /// Both `v2` and `v3` are packed into `columns.psf` by the converter, so both need the magic marker added.
  /// Gating the marker on `== v3` instead of `!= v1` under-reports every v2 index by 8 bytes per entry, and without
  /// this data provider every other test still passes.
  @DataProvider(name = "packedVersions")
  public static Object[][] packedVersions() {
    return new Object[][]{{SegmentVersion.v2}, {SegmentVersion.v3}};
  }

  @Test(dataProvider = "packedVersions")
  public void testEnabledPersistsSizeForEveryPackedIndex(SegmentVersion segmentVersion)
      throws Exception {
    File segmentDir = buildSegment(true, segmentVersion);
    PropertiesConfiguration metadata = loadMetadata(segmentDir);
    List<String> keys = indexSizeKeys(metadata);
    assertFalse(keys.isEmpty(), "Expected indexSize keys to be persisted when enabled");

    File indexMapFile =
        new File(SegmentDirectoryPaths.findSegmentDirectory(segmentDir), V1Constants.INDEX_MAP_FILE_NAME);
    assertTrue(indexMapFile.exists(), "v3 index_map should exist for a V3 segment");
    PropertiesConfiguration indexMap = CommonsConfigurationUtils.fromFile(indexMapFile);

    int compared = 0;
    for (String indexMapKey : CommonsConfigurationUtils.getKeys(indexMap)) {
      if (!indexMapKey.endsWith(".size")) {
        continue;
      }
      String withoutSuffix = indexMapKey.substring(0, indexMapKey.length() - ".size".length());
      int split = withoutSuffix.lastIndexOf('.');
      assertTrue(split > 0, "Unexpected index_map key: " + indexMapKey);
      String column = withoutSuffix.substring(0, split);
      String indexType = withoutSuffix.substring(split + 1);

      String expectedKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor(column, indexType);
      assertTrue(keys.contains(expectedKey),
          "Missing persisted index size for " + column + "/" + indexType + ", persisted keys: " + keys);
      long persisted = metadata.getLong(expectedKey);
      long packed = indexMap.getLong(indexMapKey);
      assertTrue(persisted > 0, "Index size should be positive for " + expectedKey + " but was " + persisted);
      assertEquals(persisted, packed,
          "Persisted V3 size should equal the index_map extent, magic marker included, for " + expectedKey
              + ": persisted=" + persisted + " packed=" + packed);
      compared++;
    }
    assertTrue(compared > 0, "Expected at least one sized index in the index map");
  }

  /// Pins the identity that matters: the persisted value equals the exact byte length of the V1 index file it was
  /// measured from. Uses a V1 segment so the files still exist after the build.
  ///
  /// Covers the raw forward index specifically. Raw chunk writers only emit their trailing chunk and header in
  /// `close()`, so measuring before the creators are closed reported 984 bytes for a 428208-byte index.
  @Test
  public void testPersistedSizesEqualV1FileLengths()
      throws Exception {
    SegmentGeneratorConfig config = createSegmentConfig(true);
    config.setSegmentVersion(SegmentVersion.v1);
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();
    File segmentDir = segmentDirectory();

    PropertiesConfiguration metadata = loadMetadata(segmentDir);
    List<String> keys = indexSizeKeys(metadata);
    assertFalse(keys.isEmpty(), "V1 segment should persist index sizes");

    IndexService indexService = IndexService.getInstance();
    int verified = 0;
    for (String key : keys) {
      // key shape: column.<column>.indexSizeInBytes.<indexTypeId>
      String withoutPrefix = key.substring(V1Constants.MetadataKeys.Column.COLUMN_PROPS_KEY_PREFIX.length());
      int infix = withoutPrefix.indexOf(INDEX_SIZE_KEY_INFIX);
      assertTrue(infix > 0, "Unexpected persisted key: " + key);
      String column = withoutPrefix.substring(0, infix);
      String indexTypeId = withoutPrefix.substring(infix + INDEX_SIZE_KEY_INFIX.length());

      long expected = 0;
      for (String extension : indexService.get(indexTypeId).getFileExtensions(null)) {
        File indexFile = new File(segmentDir, column + extension);
        if (indexFile.isFile()) {
          expected += indexFile.length();
        }
      }
      if (expected > 0) {
        assertEquals(metadata.getLong(key), expected, "Persisted size should equal the V1 file length for " + key);
        verified++;
      }
    }
    assertTrue(verified > 0, "Expected at least one persisted size to be verified against a V1 file");
  }

  /// Spec 5: the persisted keys must be readable through [ColumnMetadata], which is what makes the feature
  /// observable rather than a write-only on-disk format.
  ///
  /// Read via [ColumnMetadata#getPersistedIndexSizesInBytes], deliberately separate from `getIndexSizeFor` — that one
  /// is fed from `v3/index_map` and reflects the live packed layout, while these are a build-time snapshot readable
  /// without loading the segment payload, which is what lets cold-tier segments be reported.
  @Test
  public void testPersistedSizesAreReadableViaColumnMetadata()
      throws Exception {
    File segmentDir = buildSegment(true);

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(segmentDir);
    ColumnMetadata column3 = segmentMetadata.getColumnMetadataFor("column3");
    assertNotNull(column3);
    Map<String, Long> sizes = column3.getPersistedIndexSizesInBytes();

    for (String indexTypeId : List.of(StandardIndexes.dictionary().getId(), StandardIndexes.forward().getId(),
        StandardIndexes.inverted().getId())) {
      Long size = sizes.get(indexTypeId);
      assertNotNull(size, "Expected a persisted size for " + indexTypeId + ", got " + sizes);
      assertTrue(size > 0, indexTypeId + " size should be positive, got " + size);
    }

    // Values must match what was written, and reading must not disturb the live packed sizes.
    PropertiesConfiguration metadata = loadMetadata(segmentDir);
    assertEquals(sizes.get(StandardIndexes.dictionary().getId()).longValue(),
        metadata.getLong(V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
            StandardIndexes.dictionary().getId())));
    assertEquals(column3.getIndexSizeFor(StandardIndexes.dictionary()),
        metadata.getLong(V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
            StandardIndexes.dictionary().getId())),
        "getIndexSizeFor still comes from v3/index_map and must be unaffected by the persisted keys");
  }

  /// V1 segments have no `v3/index_map`, so the persisted keys are the only possible source. This is the case that
  /// the old index-map-only path could not serve at all.
  @Test
  public void testPersistedSizesReadableForV1Segment()
      throws Exception {
    SegmentGeneratorConfig config = createSegmentConfig(true);
    config.setSegmentVersion(SegmentVersion.v1);
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(segmentDirectory());
    ColumnMetadata column3 = segmentMetadata.getColumnMetadataFor("column3");
    assertNotNull(column3);
    assertFalse(column3.getPersistedIndexSizesInBytes().isEmpty(),
        "A V1 segment built with the flag on must expose persisted index sizes");
    assertEquals(column3.getIndexSizeFor(StandardIndexes.dictionary()), ColumnMetadata.UNAVAILABLE,
        "getIndexSizeFor has no index map to read on V1 and must stay UNAVAILABLE");
  }

  /// Without the flag there is nothing persisted, so the map must be empty rather than absent or throwing.
  @Test
  public void testNoPersistedSizesWhenFlagUnset()
      throws Exception {
    File segmentDir = buildSegment(null);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(segmentDir);
    ColumnMetadata column3 = segmentMetadata.getColumnMetadataFor("column3");
    assertNotNull(column3);
    assertTrue(column3.getPersistedIndexSizesInBytes().isEmpty(),
        "No keys are written when the flag is unset, so the map must be empty");
  }

  /// Spec 13: the persisted sizes must track the layout across a reload, not stay frozen at build time.
  ///
  /// Builds without an inverted index, reloads with one configured, then reloads again with it removed. Asserts the key
  /// appears and then disappears. Without the refresh hook the first assertion fails; without clearing existing entries
  /// first, the last one fails because a dropped index leaves a phantom size behind.
  @Test
  public void testReloadRefreshesAndClearsIndexSizes()
      throws Exception {
    Schema schema = schema();
    File segmentDir = buildSegmentWithoutInvertedIndex();

    String invertedKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.inverted().getId());
    assertFalse(indexSizeKeys(loadMetadata(segmentDir)).contains(invertedKey),
        "No inverted index was built, so its size must not be recorded");

    // Reload with the inverted index configured: the handler creates it, and the refresh must record its size.
    runPreProcessor(segmentDir, tableConfig(true, List.of("column3")), schema);
    PropertiesConfiguration afterAdd = loadMetadata(segmentDir);
    assertTrue(indexSizeKeys(afterAdd).contains(invertedKey),
        "Reload added an inverted index, so its size must appear; keys were: " + indexSizeKeys(afterAdd));
    assertTrue(afterAdd.getLong(invertedKey) > 0, "A newly created index must have a positive recorded size");

    // Reload with it removed: the key must be cleared rather than left behind as a phantom.
    runPreProcessor(segmentDir, tableConfig(true, List.of()), schema);
    PropertiesConfiguration afterDrop = loadMetadata(segmentDir);
    assertFalse(indexSizeKeys(afterDrop).contains(invertedKey),
        "The inverted index was dropped, so its stale size must be cleared; keys were: " + indexSizeKeys(afterDrop));
    assertTrue(indexSizeKeys(afterDrop).stream().anyMatch(k -> k.contains("forward")),
        "Surviving indexes must still be recorded after the refresh");
  }

  /// With the flag off, a reload must not introduce any keys.
  @Test
  public void testReloadWritesNothingWhenFlagOff()
      throws Exception {
    File segmentDir = buildSegment(null);
    assertTrue(indexSizeKeys(loadMetadata(segmentDir)).isEmpty(), "Sanity: the build wrote no keys");
    runPreProcessor(segmentDir, tableConfig(false, List.of("column3")), schema());
    assertTrue(indexSizeKeys(loadMetadata(segmentDir)).isEmpty(),
        "The flag is off, so a reload must not write index size keys either");
  }

  private File buildSegmentWithoutInvertedIndex()
      throws Exception {
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGeneratorConfig(new File(TestUtils.getFileFromResourceUrl(resource)),
            FileFormat.AVRO, INDEX_DIR, RAW_TABLE_NAME, tableConfig(true, List.of()), schema());
    config.setSegmentNamePostfix("1");
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();
    return segmentDirectory();
  }

  private static void runPreProcessor(File segmentDir, TableConfig tableConfig, Schema schema)
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
        SegmentPreProcessor processor =
            new SegmentPreProcessor(segmentDirectory, new IndexLoadingConfig(tableConfig, schema))) {
      processor.process();
    }
  }

  private static TableConfig tableConfig(boolean indexSizeStatsEnabled, List<String> invertedIndexColumns) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setInvertedIndexColumns(invertedIndexColumns)
        .setNoDictionaryColumns(List.of("column4"))
        .setIndexSizeStatsEnabled(indexSizeStatsEnabled)
        .build();
  }

  private static Schema schema() {
    return new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("column3", DataType.STRING)
        .addSingleValueDimension("column4", DataType.STRING)
        .addMultiValueDimension("column6", DataType.INT)
        .addMultiValueDimension("column7", DataType.INT)
        .addDateTime("daysSinceEpoch", DataType.INT, "EPOCH|HOURS", "1:HOURS")
        .build();
  }

  /// Specs 3/4: a text index held in its own directory is measured recursively and gets **no** magic marker, because
  /// the converter copies such directories alongside `columns.psf` rather than packing them into it.
  ///
  /// This is the only coverage of the directory branch and of `sizeOfDirectoryQuietly`. It also pins the marker rule:
  /// applying the marker here would over-report by 8 bytes, and adding it to a directory is the mistake the
  /// file-versus-directory split exists to prevent.
  @Test
  public void testTextIndexDirectorySizedWithoutMarker()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(List.of("column4"))
        .setIndexSizeStatsEnabled(true)
        .addFieldConfig(
            new FieldConfig("column4", FieldConfig.EncodingType.RAW, List.of(FieldConfig.IndexType.TEXT), null, null))
        .build();
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGeneratorConfig(new File(TestUtils.getFileFromResourceUrl(resource)),
            FileFormat.AVRO, INDEX_DIR, RAW_TABLE_NAME, tableConfig, schema());
    config.setSegmentNamePostfix("1");
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    File segmentDir = segmentDirectory();
    File textIndexDir = findTextIndexDirectory(new File(segmentDir, "v3"));
    assertNotNull(textIndexDir, "The text index should be a directory copied alongside columns.psf");
    long onDisk = org.apache.commons.io.FileUtils.sizeOfDirectory(textIndexDir);
    assertTrue(onDisk > 0, "Sanity: the text index directory should not be empty");

    PropertiesConfiguration metadata = loadMetadata(segmentDir);
    String textKey =
        V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column4", StandardIndexes.text().getId());
    assertTrue(indexSizeKeys(metadata).contains(textKey),
        "A text index size must be recorded; keys were: " + indexSizeKeys(metadata));
    assertEquals(metadata.getLong(textKey), onDisk,
        "The recorded text index size must equal the recursive directory size of " + textIndexDir.getName()
            + " with no magic marker added, since directories are copied rather than packed");

    // The directory is not an entry in columns.psf, which is why it needs measuring separately at all.
    PropertiesConfiguration indexMap = CommonsConfigurationUtils.fromFile(
        new File(SegmentDirectoryPaths.findSegmentDirectory(segmentDir), V1Constants.INDEX_MAP_FILE_NAME));
    for (String key : CommonsConfigurationUtils.getKeys(indexMap)) {
      assertFalse(key.startsWith("column4.text_index"),
          "An externally stored text index must not appear in index_map, but found: " + key);
    }
  }

  @Nullable
  private static File findTextIndexDirectory(File v3Dir) {
    File[] candidates = v3Dir.listFiles(f -> f.isDirectory() && f.getName().contains(".lucene")
        && f.getName().contains(".index"));
    return candidates == null || candidates.length == 0 ? null : candidates[0];
  }
}
