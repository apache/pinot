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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
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
    return buildSegmentWithoutInvertedIndex(null);
  }

  private File buildSegmentWithoutInvertedIndex(@Nullable SegmentVersion segmentVersion)
      throws Exception {
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGeneratorConfig(new File(TestUtils.getFileFromResourceUrl(resource)),
            FileFormat.AVRO, INDEX_DIR, RAW_TABLE_NAME, tableConfig(true, List.of()), schema());
    config.setSegmentNamePostfix("1");
    if (segmentVersion != null) {
      config.setSegmentVersion(segmentVersion);
    }
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
  /// This is the happy-path coverage of the directory branch of [IndexSizeUtils#sizeOfFileOrDirIndex]; see
  /// [#testDirectorySizingFailureReturnsNull] for its failure path. It also pins the marker rule: applying the marker
  /// here would over-report by 8 bytes, and adding it to a directory is the mistake the file-versus-directory split
  /// exists to prevent.
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

  /// When the directory branch of [IndexSizeUtils#sizeOfFileOrDirIndex] fails to size an existing index directory,
  /// the method must report the index as unmeasurable ([ColumnMetadata#UNAVAILABLE]) rather than a partial sum.
  /// `FileUtils.sizeOfDirectory` is mocked to throw so the failure is deterministic and does not depend on
  /// filesystem permissions.
  @Test
  public void testDirectorySizingFailureReturnsNull()
      throws Exception {
    File contentDir = new File(INDEX_DIR, "sizing-failure");
    File indexDir = new File(contentDir, "column4.dir.idx");
    assertTrue(indexDir.mkdirs());

    IndexType<?, ?, ?> indexType = mock(IndexType.class);
    when(indexType.getFileExtensions(null)).thenReturn(List.of(".dir.idx"));

    try (MockedStatic<FileUtils> mockedFileUtils = mockStatic(FileUtils.class, CALLS_REAL_METHODS)) {
      mockedFileUtils.when(() -> FileUtils.sizeOfDirectory(indexDir))
          .thenThrow(new UncheckedIOException(new IOException("Simulated directory sizing failure")));
      assertEquals(IndexSizeUtils.sizeOfFileOrDirIndex(indexType, contentDir, "column4", 8L, null),
          ColumnMetadata.UNAVAILABLE, "A directory that cannot be sized must be reported as unmeasurable, not a "
              + "partial sum");
    }
  }

  @Nullable
  private static File findTextIndexDirectory(File v3Dir) {
    File[] candidates = v3Dir.listFiles(f -> f.isDirectory() && f.getName().contains(".lucene")
        && f.getName().contains(".index"));
    return candidates == null || candidates.length == 0 ? null : candidates[0];
  }

  /// Spec 13 must not lose sizes that `index_map` cannot describe. External text/vector directories are never in
  /// `index_map`, and a V1/V2 segment has no `index_map` at all, so a refresh driven only by `getIndexSizeFor()`
  /// clears those keys and never restores them — deleting data rather than refreshing it.
  @Test
  public void testReloadPreservesExternalAndV1Sizes()
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

    String textKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column4", StandardIndexes.text().getId());
    assertTrue(indexSizeKeys(loadMetadata(segmentDir)).contains(textKey), "Sanity: seal recorded the text index size");
    long atSeal = loadMetadata(segmentDir).getLong(textKey);

    runPreProcessor(segmentDir, tableConfig, schema());

    PropertiesConfiguration afterReload = loadMetadata(segmentDir);
    assertTrue(indexSizeKeys(afterReload).contains(textKey),
        "A reload must not delete the external text index size; keys were: " + indexSizeKeys(afterReload));
    assertEquals(afterReload.getLong(textKey), atSeal,
        "The external directory size should be re-measured to the same value");
  }

  /// A present index that becomes transiently unmeasurable during a refresh (e.g. `FileUtils.sizeOfDirectory`
  /// throwing) must leave its existing persisted value untouched rather than clearing it. Clearing it would make
  /// the key permanently missing whenever the same failure recurs, which is exactly the condition that used to pin
  /// `needProcess()` at `true` forever; see [#testNeedProcessFalseWhenOnlyIndexSizeStatsAreMissing]. Every other
  /// column's keys, and this same column's other index keys, must be unaffected.
  @Test
  public void testReloadLeavesSizeUnchangedForPresentButUnmeasurableIndex()
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

    PropertiesConfiguration atSeal = loadMetadata(segmentDir);
    String textKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column4", StandardIndexes.text().getId());
    String forwardKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.forward().getId());
    assertTrue(indexSizeKeys(atSeal).contains(textKey), "Sanity: seal recorded the text index size");
    long textAtSeal = atSeal.getLong(textKey);
    long forwardAtSeal = atSeal.getLong(forwardKey);

    File textIndexDir = findTextIndexDirectory(new File(segmentDir, "v3"));
    assertNotNull(textIndexDir, "The text index should be a directory copied alongside columns.psf");

    try (MockedStatic<FileUtils> mockedFileUtils = mockStatic(FileUtils.class, CALLS_REAL_METHODS)) {
      mockedFileUtils.when(() -> FileUtils.sizeOfDirectory(textIndexDir))
          .thenThrow(new UncheckedIOException(new IOException("Simulated directory sizing failure")));
      runPreProcessor(segmentDir, tableConfig, schema());
    }

    PropertiesConfiguration afterReload = loadMetadata(segmentDir);
    assertTrue(indexSizeKeys(afterReload).contains(textKey),
        "A sizing failure must leave the existing persisted value in place, not clear it; keys were: "
            + indexSizeKeys(afterReload));
    assertEquals(afterReload.getLong(textKey), textAtSeal,
        "The stale-but-not-wrong value from seal time must survive a failed refresh attempt unchanged");
    assertEquals(afterReload.getLong(forwardKey), forwardAtSeal,
        "A sizing failure for one column's index must not affect another column's persisted size");
  }

  /// The other half of the data-loss bug: a V1 segment has no `v3/index_map` at all, so `getNumIndexes()` is 0 for
  /// every column and a refresh driven only by source 1 would restore nothing on reload -- wiping every persisted
  /// size, not just the ones the index_map-only view of the previous test covers.
  ///
  /// Reloads a V1 segment that never had an inverted index, with one now configured, and checks that both the
  /// pre-existing forward-index size and the newly-created inverted-index size survive with the exact V1 file
  /// length -- proving source 3 (individual file stat), not source 1, is what populated them.
  @Test
  public void testReloadRestoresSizesForV1Segment()
      throws Exception {
    Schema schema = schema();
    File segmentDir = buildSegmentWithoutInvertedIndex(SegmentVersion.v1);
    assertFalse(
        new File(SegmentDirectoryPaths.findSegmentDirectory(segmentDir), V1Constants.INDEX_MAP_FILE_NAME).exists(),
        "Sanity: a V1 segment has no index_map, so source 1 cannot describe anything here");

    String forwardKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.forward().getId());
    long forwardAtSeal = loadMetadata(segmentDir).getLong(forwardKey);
    assertTrue(forwardAtSeal > 0, "Sanity: seal recorded the forward index size");

    runPreProcessor(segmentDir, tableConfig(true, List.of("column3")), schema);

    PropertiesConfiguration afterReload = loadMetadata(segmentDir);
    String invertedKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.inverted().getId());
    assertTrue(indexSizeKeys(afterReload).contains(invertedKey),
        "Reload added an inverted index on a V1 segment, so its size must appear; keys were: "
            + indexSizeKeys(afterReload));
    assertTrue(indexSizeKeys(afterReload).contains(forwardKey),
        "A refresh limited to index_map would wipe every V1 key, including the untouched forward index; keys were: "
            + indexSizeKeys(afterReload));

    File segmentContentDir = SegmentDirectoryPaths.findSegmentDirectory(segmentDir);
    for (String extension : IndexService.getInstance().get(StandardIndexes.inverted().getId())
        .getFileExtensions(null)) {
      File indexFile = new File(segmentContentDir, "column3" + extension);
      if (indexFile.isFile()) {
        assertEquals(afterReload.getLong(invertedKey), indexFile.length(),
            "The new inverted index size must equal the actual V1 file length");
      }
    }
  }

  /// Scopes the refresh to "did this reload add an index" rather than "does the final layout look non-empty":
  /// adding one index must leave every other already-persisted key exactly as it was, not just present.
  @Test
  public void testReloadAddedOnlyLeavesOtherSizesUnchanged()
      throws Exception {
    Schema schema = schema();
    File segmentDir = buildSegmentWithoutInvertedIndex();
    String forwardKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.forward().getId());
    String dictionaryKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.dictionary().getId());
    long forwardBefore = loadMetadata(segmentDir).getLong(forwardKey);
    long dictionaryBefore = loadMetadata(segmentDir).getLong(dictionaryKey);

    runPreProcessor(segmentDir, tableConfig(true, List.of("column3")), schema);

    PropertiesConfiguration afterAdd = loadMetadata(segmentDir);
    String invertedKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.inverted().getId());
    assertTrue(indexSizeKeys(afterAdd).contains(invertedKey),
        "Reload added an inverted index, so its size must appear; keys were: " + indexSizeKeys(afterAdd));
    assertEquals(afterAdd.getLong(forwardKey), forwardBefore,
        "An index untouched by this reload must keep its exact previous value, not just remain present");
    assertEquals(afterAdd.getLong(dictionaryKey), dictionaryBefore,
        "An index untouched by this reload must keep its exact previous value, not just remain present");
  }

  /// The mirror of [#testReloadAddedOnlyLeavesOtherSizesUnchanged]: removing one index must clear only that key and
  /// leave every other one exactly as it was.
  @Test
  public void testReloadRemovedOnlyLeavesOtherSizesUnchanged()
      throws Exception {
    Schema schema = schema();
    File segmentDir = buildSegment(true);
    String forwardKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.forward().getId());
    String dictionaryKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.dictionary().getId());
    String invertedKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.inverted().getId());
    assertTrue(indexSizeKeys(loadMetadata(segmentDir)).contains(invertedKey), "Sanity: seal recorded the "
        + "inverted index size");
    long forwardBefore = loadMetadata(segmentDir).getLong(forwardKey);
    long dictionaryBefore = loadMetadata(segmentDir).getLong(dictionaryKey);

    runPreProcessor(segmentDir, tableConfig(true, List.of()), schema);

    PropertiesConfiguration afterDrop = loadMetadata(segmentDir);
    assertFalse(indexSizeKeys(afterDrop).contains(invertedKey),
        "The inverted index was dropped, so its stale size must be cleared; keys were: "
            + indexSizeKeys(afterDrop));
    assertEquals(afterDrop.getLong(forwardKey), forwardBefore,
        "An index untouched by this reload must keep its exact previous value, not just remain present");
    assertEquals(afterDrop.getLong(dictionaryKey), dictionaryBefore,
        "An index untouched by this reload must keep its exact previous value, not just remain present");
  }

  /// One reload can add one index and remove another at the same time (e.g. swapping which index type is
  /// configured on a column). Both transitions must be observed independently rather than one masking the other.
  @Test
  public void testReloadAddedAndRemovedInSameReload()
      throws Exception {
    File segmentDir = buildSegment(true);
    String invertedKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.inverted().getId());
    String rangeKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3", StandardIndexes.range().getId());
    assertTrue(indexSizeKeys(loadMetadata(segmentDir)).contains(invertedKey), "Sanity: seal recorded the "
        + "inverted index size");
    assertFalse(indexSizeKeys(loadMetadata(segmentDir)).contains(rangeKey), "Sanity: no range index was built");

    TableConfig reloadConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(List.of("column4"))
        .setIndexSizeStatsEnabled(true)
        .setRangeIndexColumns(List.of("column3"))
        .build();
    runPreProcessor(segmentDir, reloadConfig, schema());

    PropertiesConfiguration afterReload = loadMetadata(segmentDir);
    assertFalse(indexSizeKeys(afterReload).contains(invertedKey),
        "The inverted index was dropped by this reload, so its stale size must be cleared; keys were: "
            + indexSizeKeys(afterReload));
    assertTrue(indexSizeKeys(afterReload).contains(rangeKey),
        "The range index was added by this reload, so its size must appear; keys were: "
            + indexSizeKeys(afterReload));
    assertTrue(afterReload.getLong(rangeKey) > 0, "A newly created index must have a positive recorded size");
  }

  /// A reload that requests no index changes at all -- e.g. a pure tier-migration move -- must leave every
  /// persisted size at exactly its previous value rather than re-deriving (and potentially drifting) any of them.
  @Test
  public void testReloadWithNoIndexChangesLeavesEverySizeUnchanged()
      throws Exception {
    Schema schema = schema();
    File segmentDir = buildSegment(true);
    PropertiesConfiguration before = loadMetadata(segmentDir);
    Map<String, Long> beforeValues = new HashMap<>();
    for (String key : indexSizeKeys(before)) {
      beforeValues.put(key, before.getLong(key));
    }
    assertFalse(beforeValues.isEmpty(), "Sanity: seal recorded at least one index size");

    runPreProcessor(segmentDir, tableConfig(true, List.of("column3")), schema);

    PropertiesConfiguration after = loadMetadata(segmentDir);
    assertEquals(new HashMap<>(indexSizeKeys(after).stream()
            .collect(Collectors.toMap(k -> k, after::getLong))), beforeValues,
        "A reload with no index changes must not add, remove, or change the value of any persisted size");
  }

  /// A handler can remove and recreate an index of the same type within one reload, leaving presence unchanged
  /// (true before and after) while the underlying bytes differ -- e.g. `ForwardIndexHandler` rewriting a raw
  /// column's forward index under a new compression codec. A refresh scoped only to "added" (present after but not
  /// before) would keep the stale pre-reload size forever in this case; the size must be recomputed for every index
  /// present after the reload, not just newly-added ones.
  @Test
  public void testReloadRecomputesSizeForIndexRebuiltInPlace()
      throws Exception {
    File segmentDir = buildSegment(true);
    String forwardKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column4",
        StandardIndexes.forward().getId());
    long beforeSize = loadMetadata(segmentDir).getLong(forwardKey);
    assertTrue(beforeSize > 0, "Sanity: seal recorded the raw forward index size for column4");

    TableConfig reloadConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(List.of("column4"))
        .setIndexSizeStatsEnabled(true)
        .addFieldConfig(new FieldConfig("column4", FieldConfig.EncodingType.RAW, List.of(),
            FieldConfig.CompressionCodec.ZSTANDARD, null))
        .build();
    runPreProcessor(segmentDir, reloadConfig, schema());

    PropertiesConfiguration afterReload = loadMetadata(segmentDir);
    long afterSize = afterReload.getLong(forwardKey);
    assertNotEquals(afterSize, beforeSize,
        "ForwardIndexHandler rewrote column4's forward index in place under a new compression codec, so its "
            + "persisted size must be recomputed even though the index was present both before and after this "
            + "reload");

    PropertiesConfiguration indexMap = CommonsConfigurationUtils.fromFile(
        new File(SegmentDirectoryPaths.findSegmentDirectory(segmentDir), V1Constants.INDEX_MAP_FILE_NAME));
    long packed = indexMap.getLong("column4." + StandardIndexes.forward().getId() + ".size");
    assertEquals(afterSize, packed,
        "The recomputed size must match the freshly packed extent, not the stale build-time value");
  }

  // Note: V1 segments never have a v3/index_map at all, which is the closest reproducible analog of it being
  // unreadable at reload time: see testReloadRestoresSizesForV1Segment() for that coverage. A literal
  // "delete v3/index_map out from under an open V3 segment" is not a usable test here -- doing so makes the
  // packed columns.psf layout entirely unaddressable, which breaks every handler that reads existing packed data
  // (e.g. InvertedIndexHandler needs the forward index to build a new inverted index), not just this refresh.
  // That failure mode is a pre-existing constraint of the V3 format, not something this refresh could paper over.

  /// A column's index can be physically present on disk (so it appears in the live "before" snapshot) while
  /// `indexSizeStatsEnabled` was off at creation time, so no size was ever persisted for it. Removing that index on
  /// a reload that also turns the flag on must not fail or spuriously dirty `metadata.properties` for a key that
  /// was never there -- it must simply leave the never-persisted, now-removed key absent, and correctly persist
  /// sizes for whatever indexes the reload leaves present instead.
  @Test
  public void testReloadRemovingNeverPersistedIndexDoesNotFail()
      throws Exception {
    Schema schema = schema();
    File segmentDir = buildSegment(false);
    assertTrue(indexSizeKeys(loadMetadata(segmentDir)).isEmpty(),
        "Sanity: the flag was off at seal time, so no index size key should have been persisted");

    String invertedKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.inverted().getId());
    String forwardKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.forward().getId());

    runPreProcessor(segmentDir, tableConfig(true, List.of()), schema);

    PropertiesConfiguration afterReload = loadMetadata(segmentDir);
    assertFalse(indexSizeKeys(afterReload).contains(invertedKey),
        "The inverted index was both never persisted and dropped by this reload, so its key must stay absent; "
            + "keys were: " + indexSizeKeys(afterReload));
    assertTrue(indexSizeKeys(afterReload).contains(forwardKey),
        "The flag is now on and the forward index is still present after this reload, so its size must be "
            + "persisted; keys were: " + indexSizeKeys(afterReload));
    assertTrue(afterReload.getLong(forwardKey) > 0, "A freshly persisted index size must be positive");
  }

  /// A column's index can be physically present while `indexSizeStatsEnabled` is off, so no size is ever persisted
  /// for it; a reload with the flag off must not touch `metadata.properties` at all, so a size dropped in that
  /// window is left stale. Turning the flag back on afterward, with the index still absent, must reconcile that
  /// phantom rather than serve it forever -- it was never observed as "removed" by any reload, since the reload
  /// that actually removed it skipped the refresh entirely.
  @Test
  public void testReloadReconcilesPhantomLeftByFlagOffToggle()
      throws Exception {
    Schema schema = schema();
    File segmentDir = buildSegment(true);
    String invertedKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.inverted().getId());
    assertTrue(indexSizeKeys(loadMetadata(segmentDir)).contains(invertedKey),
        "Sanity: seal recorded the inverted index size");

    // Flag off: the inverted index is dropped, but the refresh is skipped entirely, so the stale size survives.
    runPreProcessor(segmentDir, tableConfig(false, List.of()), schema);
    assertTrue(indexSizeKeys(loadMetadata(segmentDir)).contains(invertedKey),
        "Sanity: a flag-off reload must not touch persisted sizes at all, leaving this one stale");

    // Flag back on, index still absent: the phantom left behind above must now be cleared.
    runPreProcessor(segmentDir, tableConfig(true, List.of()), schema);
    assertFalse(indexSizeKeys(loadMetadata(segmentDir)).contains(invertedKey),
        "Turning the flag back on must reconcile a phantom size left behind while it was off; keys were: "
            + indexSizeKeys(loadMetadata(segmentDir)));
  }

  /// Missing index size stats must never make `needProcess()` return `true`, whether the flag is off or was just
  /// turned on for a table whose segment already has indexes with no persisted size. `needProcess() == true` drives
  /// a full segment-directory copy and reprocess (see `BaseTableDataManager`), and a present index that is
  /// persistently unmeasurable could never satisfy a check based on presence of a size, so every future load of
  /// that segment would copy and reprocess it again forever. Missing sizes are instead backfilled opportunistically
  /// as a side effect whenever a reload runs for some other reason -- verified here by triggering one (dropping the
  /// inverted index) and confirming the backfill happens without `needProcess()` having asked for it.
  @Test
  public void testNeedProcessFalseWhenOnlyIndexSizeStatsAreMissing()
      throws Exception {
    Schema schema = schema();
    File segmentDir = buildSegment(false);
    assertTrue(indexSizeKeys(loadMetadata(segmentDir)).isEmpty(),
        "Sanity: the flag was off at seal time, so no index size key should have been persisted");

    assertFalse(needProcess(segmentDir, tableConfig(false, List.of("column3")), schema),
        "A table with the flag off must never be reprocessed just because sizes are unpersisted");
    assertFalse(needProcess(segmentDir, tableConfig(true, List.of("column3")), schema),
        "Turning the flag on must not by itself trigger reprocessing; missing sizes are backfilled opportunistically");

    // Trigger a real reload for an unrelated reason (dropping the inverted index) and confirm sizes are backfilled
    // as a side effect, without needProcess() ever having asked for a reload on their account.
    runPreProcessor(segmentDir, tableConfig(true, List.of()), schema);
    assertFalse(indexSizeKeys(loadMetadata(segmentDir)).isEmpty(),
        "A reload triggered for any other reason must still backfill missing index size stats as a side effect");
  }

  private static boolean needProcess(File segmentDir, TableConfig tableConfig, Schema schema)
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
        SegmentPreProcessor processor =
            new SegmentPreProcessor(segmentDirectory, new IndexLoadingConfig(tableConfig, schema))) {
      return processor.needProcess();
    }
  }

  /// A probe failure for one index type (e.g. `getColumnsWithIndex` throwing) must make that type's post-reload
  /// presence unknown for this reload rather than absent, so its existing persisted size is left untouched -- not
  /// cleared as if the index had disappeared. Every other index type's refresh must be unaffected by the failure.
  private static final class ThrowingIndexTypeSegmentDirectory extends SegmentLocalFSDirectory {
    private final IndexType<?, ?, ?> _throwingIndexType;
    private int _throwCount;

    ThrowingIndexTypeSegmentDirectory(File indexDir, IndexType<?, ?, ?> throwingIndexType)
        throws Exception {
      super(indexDir, ReadMode.mmap);
      _throwingIndexType = throwingIndexType;
    }

    @Override
    public Set<String> getColumnsWithIndex(IndexType<?, ?, ?> type) {
      // Index handlers call this same method for their own purposes while computing operations, before the
      // post-reload snapshot ever runs; only the snapshot's own probe -- not those handler calls -- should fail.
      if (type == _throwingIndexType && calledFromSnapshotIndexTypeIds()) {
        _throwCount++;
        throw new RuntimeException("Simulated probe failure for " + type.getId());
      }
      return super.getColumnsWithIndex(type);
    }

    private int getThrowCount() {
      return _throwCount;
    }
  }

  /// A backing `SegmentDirectory` that reports no indexes on any column for every index type, but only while
  /// `snapshotIndexTypeIds` itself is probing -- the empty-snapshot state its javadoc treats the same as a probe
  /// failure, skipping the refresh entirely rather than clearing every persisted size.
  private static final class EmptyProbeSegmentDirectory extends SegmentLocalFSDirectory {
    EmptyProbeSegmentDirectory(File indexDir)
        throws Exception {
      super(indexDir, ReadMode.mmap);
    }

    @Override
    public Set<String> getColumnsWithIndex(IndexType<?, ?, ?> type) {
      if (calledFromSnapshotIndexTypeIds()) {
        return Set.of();
      }
      return super.getColumnsWithIndex(type);
    }
  }

  /// Index handlers call `getColumnsWithIndex` for their own purposes while computing operations, before the
  /// post-reload snapshot ever runs; the test doubles above must only simulate a probe issue for the snapshot's own
  /// call, not those handler calls.
  private static boolean calledFromSnapshotIndexTypeIds() {
    for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
      if (element.getMethodName().equals("snapshotIndexTypeIds")) {
        return true;
      }
    }
    return false;
  }

  /// Runs the preprocessor with the given index type's probe rigged to fail, returning how many times the
  /// simulated failure actually fired -- so callers can assert it fired at all, not just that the run completed.
  private static int runPreProcessorWithThrowingProbe(File segmentDir, TableConfig tableConfig, Schema schema,
      IndexType<?, ?, ?> throwingIndexType)
      throws Exception {
    try (ThrowingIndexTypeSegmentDirectory segmentDirectory =
        new ThrowingIndexTypeSegmentDirectory(segmentDir, throwingIndexType);
        SegmentPreProcessor processor =
            new SegmentPreProcessor(segmentDirectory, new IndexLoadingConfig(tableConfig, schema))) {
      processor.process();
      return segmentDirectory.getThrowCount();
    }
  }

  private static void runPreProcessorWithEmptyProbe(File segmentDir, TableConfig tableConfig, Schema schema)
      throws Exception {
    try (SegmentDirectory segmentDirectory = new EmptyProbeSegmentDirectory(segmentDir);
        SegmentPreProcessor processor =
            new SegmentPreProcessor(segmentDirectory, new IndexLoadingConfig(tableConfig, schema))) {
      processor.process();
    }
  }

  @Test
  public void testReloadLeavesSizeUnchangedWhenIndexTypeProbeThrows()
      throws Exception {
    Schema schema = schema();
    File segmentDir = buildSegment(true);
    String invertedKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.inverted().getId());
    String forwardKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.forward().getId());
    String dictionaryKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.dictionary().getId());
    PropertiesConfiguration before = loadMetadata(segmentDir);
    assertTrue(indexSizeKeys(before).contains(invertedKey), "Sanity: seal recorded the inverted index size");
    long invertedBefore = before.getLong(invertedKey);
    long dictionaryBefore = before.getLong(dictionaryKey);

    // Drop the inverted index on this reload, but force its probe to throw: the drop must not be observed, so the
    // stale size must survive exactly as if the probe had never run, while unrelated indexes still refresh normally.
    int throwCount =
        runPreProcessorWithThrowingProbe(segmentDir, tableConfig(true, List.of()), schema, StandardIndexes.inverted());
    assertTrue(throwCount > 0,
        "Sanity: the simulated probe failure must actually have fired, or this test would pass vacuously");

    PropertiesConfiguration afterReload = loadMetadata(segmentDir);
    assertTrue(indexSizeKeys(afterReload).contains(invertedKey),
        "A probe failure must leave the existing persisted value in place, not clear it, even though the index "
            + "handler dropped the index; keys were: " + indexSizeKeys(afterReload));
    assertEquals(afterReload.getLong(invertedKey), invertedBefore,
        "The stale value from before this reload must survive a failed probe unchanged");
    assertEquals(afterReload.getLong(dictionaryKey), dictionaryBefore,
        "A probe failure for one index type must not affect another index type's persisted size");
    assertTrue(indexSizeKeys(afterReload).contains(forwardKey),
        "An index type whose probe did not throw must still be refreshed normally");
  }

  /// A persisted key can name an index type this node has no plugin for at all -- e.g. persisted by a different
  /// node or version with a plugin this node lacks. Such an id can never appear in a probe, since probing only
  /// iterates locally-registered [IndexType]s (see `snapshotIndexTypeIds`), so its persisted entry must survive a
  /// reload untouched even while unrelated, registered index types refresh normally.
  @Test
  public void testReloadPreservesSizeForUnregisteredIndexTypeId()
      throws Exception {
    Schema schema = schema();
    File segmentDir = buildSegment(true);
    String unregisteredKey =
        V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3", "someUnregisteredPlugin");

    PropertiesConfiguration properties = SegmentMetadataUtils.getPropertiesConfiguration(segmentDir);
    properties.setProperty(unregisteredKey, "12345");
    SegmentMetadataUtils.savePropertiesConfiguration(properties, segmentDir);
    assertEquals(loadMetadata(segmentDir).getLong(unregisteredKey), 12345L,
        "Sanity: the hand-written key for an unregistered plugin was saved");

    String forwardKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.forward().getId());
    String invertedKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor("column3",
        StandardIndexes.inverted().getId());
    // Trigger a real reload for an unrelated reason (dropping the inverted index) so the refresh path actually runs.
    runPreProcessor(segmentDir, tableConfig(true, List.of()), schema);

    PropertiesConfiguration afterReload = loadMetadata(segmentDir);
    assertEquals(afterReload.getLong(unregisteredKey), 12345L,
        "A persisted size for an index type this node has no plugin for must survive a reload unchanged, since it "
            + "can never be probed");
    assertTrue(afterReload.getLong(forwardKey) > 0,
        "Sanity: the refresh must still run normally for a registered index type in the same reload");
    assertFalse(indexSizeKeys(afterReload).contains(invertedKey),
        "Sanity: the refresh must actually have run this reload, evidenced by the dropped inverted index's size "
            + "being cleared -- both prior assertions above would hold even if the refresh never ran at all");
  }

  /// [SegmentPreProcessor]'s post-reload snapshot treats an empty result as equivalent to a probe failure -- see its
  /// javadoc -- because a non-empty segment always has a forward index or dictionary on every column, so "no
  /// indexes anywhere" means the backing directory answered spuriously rather than reporting a genuine state. That
  /// must skip the size refresh entirely for this reload, leaving every already-persisted size untouched.
  @Test
  public void testReloadSkipsRefreshWhenPostReloadSnapshotIsEmpty()
      throws Exception {
    Schema schema = schema();
    File segmentDir = buildSegment(true);
    PropertiesConfiguration before = loadMetadata(segmentDir);
    Map<String, Long> beforeValues = new HashMap<>();
    for (String key : indexSizeKeys(before)) {
      beforeValues.put(key, before.getLong(key));
    }
    assertFalse(beforeValues.isEmpty(), "Sanity: seal recorded at least one index size");

    // Drop the inverted index on this reload, but force the post-reload snapshot to see no indexes anywhere: the
    // drop must not be observed, so every persisted size must survive exactly as if no refresh had run at all.
    runPreProcessorWithEmptyProbe(segmentDir, tableConfig(true, List.of()), schema);

    PropertiesConfiguration after = loadMetadata(segmentDir);
    assertEquals(new HashMap<>(indexSizeKeys(after).stream()
            .collect(Collectors.toMap(k -> k, after::getLong))), beforeValues,
        "An empty post-reload snapshot must skip the refresh entirely, leaving every persisted size unchanged, even "
            + "though the index handler dropped the inverted index");
  }

  /// Shared assertion for a file-backed (packed) index: the persisted `metadata.properties` size for
  /// `column`/`indexTypeId` must exist, be positive, and equal the `v3/index_map` extent it was measured from (magic
  /// marker included), mirroring [#testEnabledPersistsSizeForEveryPackedIndex].
  private static void assertPackedIndexSizeMatchesIndexMap(File segmentDir, String column, String indexTypeId)
      throws ConfigurationException {
    PropertiesConfiguration metadata = loadMetadata(segmentDir);
    String key = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor(column, indexTypeId);
    assertTrue(indexSizeKeys(metadata).contains(key),
        "Expected a persisted size for " + key + ", keys were: " + indexSizeKeys(metadata));
    long persisted = metadata.getLong(key);
    assertTrue(persisted > 0, "Index size should be positive for " + key + " but was " + persisted);

    File indexMapFile =
        new File(SegmentDirectoryPaths.findSegmentDirectory(segmentDir), V1Constants.INDEX_MAP_FILE_NAME);
    assertTrue(indexMapFile.exists(), "v3 index_map should exist for a V3 segment");
    PropertiesConfiguration indexMap = CommonsConfigurationUtils.fromFile(indexMapFile);
    String indexMapKey = column + "." + indexTypeId + ".size";
    assertTrue(indexMap.containsKey(indexMapKey), "Expected an index_map entry for " + indexMapKey);
    long packed = indexMap.getLong(indexMapKey);
    assertEquals(persisted, packed,
        "Persisted size should equal the index_map extent, magic marker included, for " + key + ": persisted="
            + persisted + " packed=" + packed);
  }

  /// Builds a V3 segment from the shared AVRO fixture with a caller-supplied `tableConfig`, reusing [#schema()] for
  /// the columns. Lets each packed-index test configure just the index it cares about without duplicating the
  /// resource-lookup/driver boilerplate.
  private File buildAvroSegment(TableConfig tableConfig)
      throws Exception {
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGeneratorConfig(new File(TestUtils.getFileFromResourceUrl(resource)),
            FileFormat.AVRO, INDEX_DIR, RAW_TABLE_NAME, tableConfig, schema());
    config.setSegmentNamePostfix("1");
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();
    return segmentDirectory();
  }

  /// Builds a V3 segment from caller-supplied rows rather than the AVRO fixture, for index types (H3, null value
  /// vector, vector) whose data cannot be expressed with the fixture's plain string/int columns.
  private File buildCustomSegment(TableConfig tableConfig, Schema schema, List<GenericRow> rows)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getAbsolutePath());
    config.setSegmentName(RAW_TABLE_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return segmentDirectory();
  }

  /// Range index: file-backed/packed, built on the dictionary-encoded `column3` via the same
  /// `TableConfigBuilder` setter [RangeIndexType] tests configure it with.
  @Test
  public void testRangeIndexSizeMatchesPackedExtent()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(List.of("column4"))
        .setIndexSizeStatsEnabled(true)
        .setRangeIndexColumns(List.of("column3"))
        .build();
    File segmentDir = buildAvroSegment(tableConfig);
    assertPackedIndexSizeMatchesIndexMap(segmentDir, "column3", StandardIndexes.range().getId());
  }

  /// Bloom filter: file-backed/packed. Bloom filters hash raw values directly, so they need no dictionary and work
  /// on the same `column3` used above.
  @Test
  public void testBloomFilterSizeMatchesPackedExtent()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(List.of("column4"))
        .setIndexSizeStatsEnabled(true)
        .setBloomFilterColumns(List.of("column3"))
        .build();
    File segmentDir = buildAvroSegment(tableConfig);
    assertPackedIndexSizeMatchesIndexMap(segmentDir, "column3", StandardIndexes.bloomFilter().getId());
  }

  /// JSON index: file-backed/packed. `column3`'s fixture values are not valid JSON, so `skipInvalidJson` is set
  /// (mirroring how `JsonIndexTest` pairs `setJsonIndexColumns` with a per-column `JsonIndexConfig` via
  /// `setJsonIndexConfigs`) rather than requiring a dedicated JSON-shaped fixture.
  @Test
  public void testJsonIndexSizeMatchesPackedExtent()
      throws Exception {
    JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
    jsonIndexConfig.setSkipInvalidJson(true);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(List.of("column4"))
        .setIndexSizeStatsEnabled(true)
        .setJsonIndexColumns(List.of("column3"))
        .setJsonIndexConfigs(Map.of("column3", jsonIndexConfig))
        .build();
    File segmentDir = buildAvroSegment(tableConfig);
    assertPackedIndexSizeMatchesIndexMap(segmentDir, "column3", StandardIndexes.json().getId());
  }

  /// FST index: file-backed/packed. FST is built over the dictionary, so it is configured (like
  /// `testTextIndexDirectorySizedWithoutMarker`'s TEXT index) via a `FieldConfig`, on the already dictionary-encoded
  /// `column3`.
  @Test
  public void testFstIndexSizeMatchesPackedExtent()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(List.of("column4"))
        .setIndexSizeStatsEnabled(true)
        .addFieldConfig(new FieldConfig("column3", FieldConfig.EncodingType.DICTIONARY,
            List.of(FieldConfig.IndexType.FST), null, null))
        .build();
    File segmentDir = buildAvroSegment(tableConfig);
    assertPackedIndexSizeMatchesIndexMap(segmentDir, "column3", StandardIndexes.fst().getId());
  }

  /// H3 index: file-backed/packed. Needs real serialized geometry values, which the AVRO fixture cannot provide, so
  /// this builds a small custom BYTES-column segment. The `FieldConfig`/`resolutions` property wiring mirrors
  /// `SegmentPreProcessorTest`'s H3 coverage.
  @Test
  public void testH3IndexSizeMatchesPackedExtent()
      throws Exception {
    String column = "h3Column";
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(column, DataType.BYTES)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setIndexSizeStatsEnabled(true)
        .addFieldConfig(new FieldConfig(column, FieldConfig.EncodingType.DICTIONARY,
            List.of(FieldConfig.IndexType.H3), null, Map.of("resolutions", "5")))
        .build();
    List<GenericRow> rows = new ArrayList<>();
    for (double[] lonLat : new double[][]{{-122.084, 37.421}, {-73.968, 40.785}, {2.349, 48.864}}) {
      GenericRow row = new GenericRow();
      Point point = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(lonLat[0], lonLat[1]));
      row.putValue(column, GeometrySerializer.serialize(point));
      rows.add(row);
    }
    File segmentDir = buildCustomSegment(tableConfig, schema, rows);
    assertPackedIndexSizeMatchesIndexMap(segmentDir, column, StandardIndexes.h3().getId());
  }

  /// Null value vector index: file-backed/packed. The bitmap file (and therefore a recorded size) is only written
  /// when at least one doc is actually null, so this mirrors
  /// `NullValueVectorHandlerTest#testNonNullFlagWrittenAtCreation`: null handling on at the table level, and a null
  /// passed straight into `GenericRow#putValue` for one row.
  @Test
  public void testNullValueVectorIndexSizeMatchesPackedExtent()
      throws Exception {
    String column = "svInt";
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(column, DataType.INT)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNullHandlingEnabled(true)
        .setIndexSizeStatsEnabled(true)
        .build();
    GenericRow nonNullRow = new GenericRow();
    nonNullRow.putValue(column, 1);
    GenericRow nullRow = new GenericRow();
    nullRow.putValue(column, null);
    File segmentDir = buildCustomSegment(tableConfig, schema, List.of(nonNullRow, nullRow));
    assertPackedIndexSizeMatchesIndexMap(segmentDir, column, StandardIndexes.nullValueVector().getId());
  }

  /// HNSW vector index with `storeInSegmentFile=false` (the default): directory-backed, exactly like the TEXT index
  /// in `testTextIndexDirectorySizedWithoutMarker`, which this test otherwise mirrors. The property wiring
  /// (`vectorIndexType`/`vectorDimension`/`vectorDistanceFunction`/`version`) matches `VectorTest`'s table config.
  @Test
  public void testVectorIndexDirectorySizedWithoutMarker()
      throws Exception {
    String column = "vectorColumn";
    int dimension = 4;
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addMultiValueDimension(column, DataType.FLOAT)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setIndexSizeStatsEnabled(true)
        .setFieldConfigList(List.of(new FieldConfig.Builder(column)
            .withEncodingType(FieldConfig.EncodingType.RAW)
            .withIndexTypes(List.of(FieldConfig.IndexType.VECTOR))
            .withProperties(Map.of(
                "vectorIndexType", "HNSW",
                "vectorDimension", String.valueOf(dimension),
                "vectorDistanceFunction", "COSINE",
                "version", "1",
                "storeInSegmentFile", "false"))
            .build()))
        .build();
    List<GenericRow> rows = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      Object[] vector = new Object[dimension];
      for (int d = 0; d < dimension; d++) {
        vector[d] = (float) (i + d);
      }
      GenericRow row = new GenericRow();
      row.putValue(column, vector);
      rows.add(row);
    }
    File segmentDir = buildCustomSegment(tableConfig, schema, rows);

    File hnswDir = findDirectoryEndingWith(new File(segmentDir, "v3"),
        V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    assertNotNull(hnswDir, "The HNSW vector index should be a directory copied alongside columns.psf when "
        + "storeInSegmentFile=false");
    long onDisk = FileUtils.sizeOfDirectory(hnswDir);
    assertTrue(onDisk > 0, "Sanity: the vector index directory should not be empty");

    PropertiesConfiguration metadata = loadMetadata(segmentDir);
    String vectorKey = V1Constants.MetadataKeys.Column.getIndexSizeKeyFor(column, StandardIndexes.vector().getId());
    assertTrue(indexSizeKeys(metadata).contains(vectorKey),
        "A vector index size must be recorded; keys were: " + indexSizeKeys(metadata));
    assertEquals(metadata.getLong(vectorKey), onDisk,
        "The recorded vector index size must equal the recursive directory size of " + hnswDir.getName()
            + " with no magic marker added, since directories are copied rather than packed");

    PropertiesConfiguration indexMap = CommonsConfigurationUtils.fromFile(
        new File(SegmentDirectoryPaths.findSegmentDirectory(segmentDir), V1Constants.INDEX_MAP_FILE_NAME));
    for (String key : CommonsConfigurationUtils.getKeys(indexMap)) {
      assertFalse(key.startsWith(column + ".vector_index"),
          "An externally stored vector index must not appear in index_map, but found: " + key);
    }
  }

  /// IVF_FLAT vector index with `storeInSegmentFile=true`: unlike the HNSW directory case above, this backend
  /// writes a single file that the V1-to-V3 converter packs into `columns.psf` like any other file-backed index --
  /// this is Spec 4's other half, proving the generic file/directory check in `collectIndexSizes()` routes a
  /// file-backed vector backend through the packed `index_map` path with no double-count and no directory-sizing
  /// attempted on it, exactly like [#testVectorIndexDirectorySizedWithoutMarker] proves for the directory-backed
  /// HNSW case.
  @Test
  public void testIvfFlatVectorIndexSizeMatchesPackedExtent()
      throws Exception {
    String column = "vectorColumn";
    int dimension = 4;
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addMultiValueDimension(column, DataType.FLOAT)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setIndexSizeStatsEnabled(true)
        .setFieldConfigList(List.of(new FieldConfig.Builder(column)
            .withEncodingType(FieldConfig.EncodingType.RAW)
            .withIndexTypes(List.of(FieldConfig.IndexType.VECTOR))
            .withProperties(Map.of(
                "vectorIndexType", "IVF_FLAT",
                "vectorDimension", String.valueOf(dimension),
                "vectorDistanceFunction", "COSINE",
                "version", "1",
                "storeInSegmentFile", "true",
                "nlist", "2",
                "trainSampleSize", "20",
                "minRowsForIndex", "1"))
            .build()))
        .build();
    List<GenericRow> rows = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      Object[] vector = new Object[dimension];
      for (int d = 0; d < dimension; d++) {
        vector[d] = (float) (i + d);
      }
      GenericRow row = new GenericRow();
      row.putValue(column, vector);
      rows.add(row);
    }
    File segmentDir = buildCustomSegment(tableConfig, schema, rows);
    assertPackedIndexSizeMatchesIndexMap(segmentDir, column, StandardIndexes.vector().getId());

    File v3Dir = new File(segmentDir, "v3");
    File[] directoryEntries = v3Dir.listFiles(File::isDirectory);
    assertTrue(directoryEntries == null || directoryEntries.length == 0,
        "A storeInSegmentFile=true vector index must not leave a directory alongside columns.psf, found: "
            + (directoryEntries == null ? "null" : Arrays.toString(directoryEntries)));
  }

  @Nullable
  private static File findDirectoryEndingWith(File dir, String suffix) {
    File[] candidates = dir.listFiles(f -> f.isDirectory() && f.getName().endsWith(suffix));
    return candidates == null || candidates.length == 0 ? null : candidates[0];
  }
}
