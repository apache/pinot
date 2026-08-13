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
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
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
    SegmentGeneratorConfig config = createSegmentConfig(indexSizeStatsEnabled);
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
  @Test
  public void testEnabledPersistsSizeForEveryPackedIndex()
      throws Exception {
    File segmentDir = buildSegment(true);
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
}
