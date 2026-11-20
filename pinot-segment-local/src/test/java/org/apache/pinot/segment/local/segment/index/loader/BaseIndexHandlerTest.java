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
package org.apache.pinot.segment.local.segment.index.loader;

import java.io.File;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Unit tests for the config-persistence helpers in [BaseIndexHandler]:
/// [BaseIndexHandler#readStoredIndexConfig] and [BaseIndexHandler#setStoredIndexConfig].
///
/// Covers:
/// - Null properties (in-memory segment) → returns null, no exception
/// - Missing key (legacy segment, config never written) → returns null
/// - Round-trip: write then read back an equal config object
/// - Round-trip through disk: write to file, reload, read back equal config
/// - Special characters in serialized JSON (colons, quotes) round-trip correctly
/// - Corrupt/undeserializable stored value → returns null gracefully, no exception
/// - [BaseIndexHandler#loadMetadataProperties] returns non-null for a file-backed segment
public class BaseIndexHandlerTest {
  private static final String COLUMN = "myCol";
  private static final String CONFIG_KEY = "testConfig";

  private File _indexDir;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _indexDir = new File(FileUtils.getTempDirectory(), "base-index-handler-test-" + System.nanoTime());
    assertTrue(_indexDir.mkdirs());
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_indexDir);
  }

  @Test
  public void testReadReturnsNullForNullProperties() {
    assertNull(BaseIndexHandler.readStoredIndexConfig(COLUMN, CONFIG_KEY, JsonIndexConfig.class, null));
  }

  @Test
  public void testReadReturnsNullWhenKeyAbsent() {
    // Simulates a legacy segment that was built before config persistence was added.
    assertNull(BaseIndexHandler.readStoredIndexConfig(
        COLUMN, CONFIG_KEY, JsonIndexConfig.class, new PropertiesConfiguration()));
  }

  @Test
  public void testRoundTripInMemory()
      throws Exception {
    JsonIndexConfig config = new JsonIndexConfig();
    config.setMaxLevels(5);
    config.setExcludeArray(true);

    PropertiesConfiguration props = new PropertiesConfiguration();
    BaseIndexHandler.setStoredIndexConfig(COLUMN, CONFIG_KEY, config, props);

    JsonIndexConfig read = BaseIndexHandler.readStoredIndexConfig(COLUMN, CONFIG_KEY, JsonIndexConfig.class, props);
    assertNotNull(read);
    assertEquals(read, config);
  }

  @Test
  public void testRoundTripDefaultConfig()
      throws Exception {
    // Default JsonIndexConfig serializes to JSON with all-default values; verify it round-trips.
    PropertiesConfiguration props = new PropertiesConfiguration();
    JsonIndexConfig config = new JsonIndexConfig();
    BaseIndexHandler.setStoredIndexConfig(COLUMN, CONFIG_KEY, config, props);

    JsonIndexConfig read = BaseIndexHandler.readStoredIndexConfig(COLUMN, CONFIG_KEY, JsonIndexConfig.class, props);
    assertNotNull(read);
    assertEquals(read, config);
  }

  @Test
  public void testRoundTripWithSpecialCharactersInJson()
      throws Exception {
    // JsonIndexConfig serializes to JSON containing colons, quotes, and braces.
    // CommonsConfiguration treats ':' and '=' as key-value separators — verify the escaping
    // in setStoredIndexConfig / recoverSpecialCharacterInPropertyValue round-trips correctly.
    JsonIndexConfig config = new JsonIndexConfig();
    config.setMaxLevels(3);
    config.setMaxBytesSize(1024L);

    PropertiesConfiguration props = new PropertiesConfiguration();
    BaseIndexHandler.setStoredIndexConfig(COLUMN, CONFIG_KEY, config, props);

    // The raw stored string must be non-null (escaping applied).
    String key = V1Constants.MetadataKeys.Column.getKeyFor(COLUMN, CONFIG_KEY);
    assertNotNull(props.getString(key));

    JsonIndexConfig read = BaseIndexHandler.readStoredIndexConfig(COLUMN, CONFIG_KEY, JsonIndexConfig.class, props);
    assertNotNull(read);
    assertEquals(read, config);
  }

  @Test
  public void testRoundTripThroughDisk()
      throws Exception {
    // Write to an in-memory PropertiesConfiguration, flush to disk, reload, and verify.
    JsonIndexConfig config = new JsonIndexConfig();
    config.setMaxLevels(7);

    File metadataFile = new File(_indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    PropertiesConfiguration props = CommonsConfigurationUtils.fromFile(metadataFile);
    BaseIndexHandler.setStoredIndexConfig(COLUMN, CONFIG_KEY, config, props);
    CommonsConfigurationUtils.saveToFile(props, metadataFile);

    PropertiesConfiguration reloaded = CommonsConfigurationUtils.fromFile(metadataFile);
    JsonIndexConfig read = BaseIndexHandler.readStoredIndexConfig(COLUMN, CONFIG_KEY, JsonIndexConfig.class, reloaded);
    assertNotNull(read);
    assertEquals(read, config);
  }

  @Test
  public void testReadReturnsNullForCorruptValue() {
    // A corrupt / non-JSON stored value must not propagate an exception — returns null gracefully.
    String key = V1Constants.MetadataKeys.Column.getKeyFor(COLUMN, CONFIG_KEY);
    PropertiesConfiguration props = new PropertiesConfiguration();
    props.setProperty(key, "not-valid-json{{{");

    assertNull(BaseIndexHandler.readStoredIndexConfig(COLUMN, CONFIG_KEY, JsonIndexConfig.class, props));
  }

  @Test
  public void testLoadMetadataPropertiesReturnsNonNullForFileBacked()
      throws Exception {
    // Create a real (but empty) metadata.properties in the temp dir.
    File metadataFile = new File(_indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    CommonsConfigurationUtils.saveToFile(CommonsConfigurationUtils.fromFile(metadataFile), metadataFile);

    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("testSegment");
    when(segmentMetadata.getTotalDocs()).thenReturn(1);
    when(segmentMetadata.getAllColumns()).thenReturn(Set.of());
    when(segmentMetadata.getIndexDir()).thenReturn(_indexDir);

    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadata);

    assertNotNull(new TestIndexHandler(segmentDirectory).loadMetadataProperties());
  }

  @Test
  public void testMultipleColumnsAndKeysDoNotInterfere()
      throws Exception {
    // Storing configs for two different columns under the same key must not overwrite each other.
    JsonIndexConfig configA = new JsonIndexConfig();
    configA.setMaxLevels(2);
    JsonIndexConfig configB = new JsonIndexConfig();
    configB.setMaxLevels(8);

    PropertiesConfiguration props = new PropertiesConfiguration();
    BaseIndexHandler.setStoredIndexConfig("colA", CONFIG_KEY, configA, props);
    BaseIndexHandler.setStoredIndexConfig("colB", CONFIG_KEY, configB, props);

    assertEquals(
        BaseIndexHandler.readStoredIndexConfig("colA", CONFIG_KEY, JsonIndexConfig.class, props), configA);
    assertEquals(
        BaseIndexHandler.readStoredIndexConfig("colB", CONFIG_KEY, JsonIndexConfig.class, props), configB);
  }

  /// Minimal concrete subclass used to access [BaseIndexHandler#loadMetadataProperties],
  /// which is a protected instance method.
  private static class TestIndexHandler extends BaseIndexHandler {
    TestIndexHandler(SegmentDirectory segmentDirectory) {
      super(segmentDirectory, Map.of(), mock(TableConfig.class), mock(Schema.class));
    }

    @Override
    public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
      return false;
    }

    @Override
    public void updateIndices(SegmentDirectory.Writer segmentWriter) {
    }

    @Override
    public void postUpdateIndicesCleanup(SegmentDirectory.Writer segmentWriter) {
    }
  }
}
