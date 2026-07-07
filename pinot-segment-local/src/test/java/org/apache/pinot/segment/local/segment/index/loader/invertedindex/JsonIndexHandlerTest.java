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
package org.apache.pinot.segment.local.segment.index.loader.invertedindex;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link JsonIndexHandler} JSON index config-change detection.
 *
 * <p>When a JSON index is created, the {@link JsonIndexConfig} is persisted into {@code metadata.properties} under
 * {@code column.<name>.jsonIndexConfig}. On subsequent segment reloads, {@link JsonIndexHandler#needUpdateIndices}
 * compares the stored config with the current table config; if they differ the index is rebuilt. If no stored config
 * is present (index built before config persistence was added), the segment is treated as a legacy segment and a
 * one-time rebuild is triggered to backfill the stored config and catch any config drift.
 */
public class JsonIndexHandlerTest {
  private static final String COLUMN = "myJsonCol";

  private File _indexDir;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _indexDir = new File(FileUtils.getTempDirectory(), "json-index-handler-test-" + System.nanoTime());
    assertTrue(_indexDir.mkdirs());
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_indexDir);
  }

  @Test
  public void testNeedUpdateReturnsFalseWhenConfigUnchanged()
      throws Exception {
    JsonIndexConfig config = new JsonIndexConfig();
    storeConfig(COLUMN, config);

    JsonIndexHandler handler = createHandler(COLUMN, config);
    SegmentDirectory.Reader reader = mockReaderWithIndexOn(COLUMN);

    assertFalse(handler.needUpdateIndices(reader),
        "No rebuild expected when stored config equals current config");
  }

  @Test
  public void testNeedUpdateReturnsTrueWhenConfigChanged()
      throws Exception {
    // Store a default config (maxLevels = -1)
    JsonIndexConfig storedConfig = new JsonIndexConfig();
    storeConfig(COLUMN, storedConfig);

    // Current config has maxLevels = 3
    JsonIndexConfig currentConfig = new JsonIndexConfig();
    currentConfig.setMaxLevels(3);

    JsonIndexHandler handler = createHandler(COLUMN, currentConfig);
    SegmentDirectory.Reader reader = mockReaderWithIndexOn(COLUMN);

    assertTrue(handler.needUpdateIndices(reader),
        "Rebuild expected when maxLevels changed from default to 3");
  }

  @Test
  public void testNeedUpdateReturnsTrueWhenExcludeArrayChanges()
      throws Exception {
    JsonIndexConfig storedConfig = new JsonIndexConfig();
    storeConfig(COLUMN, storedConfig);

    JsonIndexConfig currentConfig = new JsonIndexConfig();
    currentConfig.setExcludeArray(true);

    JsonIndexHandler handler = createHandler(COLUMN, currentConfig);
    SegmentDirectory.Reader reader = mockReaderWithIndexOn(COLUMN);

    assertTrue(handler.needUpdateIndices(reader),
        "Rebuild expected when excludeArray changed from false to true");
  }

  @Test
  public void testNeedUpdateReturnsTrueWhenMaxBytesSizeChanges()
      throws Exception {
    JsonIndexConfig storedConfig = new JsonIndexConfig();
    storeConfig(COLUMN, storedConfig);

    JsonIndexConfig currentConfig = new JsonIndexConfig();
    currentConfig.setMaxBytesSize(1024L);

    JsonIndexHandler handler = createHandler(COLUMN, currentConfig);
    SegmentDirectory.Reader reader = mockReaderWithIndexOn(COLUMN);

    assertTrue(handler.needUpdateIndices(reader),
        "Rebuild expected when maxBytesSize changed");
  }

  @Test
  public void testNeedUpdateReturnsTrueWhenNoStoredConfig()
      throws Exception {
    // No stored config — index was built before config persistence was added (legacy segment).
    // Must trigger a one-time rebuild to backfill the stored config and detect any config drift.
    createEmptyMetadataFile();

    JsonIndexConfig currentConfig = new JsonIndexConfig();
    JsonIndexHandler handler = createHandler(COLUMN, currentConfig);
    SegmentDirectory.Reader reader = mockReaderWithIndexOn(COLUMN);

    assertTrue(handler.needUpdateIndices(reader),
        "Rebuild expected for legacy segments with no stored config (one-time backfill)");
  }

  // --- helpers ---

  private void storeConfig(String columnName, JsonIndexConfig config)
      throws Exception {
    File metadataFile = new File(_indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    PropertiesConfiguration properties = CommonsConfigurationUtils.fromFile(metadataFile);
    String key = V1Constants.MetadataKeys.Column.getKeyFor(columnName, "jsonIndexConfig");
    String serialized = JsonUtils.objectToString(config);
    String escaped = CommonsConfigurationUtils.replaceSpecialCharacterInPropertyValue(serialized);
    properties.setProperty(key, escaped);
    CommonsConfigurationUtils.saveToFile(properties, metadataFile);
  }

  private void createEmptyMetadataFile()
      throws Exception {
    File metadataFile = new File(_indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    PropertiesConfiguration properties = CommonsConfigurationUtils.fromFile(metadataFile);
    CommonsConfigurationUtils.saveToFile(properties, metadataFile);
  }

  private JsonIndexHandler createHandler(String columnName, JsonIndexConfig config) {
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.json(), config).build();
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("testSegment");
    when(segmentMetadata.getIndexDir()).thenReturn(_indexDir);
    when(segmentMetadata.getTotalDocs()).thenReturn(1);
    when(segmentMetadata.getAllColumns()).thenReturn(new TreeSet<>(Set.of(columnName)));
    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentDirectory.getColumnsWithIndex(StandardIndexes.json())).thenReturn(Set.of(columnName));
    return new JsonIndexHandler(segmentDirectory, Map.of(columnName, fieldIndexConfigs),
        mock(TableConfig.class), mock(Schema.class));
  }

  private SegmentDirectory.Reader mockReaderWithIndexOn(String columnName) {
    SegmentDirectory segDir = mock(SegmentDirectory.class);
    when(segDir.getColumnsWithIndex(StandardIndexes.json())).thenReturn(Set.of(columnName));
    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    when(reader.toSegmentDirectory()).thenReturn(segDir);
    return reader;
  }
}
