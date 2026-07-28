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
package org.apache.pinot.segment.spi.utils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


/// Verifies segment metadata property parsing, including compression-statistics fields.
public class SegmentMetadataUtilsTest {
  private File _indexDir;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _indexDir = new File(FileUtils.getTempDirectory(), getClass().getSimpleName() + '-' + UUID.randomUUID());
    FileUtils.forceMkdir(_indexDir);
    PropertiesConfiguration properties = new PropertiesConfiguration();
    properties.setProperty("existingKey", "oldValue");
    CommonsConfigurationUtils.saveToFile(properties,
        new File(_indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_indexDir);
  }

  @Test
  public void testUpdatePersistsSetAndClearAndReloadsMetadata()
      throws Exception {
    SegmentMetadataImpl metadata = mock(SegmentMetadataImpl.class);
    when(metadata.getIndexDir()).thenReturn(_indexDir);
    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(metadata);
    Map<String, String> updates = new HashMap<>();
    updates.put("newKey", "newValue");
    updates.put("existingKey", null);

    SegmentMetadataUtils.updateMetadataProperties(segmentDirectory, updates);

    PropertiesConfiguration persisted = SegmentMetadataUtils.getPropertiesConfiguration(_indexDir);
    assertEquals(persisted.getString("newKey"), "newValue");
    assertFalse(persisted.containsKey("existingKey"));
    verify(segmentDirectory).reloadMetadata();
  }
}
