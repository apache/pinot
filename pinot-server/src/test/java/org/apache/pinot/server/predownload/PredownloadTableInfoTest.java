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
package org.apache.pinot.server.predownload;

import java.io.IOException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.server.starter.helix.HelixInstanceDataManagerConfig;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.server.predownload.PredownloadTestUtil.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;


public class PredownloadTableInfoTest {
  private PredownloadTableInfo _predownloadTableInfo;
  private TableConfig _tableConfig;
  private InstanceDataManagerConfig _instanceDataManagerConfig;
  private PinotConfiguration _pinotConfiguration;

  @BeforeClass
  public void setUp() {
    _pinotConfiguration = getPinotConfiguration();
    _tableConfig = mock(TableConfig.class);
    when(_tableConfig.getIndexingConfig()).thenReturn(new IndexingConfig());
    Schema schema = new Schema();
    _instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(_instanceDataManagerConfig.getConfig()).thenReturn(_pinotConfiguration);
    _predownloadTableInfo = new PredownloadTableInfo(TABLE_NAME, _tableConfig, schema, _instanceDataManagerConfig);
  }

  @Test
  public void testGetter() {
    assertEquals(_tableConfig, _predownloadTableInfo.getTableConfig());
  }

  @Test
  public void testLoadSegmentFromLocal()
      throws Exception {
    PredownloadSegmentInfo predownloadSegmentInfo = new PredownloadSegmentInfo(TABLE_NAME, SEGMENT_NAME);
    SegmentZKMetadata metadata = createSegmentZKMetadata();
    predownloadSegmentInfo.updateSegmentInfo(metadata);
    InstanceDataManagerConfig instanceDataManagerConfig = spy(new HelixInstanceDataManagerConfig(_pinotConfiguration));

    SegmentDirectoryLoader segmentDirectoryLoader = mock(SegmentDirectoryLoader.class);
    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    SegmentMetadataImpl segmentMetadataImpl = mock(SegmentMetadataImpl.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadataImpl);
    when(segmentDirectory.getDiskSizeBytes()).thenReturn(DISK_SIZE_BYTES);
    when(segmentDirectoryLoader.load(any(), any())).thenReturn(segmentDirectory);

    // Has segment with same CRC
    try (MockedStatic<SegmentDirectoryLoaderRegistry> segmentDirectoryLoaderRegistryMockedStatic = mockStatic(
        SegmentDirectoryLoaderRegistry.class)) {
      segmentDirectoryLoaderRegistryMockedStatic.when(
              () -> SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(anyString()))
          .thenReturn(segmentDirectoryLoader);
      when(segmentMetadataImpl.getCrc()).thenReturn(String.valueOf(CRC));

      assertTrue(_predownloadTableInfo.loadSegmentFromLocal(predownloadSegmentInfo, instanceDataManagerConfig));
      assertEquals(predownloadSegmentInfo.getLocalCrc(), String.valueOf(CRC));
      assertTrue(predownloadSegmentInfo.isDownloaded());
      assertEquals(predownloadSegmentInfo.getLocalSizeBytes(), DISK_SIZE_BYTES);
    }

    // Has segment with different CRC
    try (MockedStatic<SegmentDirectoryLoaderRegistry> segmentDirectoryLoaderRegistryMockedStatic = mockStatic(
        SegmentDirectoryLoaderRegistry.class)) {
      segmentDirectoryLoaderRegistryMockedStatic.when(
              () -> SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(anyString()))
          .thenReturn(segmentDirectoryLoader);
      long newCrc = CRC + 1;
      when(segmentMetadataImpl.getCrc()).thenReturn(String.valueOf(newCrc));

      assertFalse(_predownloadTableInfo.loadSegmentFromLocal(predownloadSegmentInfo, instanceDataManagerConfig));
      assertEquals(predownloadSegmentInfo.getLocalCrc(), String.valueOf(newCrc));
      assertFalse(predownloadSegmentInfo.isDownloaded());
      assertEquals(predownloadSegmentInfo.getLocalSizeBytes(), DISK_SIZE_BYTES);
    }

    // Does not have segment
    try (MockedStatic<SegmentDirectoryLoaderRegistry> segmentDirectoryLoaderRegistryMockedStatic = mockStatic(
        SegmentDirectoryLoaderRegistry.class)) {
      segmentDirectoryLoaderRegistryMockedStatic.when(
              () -> SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(anyString()))
          .thenReturn(segmentDirectoryLoader);
      when(segmentMetadataImpl.getCrc()).thenReturn(null);
      doThrow(IOException.class).when(segmentDirectory).close();

      assertFalse(_predownloadTableInfo.loadSegmentFromLocal(predownloadSegmentInfo, instanceDataManagerConfig));
      assertFalse(predownloadSegmentInfo.isDownloaded());
      assertEquals(predownloadSegmentInfo.getLocalSizeBytes(), DISK_SIZE_BYTES);
    }
  }
}
