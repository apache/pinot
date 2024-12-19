package org.apache.pinot.tools.predownload;

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

import static org.apache.pinot.tools.predownload.TestUtil.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;


public class TableInfoTest {
  private TableInfo _tableInfo;
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
    _tableInfo = new TableInfo(TABLE_NAME, _tableConfig, schema, _instanceDataManagerConfig);
  }

  @Test
  public void testGetter() {
    assertEquals(_tableConfig, _tableInfo.getTableConfig());
  }

  @Test
  public void testLoadSegmentFromLocal()
      throws Exception {
    SegmentInfo segmentInfo = new SegmentInfo(TABLE_NAME, SEGMENT_NAME);
    SegmentZKMetadata metadata = createSegmentZKMetadata();
    segmentInfo.updateSegmentInfo(metadata);
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

      assertTrue(_tableInfo.loadSegmentFromLocal(segmentInfo, instanceDataManagerConfig));
      assertEquals(segmentInfo.getLocalCrc(), String.valueOf(CRC));
      assertTrue(segmentInfo.isDownloaded());
      assertEquals(segmentInfo.getLocalSizeBytes(), DISK_SIZE_BYTES);
    }

    // Has segment with different CRC
    try (MockedStatic<SegmentDirectoryLoaderRegistry> segmentDirectoryLoaderRegistryMockedStatic = mockStatic(
        SegmentDirectoryLoaderRegistry.class)) {
      segmentDirectoryLoaderRegistryMockedStatic.when(
              () -> SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(anyString()))
          .thenReturn(segmentDirectoryLoader);
      long newCrc = CRC + 1;
      when(segmentMetadataImpl.getCrc()).thenReturn(String.valueOf(newCrc));

      assertFalse(_tableInfo.loadSegmentFromLocal(segmentInfo, instanceDataManagerConfig));
      assertEquals(segmentInfo.getLocalCrc(), String.valueOf(newCrc));
      assertFalse(segmentInfo.isDownloaded());
      assertEquals(segmentInfo.getLocalSizeBytes(), DISK_SIZE_BYTES);
    }

    // Does not have segment
    try (MockedStatic<SegmentDirectoryLoaderRegistry> segmentDirectoryLoaderRegistryMockedStatic = mockStatic(
        SegmentDirectoryLoaderRegistry.class)) {
      segmentDirectoryLoaderRegistryMockedStatic.when(
              () -> SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(anyString()))
          .thenReturn(segmentDirectoryLoader);
      when(segmentMetadataImpl.getCrc()).thenReturn(null);
      doThrow(IOException.class).when(segmentDirectory).close();

      assertFalse(_tableInfo.loadSegmentFromLocal(segmentInfo, instanceDataManagerConfig));
      assertFalse(segmentInfo.isDownloaded());
      assertEquals(segmentInfo.getLocalSizeBytes(), DISK_SIZE_BYTES);
    }
  }
}
