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

import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.RangeIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link RangeIndexHandler}.
 *
 * <p>Covers:
 * <ul>
 *   <li>Version change detection (v1 on disk, v2 configured → rebuild required)</li>
 *   <li>No rebuild when on-disk version matches configured version</li>
 *   <li>Index removal when column is no longer in config</li>
 *   <li>New index creation when column is added to config (unsorted column)</li>
 *   <li>Sorted columns are skipped even when range index is configured</li>
 * </ul>
 */
public class RangeIndexHandlerTest {
  private static final String COLUMN = "price";

  // ---------------------------------------------------------------------------
  // Version change detection
  // ---------------------------------------------------------------------------

  @Test
  public void testNeedUpdateReturnsTrueWhenVersionChangesV1ToV2()
      throws Exception {
    // On-disk version is 1 (RangeIndexCreator), config requests version 2 (BitSlicedRangeIndexCreator).
    SegmentDirectory segmentDirectory = mockSegmentDirectory(COLUMN);
    SegmentDirectory.Reader reader = mockReaderWithVersion(segmentDirectory, COLUMN, 1);

    RangeIndexHandler handler = createHandler(segmentDirectory, new RangeIndexConfig(2));

    assertTrue(handler.needUpdateIndices(reader),
        "Rebuild expected when on-disk range index version differs from configured version");
  }

  @Test
  public void testNeedUpdateReturnsTrueWhenVersionChangesV2ToV1()
      throws Exception {
    // On-disk version is 2, config requests version 1 — downgrade also requires rebuild.
    SegmentDirectory segmentDirectory = mockSegmentDirectory(COLUMN);
    SegmentDirectory.Reader reader = mockReaderWithVersion(segmentDirectory, COLUMN, 2);

    RangeIndexHandler handler = createHandler(segmentDirectory, new RangeIndexConfig(1));

    assertTrue(handler.needUpdateIndices(reader),
        "Rebuild expected when configured version downgrades from v2 to v1");
  }

  @Test
  public void testNeedUpdateReturnsFalseWhenVersionUnchanged()
      throws Exception {
    // On-disk version matches configured version — no rebuild needed.
    SegmentDirectory segmentDirectory = mockSegmentDirectory(COLUMN);
    SegmentDirectory.Reader reader = mockReaderWithVersion(segmentDirectory, COLUMN, 2);

    RangeIndexHandler handler = createHandler(segmentDirectory, new RangeIndexConfig(2));

    assertFalse(handler.needUpdateIndices(reader),
        "No rebuild expected when on-disk version matches configured version");
  }

  // ---------------------------------------------------------------------------
  // Index removal when column is dropped from config
  // ---------------------------------------------------------------------------

  @Test
  public void testNeedUpdateReturnsTrueWhenColumnRemovedFromConfig()
      throws Exception {
    // Column has a range index on disk but the new config has no range index for it.
    SegmentDirectory segmentDirectory = mockSegmentDirectory(COLUMN);
    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);

    // Handler with no columns configured for range index.
    RangeIndexHandler handler = new RangeIndexHandler(segmentDirectory, Map.of(),
        mock(TableConfig.class), mock(Schema.class));

    assertTrue(handler.needUpdateIndices(reader),
        "Rebuild expected to remove index when column is dropped from range index config");
  }

  @Test
  public void testUpdateIndicesRemovesIndexWhenColumnDroppedFromConfig()
      throws Exception {
    SegmentDirectory segmentDirectory = mockSegmentDirectory(COLUMN);
    SegmentDirectory.Writer writer = mock(SegmentDirectory.Writer.class);
    when(writer.toSegmentDirectory()).thenReturn(segmentDirectory);

    RangeIndexHandler handler = new RangeIndexHandler(segmentDirectory, Map.of(),
        mock(TableConfig.class), mock(Schema.class));
    handler.updateIndices(writer);

    verify(writer).removeIndex(COLUMN, StandardIndexes.range());
  }

  // ---------------------------------------------------------------------------
  // New column added to config
  // ---------------------------------------------------------------------------

  @Test
  public void testNeedUpdateReturnsTrueWhenNewUnsortedColumnAdded()
      throws Exception {
    // Column has no range index yet and is not sorted — needs creation.
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("testSegment");

    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
    when(columnMetadata.isSorted()).thenReturn(false);
    when(segmentMetadata.getColumnMetadataFor(COLUMN)).thenReturn(columnMetadata);

    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadata);
    // No existing range index columns.
    when(segmentDirectory.getColumnsWithIndex(StandardIndexes.range())).thenReturn(Set.of());

    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);

    RangeIndexHandler handler = createHandler(segmentDirectory, new RangeIndexConfig(2));

    assertTrue(handler.needUpdateIndices(reader),
        "New range index expected for unsorted column not yet indexed");
  }

  // ---------------------------------------------------------------------------
  // Sorted columns are skipped
  // ---------------------------------------------------------------------------

  @Test
  public void testNeedUpdateReturnsFalseWhenColumnIsSorted()
      throws Exception {
    // Sorted columns use a different query path — range index is not created for them.
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("testSegment");

    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
    when(columnMetadata.isSorted()).thenReturn(true);
    when(segmentMetadata.getColumnMetadataFor(COLUMN)).thenReturn(columnMetadata);

    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentDirectory.getColumnsWithIndex(StandardIndexes.range())).thenReturn(Set.of());

    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);

    RangeIndexHandler handler = createHandler(segmentDirectory, new RangeIndexConfig(2));

    assertFalse(handler.needUpdateIndices(reader),
        "No range index creation expected for sorted columns");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static RangeIndexHandler createHandler(SegmentDirectory segmentDirectory,
      RangeIndexConfig rangeIndexConfig) {
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.range(), rangeIndexConfig).build();
    return new RangeIndexHandler(segmentDirectory, Map.of(COLUMN, fieldIndexConfigs),
        mock(TableConfig.class), mock(Schema.class));
  }

  /**
   * Creates a {@link SegmentDirectory} mock where {@code column} already has a range index on disk.
   */
  private static SegmentDirectory mockSegmentDirectory(String column) {
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("testSegment");

    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentDirectory.getColumnsWithIndex(StandardIndexes.range())).thenReturn(Set.of(column));
    return segmentDirectory;
  }

  /**
   * Creates a {@link SegmentDirectory.Reader} mock whose range index buffer for {@code column}
   * reports the given on-disk {@code version} at offset 0.
   */
  private static SegmentDirectory.Reader mockReaderWithVersion(SegmentDirectory segmentDirectory,
      String column, int version)
      throws Exception {
    PinotDataBuffer buffer = mock(PinotDataBuffer.class);
    when(buffer.getInt(0)).thenReturn(version);

    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);
    when(reader.getIndexFor(column, StandardIndexes.range())).thenReturn(buffer);
    return reader;
  }
}
