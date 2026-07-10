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
import org.apache.pinot.segment.spi.index.FstIndexConfig;
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
 * Unit tests for {@link FSTIndexHandler}.
 *
 * <p>Covers:
 * <ul>
 *   <li>Legacy native FST detection — buffers with the native magic bytes trigger a rebuild</li>
 *   <li>Index removal when a column is dropped from the FST index config</li>
 *   <li>New index creation when a column is added to the config</li>
 *   <li>No update required when the index is already up-to-date</li>
 * </ul>
 */
public class FSTIndexHandlerTest {
  private static final String COLUMN = "name";

  /**
   * Magic int written at offset 0 of every legacy native FST file.
   * Mirrors the private constant in {@code FstIndexUtils}.
   */
  private static final int LEGACY_NATIVE_FST_MAGIC = ('\\' << 24) | ('f' << 16) | ('s' << 8) | 'a';

  // ---------------------------------------------------------------------------
  // Legacy native FST detection
  // ---------------------------------------------------------------------------

  @Test
  public void testNeedUpdateReturnsTrueWhenLegacyNativeFstDetected()
      throws Exception {
    SegmentDirectory segmentDirectory = mockSegmentDirectory(COLUMN);
    SegmentDirectory.Reader reader = mockReaderWithFstBuffer(segmentDirectory, COLUMN,
        legacyNativeBuffer());

    FSTIndexHandler handler = createHandler(segmentDirectory);

    assertTrue(handler.needUpdateIndices(reader),
        "Rebuild expected when legacy native FST index is detected");
  }

  @Test
  public void testUpdateIndicesRemovesLegacyNativeFstIndex()
      throws Exception {
    SegmentDirectory segmentDirectory = mockSegmentDirectory(COLUMN);
    SegmentDirectory.Writer writer = mockWriterWithFstBuffer(segmentDirectory, COLUMN,
        legacyNativeBuffer());

    FSTIndexHandler handler = createHandler(segmentDirectory);
    handler.updateIndices(writer);

    verify(writer).removeIndex(COLUMN, StandardIndexes.fst());
  }

  // ---------------------------------------------------------------------------
  // Column removed from config
  // ---------------------------------------------------------------------------

  @Test
  public void testNeedUpdateReturnsTrueWhenColumnRemovedFromConfig()
      throws Exception {
    // Column has an FST index on disk but the new config has no FST index for it.
    SegmentDirectory segmentDirectory = mockSegmentDirectory(COLUMN);
    SegmentDirectory.Reader reader = mockReaderWithFstBuffer(segmentDirectory, COLUMN,
        nonLegacyBuffer());

    // Handler with no columns configured for FST index.
    FSTIndexHandler handler = new FSTIndexHandler(segmentDirectory, Map.of(),
        mock(TableConfig.class), mock(Schema.class));

    assertTrue(handler.needUpdateIndices(reader),
        "Rebuild expected to remove index when column is dropped from FST index config");
  }

  @Test
  public void testUpdateIndicesRemovesIndexWhenColumnDroppedFromConfig()
      throws Exception {
    SegmentDirectory segmentDirectory = mockSegmentDirectory(COLUMN);
    SegmentDirectory.Writer writer = mockWriterWithFstBuffer(segmentDirectory, COLUMN,
        nonLegacyBuffer());

    FSTIndexHandler handler = new FSTIndexHandler(segmentDirectory, Map.of(),
        mock(TableConfig.class), mock(Schema.class));
    handler.updateIndices(writer);

    verify(writer).removeIndex(COLUMN, StandardIndexes.fst());
  }

  // ---------------------------------------------------------------------------
  // New column added to config
  // ---------------------------------------------------------------------------

  @Test
  public void testNeedUpdateReturnsTrueWhenNewColumnAdded()
      throws Exception {
    // Column is in config but has no existing FST index.
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("testSegment");
    when(segmentMetadata.getColumnMetadataFor(COLUMN)).thenReturn(mock(ColumnMetadata.class));

    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentDirectory.getColumnsWithIndex(StandardIndexes.fst())).thenReturn(Set.of());

    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);

    FSTIndexHandler handler = createHandler(segmentDirectory);

    assertTrue(handler.needUpdateIndices(reader),
        "New FST index expected for column added to config");
  }

  // ---------------------------------------------------------------------------
  // No update when already up-to-date
  // ---------------------------------------------------------------------------

  @Test
  public void testNeedUpdateReturnsFalseWhenIndexUpToDate()
      throws Exception {
    // Column has a Lucene FST index, is in config, and the buffer is not a legacy native index.
    SegmentDirectory segmentDirectory = mockSegmentDirectory(COLUMN);
    SegmentDirectory.Reader reader = mockReaderWithFstBuffer(segmentDirectory, COLUMN,
        nonLegacyBuffer());

    FSTIndexHandler handler = createHandler(segmentDirectory);

    assertFalse(handler.needUpdateIndices(reader),
        "No rebuild expected when FST index is current and matches config");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static FSTIndexHandler createHandler(SegmentDirectory segmentDirectory) {
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.fst(), new FstIndexConfig()).build();
    return new FSTIndexHandler(segmentDirectory, Map.of(COLUMN, fieldIndexConfigs),
        mock(TableConfig.class), mock(Schema.class));
  }

  private static SegmentDirectory mockSegmentDirectory(String column) {
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("testSegment");

    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentDirectory.getColumnsWithIndex(StandardIndexes.fst())).thenReturn(Set.of(column));
    return segmentDirectory;
  }

  private static SegmentDirectory.Reader mockReaderWithFstBuffer(SegmentDirectory segmentDirectory,
      String column, PinotDataBuffer buffer)
      throws Exception {
    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);
    when(reader.getIndexFor(column, StandardIndexes.fst())).thenReturn(buffer);
    return reader;
  }

  private static SegmentDirectory.Writer mockWriterWithFstBuffer(SegmentDirectory segmentDirectory,
      String column, PinotDataBuffer buffer)
      throws Exception {
    SegmentDirectory.Writer writer = mock(SegmentDirectory.Writer.class);
    when(writer.toSegmentDirectory()).thenReturn(segmentDirectory);
    when(writer.getIndexFor(column, StandardIndexes.fst())).thenReturn(buffer);
    return writer;
  }

  /** A {@link PinotDataBuffer} whose first int is the legacy native FST magic value. */
  private static PinotDataBuffer legacyNativeBuffer() {
    PinotDataBuffer buffer = mock(PinotDataBuffer.class);
    when(buffer.size()).thenReturn((long) Integer.BYTES);
    when(buffer.getInt(0)).thenReturn(LEGACY_NATIVE_FST_MAGIC);
    return buffer;
  }

  /** A {@link PinotDataBuffer} whose first int is NOT the legacy native FST magic value. */
  private static PinotDataBuffer nonLegacyBuffer() {
    PinotDataBuffer buffer = mock(PinotDataBuffer.class);
    when(buffer.size()).thenReturn((long) Integer.BYTES);
    when(buffer.getInt(0)).thenReturn(0);
    return buffer;
  }
}
