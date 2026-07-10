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
 * Unit tests for {@link IFSTIndexHandler}.
 *
 * <p>IFST (case-Insensitive FST) indexes share the same index lifecycle as FST indexes but
 * use a separate index type ({@link StandardIndexes#ifst()}) and file extension.
 *
 * <p>Covers:
 * <ul>
 *   <li>Index removal when a column is dropped from the IFST index config</li>
 *   <li>New index creation when a column is added to the config</li>
 *   <li>No update required when the index is already present and matches config</li>
 *   <li>No update when column metadata is absent (column not in segment)</li>
 * </ul>
 */
public class IFSTIndexHandlerTest {
  private static final String COLUMN = "category";

  // ---------------------------------------------------------------------------
  // Column removed from config
  // ---------------------------------------------------------------------------

  @Test
  public void testNeedUpdateReturnsTrueWhenColumnRemovedFromConfig()
      throws Exception {
    // Column has an IFST index on disk but the new config has no IFST index for it.
    SegmentDirectory segmentDirectory = mockSegmentDirectory(COLUMN);
    SegmentDirectory.Reader reader = mockReader(segmentDirectory);

    IFSTIndexHandler handler = new IFSTIndexHandler(segmentDirectory, Map.of(),
        mock(TableConfig.class), mock(Schema.class));

    assertTrue(handler.needUpdateIndices(reader),
        "Rebuild expected to remove index when column is dropped from IFST index config");
  }

  @Test
  public void testUpdateIndicesRemovesIndexWhenColumnDroppedFromConfig()
      throws Exception {
    SegmentDirectory segmentDirectory = mockSegmentDirectory(COLUMN);
    SegmentDirectory.Writer writer = mock(SegmentDirectory.Writer.class);
    when(writer.toSegmentDirectory()).thenReturn(segmentDirectory);

    IFSTIndexHandler handler = new IFSTIndexHandler(segmentDirectory, Map.of(),
        mock(TableConfig.class), mock(Schema.class));
    handler.updateIndices(writer);

    verify(writer).removeIndex(COLUMN, StandardIndexes.ifst());
  }

  // ---------------------------------------------------------------------------
  // New column added to config
  // ---------------------------------------------------------------------------

  @Test
  public void testNeedUpdateReturnsTrueWhenNewColumnAdded()
      throws Exception {
    // Column is in config but has no existing IFST index — metadata present.
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("testSegment");
    when(segmentMetadata.getColumnMetadataFor(COLUMN)).thenReturn(mock(ColumnMetadata.class));

    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentDirectory.getColumnsWithIndex(StandardIndexes.ifst())).thenReturn(Set.of());

    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);

    assertTrue(createHandler(segmentDirectory).needUpdateIndices(reader),
        "New IFST index expected for column added to config with metadata present");
  }

  @Test
  public void testNeedUpdateReturnsFalseWhenColumnMetadataAbsent()
      throws Exception {
    // Column is in config but getColumnMetadataFor returns null — not yet in segment.
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("testSegment");
    when(segmentMetadata.getColumnMetadataFor(COLUMN)).thenReturn(null);

    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentDirectory.getColumnsWithIndex(StandardIndexes.ifst())).thenReturn(Set.of());

    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);

    assertFalse(createHandler(segmentDirectory).needUpdateIndices(reader),
        "No update expected when column metadata is absent");
  }

  // ---------------------------------------------------------------------------
  // No update when index is already up-to-date
  // ---------------------------------------------------------------------------

  @Test
  public void testNeedUpdateReturnsFalseWhenIndexUpToDate()
      throws Exception {
    // Column has an IFST index and is still in config — nothing to do.
    SegmentDirectory segmentDirectory = mockSegmentDirectory(COLUMN);
    SegmentDirectory.Reader reader = mockReader(segmentDirectory);

    assertFalse(createHandler(segmentDirectory).needUpdateIndices(reader),
        "No update expected when IFST index is present and matches config");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static IFSTIndexHandler createHandler(SegmentDirectory segmentDirectory) {
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.ifst(), new FstIndexConfig()).build();
    return new IFSTIndexHandler(segmentDirectory, Map.of(COLUMN, fieldIndexConfigs),
        mock(TableConfig.class), mock(Schema.class));
  }

  private static SegmentDirectory mockSegmentDirectory(String column) {
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("testSegment");

    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentDirectory.getColumnsWithIndex(StandardIndexes.ifst())).thenReturn(Set.of(column));
    return segmentDirectory;
  }

  private static SegmentDirectory.Reader mockReader(SegmentDirectory segmentDirectory) {
    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);
    return reader;
  }
}
