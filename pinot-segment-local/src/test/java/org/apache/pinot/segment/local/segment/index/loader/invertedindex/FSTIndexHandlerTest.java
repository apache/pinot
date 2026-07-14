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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FstIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Unit tests for [FSTIndexHandler].
///
/// Covers:
/// - needUpdateIndices returns true when legacy native FST is detected (triggers rebuild)
/// - updateIndices removes the legacy native FST index
/// - needUpdateIndices returns true when a column is dropped from the FST index config
/// - updateIndices removes the on-disk index when a column is dropped from config
/// - needUpdateIndices returns true when a new column is added to the config
/// - needUpdateIndices returns false when the index is already present and matches config
/// - needUpdateIndices throws UnsupportedOperationException for non-STRING, no-dict, or MV columns
/// - updateIndices creates a new FST index file for a newly configured column
public class FSTIndexHandlerTest {
  private static final String COLUMN = "name";

  /**
   * Magic int written at offset 0 of every legacy native FST file.
   * Mirrors the private constant in {@code FstIndexUtils}.
   */
  private static final int LEGACY_NATIVE_FST_MAGIC = ('\\' << 24) | ('f' << 16) | ('s' << 8) | 'a';

  /** [FieldIndexConfigs] with FST explicitly disabled — used in removed-column tests. */
  private static final FieldIndexConfigs NO_FST =
      new FieldIndexConfigs.Builder().add(StandardIndexes.fst(), FstIndexConfig.DISABLED).build();

  @Test
  public void testNeedUpdateReturnsTrueWhenLegacyNativeFstDetected()
      throws Exception {
    SegmentDirectory segmentDirectory = mockSegmentDirectory(COLUMN);
    SegmentDirectory.Reader reader = mockReaderWithFstBuffer(segmentDirectory, COLUMN, legacyNativeBuffer());

    FSTIndexHandler handler = createHandler(segmentDirectory);

    assertTrue(handler.needUpdateIndices(reader),
        "Rebuild expected when legacy native FST index is detected");
  }

  @Test
  public void testUpdateIndicesRemovesLegacyNativeFstIndex()
      throws Exception {
    SegmentDirectory segmentDirectory = mockSegmentDirectory(COLUMN);
    SegmentDirectory.Writer writer = mockWriterWithFstBuffer(segmentDirectory, COLUMN, legacyNativeBuffer());

    FSTIndexHandler handler = createHandler(segmentDirectory);
    handler.updateIndices(writer);

    verify(writer).removeIndex(COLUMN, StandardIndexes.fst());
  }

  @Test
  public void testNeedUpdateReturnsTrueWhenColumnRemovedFromConfig()
      throws Exception {
    // Column has an FST index on disk but the new config has no FST index for it.
    SegmentDirectory segmentDirectory = mockSegmentDirectory(COLUMN);
    SegmentDirectory.Reader reader = mockReaderWithFstBuffer(segmentDirectory, COLUMN, nonLegacyBuffer());

    FSTIndexHandler handler = new FSTIndexHandler(segmentDirectory, Map.of(COLUMN, NO_FST),
        mock(TableConfig.class), mock(Schema.class));

    assertTrue(handler.needUpdateIndices(reader),
        "Rebuild expected to remove index when column is dropped from FST index config");
  }

  @Test
  public void testUpdateIndicesRemovesIndexWhenColumnDroppedFromConfig()
      throws Exception {
    SegmentDirectory segmentDirectory = mockSegmentDirectory(COLUMN);
    SegmentDirectory.Writer writer = mockWriterWithFstBuffer(segmentDirectory, COLUMN, nonLegacyBuffer());

    FSTIndexHandler handler = new FSTIndexHandler(segmentDirectory, Map.of(COLUMN, NO_FST),
        mock(TableConfig.class), mock(Schema.class));
    handler.updateIndices(writer);

    verify(writer).removeIndex(COLUMN, StandardIndexes.fst());
  }

  @Test
  public void testNeedUpdateReturnsTrueWhenNewColumnAdded()
      throws Exception {
    // Column is in config but has no existing FST index.
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("testSegment");
    when(segmentMetadata.getTotalDocs()).thenReturn(1);
    when(segmentMetadata.getAllColumns()).thenReturn(new TreeSet<>(Set.of(COLUMN)));
    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
    when(columnMetadata.getColumnName()).thenReturn(COLUMN);
    when(columnMetadata.getDataType()).thenReturn(FieldSpec.DataType.STRING);
    when(columnMetadata.hasDictionary()).thenReturn(true);
    when(columnMetadata.isSingleValue()).thenReturn(true);
    when(segmentMetadata.getColumnMetadataFor(COLUMN)).thenReturn(columnMetadata);

    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentDirectory.getColumnsWithIndex(StandardIndexes.fst())).thenReturn(Set.of());

    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);

    FSTIndexHandler handler = createHandler(segmentDirectory);

    assertTrue(handler.needUpdateIndices(reader),
        "New FST index expected for column added to config");
  }

  @Test
  public void testNeedUpdateReturnsFalseWhenIndexUpToDate()
      throws Exception {
    // Column has a Lucene FST index, is in config, and the buffer is not a legacy native index.
    SegmentDirectory segmentDirectory = mockSegmentDirectory(COLUMN);
    SegmentDirectory.Reader reader = mockReaderWithFstBuffer(segmentDirectory, COLUMN, nonLegacyBuffer());

    FSTIndexHandler handler = createHandler(segmentDirectory);

    assertFalse(handler.needUpdateIndices(reader),
        "No rebuild expected when FST index is current and matches config");
  }

  @Test
  public void testNeedUpdateThrowsForNonStringColumn()
      throws Exception {
    SegmentDirectory segmentDirectory = mockSegmentDirectoryForNewColumn(FieldSpec.DataType.INT, true, true);
    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);

    FSTIndexHandler handler = createHandler(segmentDirectory);

    assertThrows(UnsupportedOperationException.class, () -> handler.needUpdateIndices(reader));
  }

  @Test
  public void testNeedUpdateThrowsWhenNoDictionary()
      throws Exception {
    SegmentDirectory segmentDirectory = mockSegmentDirectoryForNewColumn(FieldSpec.DataType.STRING, false, true);
    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);

    FSTIndexHandler handler = createHandler(segmentDirectory);

    assertThrows(UnsupportedOperationException.class, () -> handler.needUpdateIndices(reader));
  }

  @Test
  public void testNeedUpdateThrowsForMultiValueColumn()
      throws Exception {
    SegmentDirectory segmentDirectory = mockSegmentDirectoryForNewColumn(FieldSpec.DataType.STRING, true, false);
    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);

    FSTIndexHandler handler = createHandler(segmentDirectory);

    assertThrows(UnsupportedOperationException.class, () -> handler.needUpdateIndices(reader));
  }

  @Test
  public void testUpdateIndicesCreatesNewFstIndexForNewColumn()
      throws Exception {
    File indexDir = buildMinimalSegment();
    try {
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable_OFFLINE").build();
      Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(COLUMN, FieldSpec.DataType.STRING).build();
      FieldIndexConfigs fieldIndexConfigs =
          new FieldIndexConfigs.Builder().add(StandardIndexes.fst(), new FstIndexConfig()).build();

      try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, ReadMode.mmap);
          SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
        // Writer extends Reader, so it can be used for both the pre-check and the update.
        FSTIndexHandler handler = new FSTIndexHandler(segmentDirectory, Map.of(COLUMN, fieldIndexConfigs),
            tableConfig, schema);
        assertTrue(handler.needUpdateIndices(writer), "New column should require FST index creation");
        handler.updateIndices(writer);
      }

      assertTrue(new File(indexDir, COLUMN + V1Constants.Indexes.LUCENE_V912_FST_INDEX_FILE_EXTENSION).exists(),
          "FST index file should be created for a newly configured column");
    } finally {
      FileUtils.deleteQuietly(indexDir.getParentFile());
    }
  }

  private static FSTIndexHandler createHandler(SegmentDirectory segmentDirectory) {
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.fst(), new FstIndexConfig()).build();
    return new FSTIndexHandler(segmentDirectory, Map.of(COLUMN, fieldIndexConfigs),
        mock(TableConfig.class), mock(Schema.class));
  }

  private static SegmentDirectory mockSegmentDirectory(String column) {
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("testSegment");
    when(segmentMetadata.getTotalDocs()).thenReturn(1);
    when(segmentMetadata.getAllColumns()).thenReturn(new TreeSet<>(Set.of(column)));

    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentDirectory.getColumnsWithIndex(StandardIndexes.fst())).thenReturn(Set.of(column));
    return segmentDirectory;
  }

  /**
   * Mocks a {@link SegmentDirectory} where {@code COLUMN} has no existing FST index but is
   * configured to receive one — used by the unsupported-operation tests.
   */
  private static SegmentDirectory mockSegmentDirectoryForNewColumn(
      FieldSpec.DataType dataType, boolean hasDictionary, boolean isSingleValue) {
    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
    when(columnMetadata.getColumnName()).thenReturn(COLUMN);
    when(columnMetadata.getDataType()).thenReturn(dataType);
    when(columnMetadata.hasDictionary()).thenReturn(hasDictionary);
    when(columnMetadata.isSingleValue()).thenReturn(isSingleValue);

    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("testSegment");
    when(segmentMetadata.getTotalDocs()).thenReturn(1);
    when(segmentMetadata.getAllColumns()).thenReturn(new TreeSet<>(Set.of(COLUMN)));
    when(segmentMetadata.getColumnMetadataFor(COLUMN)).thenReturn(columnMetadata);

    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentDirectory.getColumnsWithIndex(StandardIndexes.fst())).thenReturn(Set.of());
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

  /**
   * Builds a minimal v1 segment with a single dictionary-encoded STRING column ({@code COLUMN}).
   *
   * @return the segment directory (tempDir/segmentName)
   */
  private static File buildMinimalSegment()
      throws Exception {
    File tempDir = new File(FileUtils.getTempDirectory(), "fst-index-handler-test-" + System.nanoTime());
    FileUtils.deleteQuietly(tempDir);
    if (!tempDir.mkdirs()) {
      throw new IOException("Failed to create temp directory: " + tempDir);
    }

    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(COLUMN, FieldSpec.DataType.STRING).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable_OFFLINE").build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(tempDir.getAbsolutePath());
    config.setSegmentName("fst-handler-test-segment");
    config.setSegmentVersion(SegmentVersion.v1);

    List<GenericRow> rows = new ArrayList<>();
    for (String value : List.of("apple", "banana", "cherry")) {
      GenericRow row = new GenericRow();
      row.putValue(COLUMN, value);
      rows.add(row);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    return new File(tempDir, "fst-handler-test-segment");
  }
}
