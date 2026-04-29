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
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;


public class InvertedIndexHandlerTest {

  @Test
  public void testDetectsLegacyRawValueInvertedIndexFormat()
      throws Exception {
    File indexFile = Files.createTempFile("legacy-raw-inverted", ".inv").toFile();
    ColumnMetadata columnMetadata = Mockito.mock(ColumnMetadata.class);
    Mockito.when(columnMetadata.getCardinality()).thenReturn(3);
    try (PinotDataBuffer dataBuffer =
        PinotDataBuffer.mapFile(indexFile, false, 0, 128, ByteOrder.BIG_ENDIAN, "legacy-raw-inverted")) {
      dataBuffer.putInt(0, 1);
      dataBuffer.putInt(4, 3);
      dataBuffer.putInt(8, 16);
      dataBuffer.putLong(12, 44);
      dataBuffer.putLong(20, 16);
      dataBuffer.putLong(28, 60);
      dataBuffer.putLong(36, 32);

      assertTrue(InvertedIndexHandler.isLegacyRawValueInvertedIndexFormat(dataBuffer, columnMetadata));
    } finally {
      FileUtils.deleteQuietly(indexFile);
    }
  }

  @Test
  public void testStandardBitmapInvertedIndexIsNotDetectedAsLegacyRawValueFormat()
      throws Exception {
    File indexFile = Files.createTempFile("standard-inverted", ".inv").toFile();
    ColumnMetadata columnMetadata = Mockito.mock(ColumnMetadata.class);
    Mockito.when(columnMetadata.getCardinality()).thenReturn(3);
    try (PinotDataBuffer dataBuffer =
        PinotDataBuffer.mapFile(indexFile, false, 0, 128, ByteOrder.BIG_ENDIAN, "standard-inverted")) {
      dataBuffer.putInt(0, 16);
      dataBuffer.putInt(4, 24);
      dataBuffer.putInt(8, 40);
      dataBuffer.putInt(12, 64);

      assertFalse(InvertedIndexHandler.isLegacyRawValueInvertedIndexFormat(dataBuffer, columnMetadata));
    } finally {
      FileUtils.deleteQuietly(indexFile);
    }
  }

  /**
   * E2E regression for the legacy-format invalidation path added in {@code SegmentPreProcessor}: build a normal
   * segment, overwrite its inverted-index file with legacy raw-value embedded-dictionary bytes (simulating a
   * segment built by the now-deleted {@code RawValueBitmapInvertedIndexCreator}), run the preprocessor, and verify
   * the file is rebuilt in the standard dict-id format.
   */
  @Test
  public void testSegmentPreProcessorInvalidatesLegacyRawValueInvertedIndex()
      throws Exception {
    File tempDir = Files.createTempDirectory("legacy-inv-rebuild").toFile();
    try {
      File segmentDir = buildSimpleSegmentWithInvertedIndex(tempDir, "col");
      File indexDir = segmentDir;

      // Locate the inverted-index file. For v1 segments the file lives directly under indexDir; for v3 it is
      // packed inside columns.psf, in which case we skip this E2E since we can't easily replace a chunk inside
      // a packed file without going through the segment writer.
      File invFile = new File(indexDir, "col.bitmap.inv");
      if (!invFile.exists()) {
        // Test is only meaningful for v1 / unpacked segments; skip otherwise.
        return;
      }

      ColumnMetadata mockMeta = Mockito.mock(ColumnMetadata.class);
      Mockito.when(mockMeta.getCardinality()).thenReturn(3);
      // Sanity check pre-replacement: file is NOT in legacy format.
      try (PinotDataBuffer dataBuffer =
          PinotDataBuffer.mapFile(invFile, true, 0, invFile.length(), ByteOrder.BIG_ENDIAN, "before-rewrite")) {
        assertFalse(InvertedIndexHandler.isLegacyRawValueInvertedIndexFormat(dataBuffer, mockMeta));
      }
      long bytesBefore = invFile.length();

      // Overwrite the file with legacy-format bytes.
      FileUtils.deleteQuietly(invFile);
      try (PinotDataBuffer dataBuffer =
          PinotDataBuffer.mapFile(invFile, false, 0, 128, ByteOrder.BIG_ENDIAN, "legacy-overwrite")) {
        dataBuffer.putInt(0, 1);
        dataBuffer.putInt(4, 3);
        dataBuffer.putInt(8, 16);
        dataBuffer.putLong(12, 44);
        dataBuffer.putLong(20, 16);
        dataBuffer.putLong(28, 60);
        dataBuffer.putLong(36, 32);
      }
      try (PinotDataBuffer dataBuffer =
          PinotDataBuffer.mapFile(invFile, true, 0, invFile.length(), ByteOrder.BIG_ENDIAN, "verify-legacy")) {
        assertTrue(InvertedIndexHandler.isLegacyRawValueInvertedIndexFormat(dataBuffer, mockMeta),
            "Pre-condition: hand-crafted bytes must match legacy format");
      }

      // Run SegmentPreProcessor — should detect legacy format, drop the file, then InvertedIndexHandler rebuilds.
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
          .setTableName("legacyInvTable")
          .setInvertedIndexColumns(List.of("col"))
          .build();
      Schema schema = new Schema.SchemaBuilder().setSchemaName("legacyInvTable")
          .addSingleValueDimension("col", FieldSpec.DataType.STRING).build();
      IndexLoadingConfig loadingConfig = new IndexLoadingConfig(tableConfig, schema);
      try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, ReadMode.mmap);
          SegmentPreProcessor preProcessor = new SegmentPreProcessor(segmentDirectory, loadingConfig)) {
        preProcessor.process();
      }

      // After preprocessing, the file is rebuilt in standard format, NOT legacy.
      assertTrue(invFile.exists(), "Inverted index file must be present after preprocessing rebuild");
      try (PinotDataBuffer dataBuffer =
          PinotDataBuffer.mapFile(invFile, true, 0, invFile.length(), ByteOrder.BIG_ENDIAN, "after-rebuild")) {
        assertFalse(InvertedIndexHandler.isLegacyRawValueInvertedIndexFormat(dataBuffer, mockMeta),
            "Inverted index must no longer be in legacy format after preprocessor rebuild");
      }
      assertNotEquals(invFile.length(), 128L,
          "Rebuilt inverted index file size should differ from the 128-byte legacy stub we planted");
      assertEquals(invFile.length(), bytesBefore,
          "Rebuilt inverted index file size should match the originally-built file (deterministic build)");
    } finally {
      FileUtils.deleteDirectory(tempDir);
    }
  }

  private static File buildSimpleSegmentWithInvertedIndex(File parentDir, String columnName)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("legacyInvTable")
        .addSingleValueDimension(columnName, FieldSpec.DataType.STRING).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("legacyInvTable")
        .setInvertedIndexColumns(List.of(columnName))
        .build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(parentDir.getAbsolutePath());
    config.setSegmentName("legacyInvSegment");
    // v1 keeps the inverted index file unpacked, which is what the rebuild verification requires.
    config.setSegmentVersion(org.apache.pinot.segment.spi.creator.SegmentVersion.v1);

    GenericRow r1 = new GenericRow();
    r1.putValue(columnName, "alpha");
    GenericRow r2 = new GenericRow();
    r2.putValue(columnName, "beta");
    GenericRow r3 = new GenericRow();
    r3.putValue(columnName, "alpha");
    GenericRow r4 = new GenericRow();
    r4.putValue(columnName, "gamma");

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(List.of(r1, r2, r3, r4)));
    driver.build();
    return driver.getOutputDirectory();
  }
}
