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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Unit tests for the [LegacyRawValueInvertedIndexCleanup#isLegacyRawValueInvertedIndexFormat] format detector.
/// The end-to-end rebuild path (preprocessor detects legacy bytes, drops the file, handler rebuilds in modern
/// format) is covered by `LegacyRawValueInvertedIndexMigrationIntegrationTest` against real v1 and v3 fixtures,
/// so this file only pins down the byte-pattern detector itself.
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

      assertTrue(LegacyRawValueInvertedIndexCleanup.isLegacyRawValueInvertedIndexFormat(dataBuffer, columnMetadata));
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

      assertFalse(LegacyRawValueInvertedIndexCleanup.isLegacyRawValueInvertedIndexFormat(dataBuffer, columnMetadata));
    } finally {
      FileUtils.deleteQuietly(indexFile);
    }
  }
}
