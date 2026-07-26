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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkForwardIndexReaderV4;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkForwardIndexReaderV6;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Verifies that `enforceTargetDocsPerChunk` reaches the V6 writer through
/// [SingleValueVarByteRawIndexCreator] and actually bounds a chunk by document count.
///
/// The interesting case is a column whose longest value dwarfs its average: the byte-size derivation
/// `max(min(maxLength * targetDocsPerChunk, targetMaxChunkSizeBytes), 4KB)` clamps to
/// `targetMaxChunkSizeBytes` and stops tracking `targetDocsPerChunk` at all. Enforcement restores it.
public class EnforceTargetDocsPerChunkTest {
  private static final int WRITER_VERSION = 6;
  private static final int NUM_DOCS = 5000;
  private static final int TARGET_DOCS_PER_CHUNK = 100;
  private static final int TARGET_MAX_CHUNK_SIZE = 1024 * 1024;
  private static final String COLUMN = "skewedColumn";

  private File _indexDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _indexDir = new File(FileUtils.getTempDirectory(), "EnforceTargetDocsPerChunkTest");
    FileUtils.deleteQuietly(_indexDir);
    FileUtils.forceMkdir(_indexDir);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_indexDir);
  }

  @Test
  public void enforcedCapBoundsChunkByDocCount()
      throws IOException {
    String[] values = skewedValues();
    write(values, true);
    assertEquals(numChunks(), NUM_DOCS / TARGET_DOCS_PER_CHUNK, "Each chunk should hold exactly the target doc count");
    assertRoundTrips(values);
  }

  @Test
  public void withoutEnforcementByteDerivationIgnoresDocCount()
      throws IOException {
    String[] values = skewedValues();
    write(values, false);
    // maxLength * targetDocsPerChunk exceeds targetMaxChunkSize, so the chunk size clamps to 1MB and each chunk
    // holds far more than TARGET_DOCS_PER_CHUNK docs.
    int chunks = numChunks();
    assertTrue(chunks < NUM_DOCS / TARGET_DOCS_PER_CHUNK,
        "Expected fewer chunks than the doc-count target implies, got " + chunks);
    assertRoundTrips(values);
  }

  /// Short values with a few large outliers, so `maxLength` is ~500x the average length.
  private static String[] skewedValues() {
    String[] values = new String[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      values[i] = i % 500 == 0 ? "x".repeat(50_000) : "short-value-" + i;
    }
    return values;
  }

  private void write(String[] values, boolean enforceTargetDocsPerChunk)
      throws IOException {
    int maxLength = 0;
    for (String value : values) {
      maxLength = Math.max(maxLength, value.getBytes(StandardCharsets.UTF_8).length);
    }
    try (SingleValueVarByteRawIndexCreator creator = new SingleValueVarByteRawIndexCreator(_indexDir,
        ChunkCompressionType.ZSTANDARD, COLUMN, NUM_DOCS, DataType.STRING, maxLength, false, WRITER_VERSION,
        TARGET_MAX_CHUNK_SIZE, TARGET_DOCS_PER_CHUNK, enforceTargetDocsPerChunk)) {
      for (String value : values) {
        creator.putString(value);
      }
    }
  }

  private File indexFile() {
    return new File(_indexDir, COLUMN + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
  }

  /// The V4-family header is `[version][targetChunkSize][compressionType][chunksStartOffset]` followed by an
  /// 8-byte `[firstDocId][chunkStartOffset]` entry per chunk.
  private int numChunks()
      throws IOException {
    try (PinotDataBuffer buffer = PinotDataBuffer.loadBigEndianFile(indexFile())) {
      int chunksStartOffset = buffer.getInt(3 * Integer.BYTES);
      return (chunksStartOffset - 4 * Integer.BYTES) / (2 * Integer.BYTES);
    }
  }

  private void assertRoundTrips(String[] values)
      throws IOException {
    try (PinotDataBuffer buffer = PinotDataBuffer.loadBigEndianFile(indexFile());
        VarByteChunkForwardIndexReaderV6 reader = new VarByteChunkForwardIndexReaderV6(buffer, DataType.STRING, true);
        VarByteChunkForwardIndexReaderV4.ReaderContext context = reader.createContext()) {
      for (int i = 0; i < values.length; i++) {
        assertEquals(reader.getString(i, context), values[i], "Mismatch at docId " + i);
      }
    }
  }
}
