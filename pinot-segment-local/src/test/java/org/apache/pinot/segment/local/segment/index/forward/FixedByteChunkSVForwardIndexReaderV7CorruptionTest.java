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
package org.apache.pinot.segment.local.segment.index.forward;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriterV7;
import org.apache.pinot.segment.local.segment.index.readers.forward.ChunkReaderContext;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReaderV7;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.memory.unsafe.UnsafePinotBufferFactory;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Corrupt-header and chunk-boundary coverage for the self-describing V7 reader. Buffers use the
/// Unsafe implementation so an overflowed offset would be capable of reaching native memory if
/// the reader's subtraction-based checks regressed.
public class FixedByteChunkSVForwardIndexReaderV7CorruptionTest {

  private static final String SPEC = "DELTA";

  @Test
  public void testRejectsTruncatedFixedHeader()
      throws IOException {
    for (int size : new int[]{0, 1, 3, 4, 19, FixedByteChunkForwardIndexWriterV7.FIXED_HEADER_BYTES - 1}) {
      try (PinotDataBuffer buffer = allocate(size)) {
        expectThrows(IllegalArgumentException.class,
            () -> new FixedByteChunkSVForwardIndexReaderV7(buffer, DataType.INT));
      }
    }
  }

  @Test
  public void testRejectsOversizedCodecSpecBeforeAllocation()
      throws IOException {
    try (PinotDataBuffer buffer = newHeader(0, 1, 0, dataHeaderStart())) {
      buffer.putInt(6L * Integer.BYTES, FixedByteChunkForwardIndexWriterV7.MAX_CODEC_SPEC_LENGTH_BYTES + 1);
      expectThrows(IllegalArgumentException.class,
          () -> new FixedByteChunkSVForwardIndexReaderV7(buffer, DataType.INT));
    }
  }

  @Test
  public void testFactoryDoesNotFallBackToLegacyReaderForCorruptPipelineHeader()
      throws IOException {
    try (PinotDataBuffer buffer = newHeader(0, 1, 0, dataHeaderStart())) {
      buffer.putInt(6L * Integer.BYTES, FixedByteChunkForwardIndexWriterV7.MAX_CODEC_SPEC_LENGTH_BYTES + 1);
      IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
          () -> ForwardIndexReaderFactory.getInstance().createRawIndexReader(buffer, DataType.INT, true));
      assertTrue(error.getMessage().contains("Invalid specLength"), error.getMessage());
    }
  }

  @Test
  public void testRejectsOversizedDecodedChunkBeforeContextAllocation()
      throws IOException {
    int chunkStart = dataSectionStart(1);
    try (PinotDataBuffer buffer = newHeader(1, 1 << 25, 1,
        chunkStart + FixedByteChunkForwardIndexWriterV7.CHUNK_HEADER_BYTES)) {
      buffer.putLong(dataHeaderStart(), chunkStart);
      expectThrows(IllegalArgumentException.class,
          () -> new FixedByteChunkSVForwardIndexReaderV7(buffer, DataType.INT));
    }
  }

  @Test
  public void testRejectsOutOfFileChunkOffsetsWithoutUnsafeRead()
      throws IOException {
    int chunkStart = dataSectionStart(1);
    int bufferSize = chunkStart + FixedByteChunkForwardIndexWriterV7.CHUNK_HEADER_BYTES;
    for (long offset : new long[]{Long.MAX_VALUE, bufferSize, bufferSize - 7L}) {
      try (PinotDataBuffer buffer = newHeader(1, 1, 1, bufferSize)) {
        buffer.putLong(dataHeaderStart(), offset);
        expectThrows(IllegalArgumentException.class,
            () -> new FixedByteChunkSVForwardIndexReaderV7(buffer, DataType.INT));
      }
    }
  }

  @Test
  public void testRejectsPayloadThatCrossesNextChunk()
      throws IOException {
    int firstChunkStart = dataSectionStart(2);
    int secondChunkStart = firstChunkStart + FixedByteChunkForwardIndexWriterV7.CHUNK_HEADER_BYTES + Integer.BYTES;
    int bufferSize = secondChunkStart + FixedByteChunkForwardIndexWriterV7.CHUNK_HEADER_BYTES + Integer.BYTES;
    try (PinotDataBuffer buffer = newHeader(2, 1, 2, bufferSize)) {
      int offsets = dataHeaderStart();
      buffer.putLong(offsets, firstChunkStart);
      buffer.putLong(offsets + Long.BYTES, secondChunkStart);
      buffer.putInt(firstChunkStart, 5); // only four payload bytes fit before the next chunk
      buffer.putInt(firstChunkStart + Integer.BYTES, Integer.BYTES);

      try (FixedByteChunkSVForwardIndexReaderV7 reader =
          new FixedByteChunkSVForwardIndexReaderV7(buffer, DataType.INT);
          ChunkReaderContext context = reader.createContext()) {
        expectThrows(IllegalStateException.class, () -> reader.getInt(0, context));
      }
    }
  }

  @Test
  public void testRejectsWrongFullChunkDecodedSize()
      throws IOException {
    int chunkStart = dataSectionStart(1);
    try (PinotDataBuffer buffer = newHeader(1, 1, 1,
        chunkStart + FixedByteChunkForwardIndexWriterV7.CHUNK_HEADER_BYTES + Integer.BYTES)) {
      buffer.putLong(dataHeaderStart(), chunkStart);
      buffer.putInt(chunkStart, Integer.BYTES);
      buffer.putInt(chunkStart + Integer.BYTES, 2 * Integer.BYTES);

      try (FixedByteChunkSVForwardIndexReaderV7 reader =
          new FixedByteChunkSVForwardIndexReaderV7(buffer, DataType.INT);
          ChunkReaderContext context = reader.createContext()) {
        expectThrows(IllegalStateException.class, () -> reader.getInt(0, context));
      }
    }
  }

  @Test
  public void testRejectsWrongPartialChunkDecodedSize()
      throws IOException {
    int firstChunkStart = dataSectionStart(2);
    int secondChunkStart = firstChunkStart + FixedByteChunkForwardIndexWriterV7.CHUNK_HEADER_BYTES + Integer.BYTES;
    int bufferSize = secondChunkStart + FixedByteChunkForwardIndexWriterV7.CHUNK_HEADER_BYTES + Integer.BYTES;
    try (PinotDataBuffer buffer = newHeader(2, 4, 5, bufferSize)) {
      int offsets = dataHeaderStart();
      buffer.putLong(offsets, firstChunkStart);
      buffer.putLong(offsets + Long.BYTES, secondChunkStart);
      buffer.putInt(secondChunkStart, Integer.BYTES);
      buffer.putInt(secondChunkStart + Integer.BYTES, 4 * Integer.BYTES); // final chunk contains one value

      try (FixedByteChunkSVForwardIndexReaderV7 reader =
          new FixedByteChunkSVForwardIndexReaderV7(buffer, DataType.INT);
          ChunkReaderContext context = reader.createContext()) {
        expectThrows(IllegalStateException.class, () -> reader.getInt(4, context));
      }
    }
  }

  @Test
  public void testRejectsPayloadLargerThanComposedPipelineBound()
      throws IOException {
    int chunkStart = dataSectionStart(1);
    int encodedSize = Integer.BYTES + 1;
    int bufferSize = chunkStart + FixedByteChunkForwardIndexWriterV7.CHUNK_HEADER_BYTES + encodedSize;
    try (PinotDataBuffer buffer = newHeader(1, 1, 1, bufferSize)) {
      buffer.putLong(dataHeaderStart(), chunkStart);
      buffer.putInt(chunkStart, encodedSize);
      buffer.putInt(chunkStart + Integer.BYTES, Integer.BYTES);

      try (FixedByteChunkSVForwardIndexReaderV7 reader =
          new FixedByteChunkSVForwardIndexReaderV7(buffer, DataType.INT);
          ChunkReaderContext context = reader.createContext()) {
        IllegalStateException error = expectThrows(IllegalStateException.class, () -> reader.getInt(0, context));
        assertTrue(error.getMessage().contains("exceeds pipeline bound"), error.getMessage());
      }
    }
  }

  @Test
  public void testReaderRejectsPipelineWhoseComposedBoundIsUnsafeBeforeContextAllocation()
      throws IOException {
    String spec = "CODEC(" + String.join(",", Collections.nCopies(32, "SNAPPY")) + ")";
    try (PinotDataBuffer buffer = newHeader(spec, 0, 1 << 22, 0, dataHeaderStart(spec))) {
      IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
          () -> new FixedByteChunkSVForwardIndexReaderV7(buffer, DataType.INT));
      assertTrue(error.getMessage().contains("cannot safely bound"), error.getMessage());
    }
  }

  private static PinotDataBuffer newHeader(int numChunks, int numDocsPerChunk, int totalDocs, int size) {
    return newHeader(SPEC, numChunks, numDocsPerChunk, totalDocs, size);
  }

  private static PinotDataBuffer newHeader(String specText, int numChunks, int numDocsPerChunk, int totalDocs,
      int size) {
    PinotDataBuffer buffer = allocate(size);
    byte[] spec = specText.getBytes(StandardCharsets.UTF_8);
    buffer.putInt(0, FixedByteChunkSVForwardIndexReaderV7.VERSION);
    buffer.putInt(Integer.BYTES, FixedByteChunkForwardIndexWriterV7.FORMAT_MAGIC);
    buffer.putInt(2L * Integer.BYTES, numChunks);
    buffer.putInt(3L * Integer.BYTES, numDocsPerChunk);
    buffer.putInt(4L * Integer.BYTES, Integer.BYTES);
    buffer.putInt(5L * Integer.BYTES, totalDocs);
    buffer.putInt(6L * Integer.BYTES, spec.length);
    buffer.putInt(7L * Integer.BYTES, dataHeaderStart(specText));
    buffer.readFrom(FixedByteChunkForwardIndexWriterV7.FIXED_HEADER_BYTES, spec);
    return buffer;
  }

  private static int dataHeaderStart() {
    return dataHeaderStart(SPEC);
  }

  private static int dataHeaderStart(String spec) {
    return FixedByteChunkForwardIndexWriterV7.FIXED_HEADER_BYTES
        + spec.getBytes(StandardCharsets.UTF_8).length;
  }

  private static int dataSectionStart(int numChunks) {
    return dataHeaderStart() + numChunks * Long.BYTES;
  }

  private static PinotDataBuffer allocate(int size) {
    return new UnsafePinotBufferFactory().allocateDirect(size, ByteOrder.BIG_ENDIAN);
  }
}
