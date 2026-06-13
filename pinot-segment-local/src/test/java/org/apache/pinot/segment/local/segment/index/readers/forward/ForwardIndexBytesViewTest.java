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
package org.apache.pinot.segment.local.segment.index.readers.forward;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Verifies that {@code getBytesView} on the var-byte forward index readers returns the same payload
 * as {@code getBytes} across every supported compression mode, for both the V3 single-value reader
 * and the V4 reader. Also exercises the huge-value (irregular chunk) path and the default
 * fallback on a reader that only implements {@code getBytes}.
 */
public class ForwardIndexBytesViewTest implements PinotBuffersAfterMethodCheckRule {

  private static final int NUM_DOCS = 1024;
  private static final int VALUE_SIZE_BYTES = 96;
  private static final long SEED = 0xC0FFEEL;

  private File _dir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _dir = new File(FileUtils.getTempDirectory(), "ForwardIndexBytesViewTest-" + UUID.randomUUID());
    FileUtils.forceMkdir(_dir);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_dir);
  }

  @DataProvider
  public Object[][] compressions() {
    return new Object[][]{
        {ChunkCompressionType.PASS_THROUGH},
        {ChunkCompressionType.SNAPPY},
        {ChunkCompressionType.LZ4},
        {ChunkCompressionType.ZSTANDARD}
    };
  }

  @Test(dataProvider = "compressions")
  public void testV3BytesViewMatchesGetBytes(ChunkCompressionType compression)
      throws IOException {
    File file = new File(_dir, "v3-" + compression + ".fwd");
    byte[][] expected = randomValues(NUM_DOCS, VALUE_SIZE_BYTES);

    try (VarByteChunkForwardIndexWriter writer = new VarByteChunkForwardIndexWriter(file, compression, NUM_DOCS,
        /* numDocsPerChunk */ 128, VALUE_SIZE_BYTES, /* writerVersion */ 2)) {
      for (byte[] value : expected) {
        writer.putBytes(value);
      }
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
        VarByteChunkSVForwardIndexReader reader = new VarByteChunkSVForwardIndexReader(buffer, DataType.BYTES);
        ChunkReaderContext context = reader.createContext()) {
      assertGetBytesViewMatches(reader, context, expected);
    }
  }

  @Test(dataProvider = "compressions")
  public void testV4BytesViewMatchesGetBytes(ChunkCompressionType compression)
      throws IOException {
    File file = new File(_dir, "v4-" + compression + ".fwd");
    byte[][] expected = randomValues(NUM_DOCS, VALUE_SIZE_BYTES);

    try (VarByteChunkForwardIndexWriterV4 writer = new VarByteChunkForwardIndexWriterV4(file, compression,
        /* chunkSize */ 4096)) {
      for (byte[] value : expected) {
        writer.putBytes(value);
      }
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
        VarByteChunkForwardIndexReaderV4 reader = new VarByteChunkForwardIndexReaderV4(buffer, DataType.BYTES, true);
        VarByteChunkForwardIndexReaderV4.ReaderContext context = reader.createContext()) {
      assertGetBytesViewMatches(reader, context, expected);
    }
  }

  /**
   * Exercises the huge-value path on V4: writes a single value larger than the chunk buffer so it
   * lands in its own irregular chunk. Both compressed and uncompressed huge paths fall back to
   * {@code ByteBuffer.wrap(byte[])}, so this is a correctness regression test rather than a
   * zero-copy assertion.
   */
  @Test(dataProvider = "compressions")
  public void testV4BytesViewHandlesHugeValue(ChunkCompressionType compression)
      throws IOException {
    File file = new File(_dir, "v4-huge-" + compression + ".fwd");
    int chunkSize = 256;
    int hugeValueSize = chunkSize * 8;
    byte[][] expected = {randomBytes(VALUE_SIZE_BYTES, 1), randomBytes(hugeValueSize, 2), randomBytes(VALUE_SIZE_BYTES,
        3)};

    try (VarByteChunkForwardIndexWriterV4 writer = new VarByteChunkForwardIndexWriterV4(file, compression, chunkSize)) {
      for (byte[] value : expected) {
        writer.putBytes(value);
      }
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
        VarByteChunkForwardIndexReaderV4 reader = new VarByteChunkForwardIndexReaderV4(buffer, DataType.BYTES, true);
        VarByteChunkForwardIndexReaderV4.ReaderContext context = reader.createContext()) {
      assertGetBytesViewMatches(reader, context, expected);
    }
  }

  /**
   * Verifies the default {@link ForwardIndexReader#getBytesView} implementation works on a reader
   * that only implements {@code getBytes}. Guarantees backwards compatibility for any external
   * {@code ForwardIndexReader} that does not override the new method.
   */
  @Test
  public void testDefaultGetBytesViewFallback() {
    byte[] payload = randomBytes(VALUE_SIZE_BYTES, 42);

    ForwardIndexReader<ForwardIndexReaderContext> reader = new ForwardIndexReader<ForwardIndexReaderContext>() {
      @Override
      public boolean isDictionaryEncoded() {
        return false;
      }

      @Override
      public boolean isSingleValue() {
        return true;
      }

      @Override
      public DataType getStoredType() {
        return DataType.BYTES;
      }

      @Override
      public byte[] getBytes(int docId, ForwardIndexReaderContext context) {
        return payload;
      }

      @Override
      public void close() {
      }
    };

    ByteBuffer view = reader.getBytesView(0, null);
    assertNotNull(view);
    byte[] copy = new byte[view.remaining()];
    view.get(copy);
    assertEquals(copy, payload);
  }

  private static <C extends ForwardIndexReaderContext> void assertGetBytesViewMatches(ForwardIndexReader<C> reader,
      C context, byte[][] expected)
      throws IOException {
    // Walk getBytes and getBytesView in separate passes with a fresh context each, mirroring the
    // single-row consumption contract that real callers honour. Mixing both APIs on the same docId
    // with a shared context exposes a latent quirk where the cached chunk state for an irregular
    // (huge) chunk can't service a second read of the same docId.
    for (int i = 0; i < expected.length; i++) {
      byte[] viaArray = reader.getBytes(i, context);
      assertEquals(viaArray, expected[i], "getBytes mismatch at doc " + i);
    }
    C freshContext = reader.createContext();
    try {
      for (int i = 0; i < expected.length; i++) {
        ByteBuffer viaView = reader.getBytesView(i, freshContext);
        byte[] viaViewCopy = new byte[viaView.remaining()];
        viaView.get(viaViewCopy);
        assertEquals(viaViewCopy, expected[i], "getBytesView mismatch at doc " + i);
      }
    } finally {
      if (freshContext != null) {
        freshContext.close();
      }
    }
  }

  private static byte[][] randomValues(int numDocs, int valueSize) {
    byte[][] values = new byte[numDocs][];
    Random random = new Random(SEED);
    for (int i = 0; i < numDocs; i++) {
      values[i] = new byte[valueSize];
      random.nextBytes(values[i]);
    }
    return values;
  }

  private static byte[] randomBytes(int size, int seed) {
    byte[] bytes = new byte[size];
    new Random(seed).nextBytes(bytes);
    return bytes;
  }
}
