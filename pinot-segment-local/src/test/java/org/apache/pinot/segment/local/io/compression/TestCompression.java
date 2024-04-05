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
package org.apache.pinot.segment.local.io.compression;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;
import org.apache.pinot.segment.spi.compression.ChunkDecompressor;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;


public class TestCompression {

  @DataProvider
  public Object[][] formats() {
    byte[] input = "testing123".getBytes(StandardCharsets.UTF_8);
    ByteBuffer buffer = ByteBuffer.allocateDirect(input.length);
    buffer.put(input);
    buffer.flip();
    return new Object[][]{
        {ChunkCompressionType.PASS_THROUGH, buffer.slice()}, {ChunkCompressionType.SNAPPY, buffer.slice()},
        {ChunkCompressionType.LZ4, buffer.slice()}, {ChunkCompressionType.LZ4_LENGTH_PREFIXED, buffer.slice()},
        {ChunkCompressionType.ZSTANDARD, buffer.slice()}, {ChunkCompressionType.GZIP, buffer.slice()}
    };
  }

  @Test(dataProvider = "formats")
  public void testRoundtrip(ChunkCompressionType type, ByteBuffer rawInput)
      throws IOException {
    try (ChunkCompressor compressor = ChunkCompressorFactory.getCompressor(type)) {
      assertEquals(compressor.compressionType(), type, "upgrade is opt in");
      roundtrip(compressor, rawInput);
    }
  }

  @Test(dataProvider = "formats")
  public void testRoundtripWithUpgrade(ChunkCompressionType type, ByteBuffer rawInput)
      throws IOException {
    try (ChunkCompressor compressor = ChunkCompressorFactory.getCompressor(type, true)) {
      assertNotEquals(compressor.compressionType(), ChunkCompressionType.LZ4,
          "LZ4 compression type does not support length prefix");
      roundtrip(compressor, rawInput);
    }
  }

  @Test(dataProvider = "formats")
  public void testConcurrent(ChunkCompressionType type, ByteBuffer ignore) {

    String expected = "The gzip file format is:\n"
        + "- a 10-byte header, containing a magic number (1f 8b), the compression method (08 for DEFLATE), "
        + "1-byte of header flags, a 4-byte timestamp, compression flags and the operating system ID.\n"
        + "- optional extra headers as allowed by the header flags, including the original filename, a "
        + "comment field, an 'extra' field, and the lower half of a CRC-32 checksum for the header section.\n"
        + "- a body, containing a DEFLATE-compressed payload.\n"
        + "- an 8-byte trailer, containing a CRC-32 checksum and the length of the original uncompressed "
        + "data, modulo 232.[4]\n"
        + "gzip is normally used to compress just single files. Compressed archives are typically created "
        + "by assembling collections of files into a single tar archive and then compressing that archive "
        + "with gzip.\n gzip is not to be confused with ZIP, which can hold collections of files without "
        + "an external archiver, but is less compact than compressed tarballs holding the same data, because "
        + "it compresses files individually and cannot take advantage of redundancy between files.\n\n";
    byte[] input = expected.getBytes(StandardCharsets.UTF_8);
    ByteBuffer rawInput = ByteBuffer.allocateDirect(input.length).put(input).flip();

    Thread[] workers = new Thread[5];
    ByteBuffer[] compressed = new ByteBuffer[workers.length];
    ByteBuffer[] decompressed = new ByteBuffer[workers.length];
    CountDownLatch done = new CountDownLatch(workers.length);
    AtomicInteger errors = new AtomicInteger();
    for (int i = 0; i < workers.length; i++) {
      int idx = i;
      workers[i] = new Thread(() -> {
        try {
          // compress
          try (ChunkCompressor compressor = ChunkCompressorFactory.getCompressor(type)) {
            compressed[idx] = ByteBuffer.allocateDirect(compressor.maxCompressedSize(rawInput.limit()));
            compressor.compress(rawInput.slice(), compressed[idx]);
          }

          // small context switch
          TimeUnit.MILLISECONDS.sleep(1L + (long) (ThreadLocalRandom.current().nextDouble() * 10.0));

          // decompress
          try (ChunkDecompressor decompressor = ChunkCompressorFactory.getDecompressor(type)) {
            int size = decompressor.decompressedLength(compressed[idx]);
            if (type == ChunkCompressionType.LZ4) {
              size = rawInput.limit();
            }
            decompressed[idx] = ByteBuffer.allocateDirect(size);
            decompressor.decompress(compressed[idx], decompressed[idx]);
          }
        } catch (Throwable e) {
          e.printStackTrace();
          errors.incrementAndGet();
        } finally {
          done.countDown();
        }
      });
      workers[i].start();
    }

    try {
      done.await(60L, TimeUnit.SECONDS); // it will not take this long
    } catch (InterruptedException e) {
      throw new AssertionError("timed-out");
    }

    // there are no errors
    assertEquals(errors.get(), 0);

    // all decompressed buffers contain the original text
    for (int i = 0; i < workers.length; i++) {
      assertEquals(StandardCharsets.UTF_8.decode(decompressed[i]).toString(), expected);
      compressed[i].clear();
      decompressed[i].clear();
    }
  }

  @Test
  public void testGzipCompressedFileHasSize()
      throws Exception {

    // read file into fileContent
    ByteBuffer fileContent;
    int fileSize;
    URL url = getClass().getResource("/data/words.txt");
    try (RandomAccessFile raf = new RandomAccessFile(url.getFile(), "r"); FileChannel channel = raf.getChannel()) {
      fileSize = (int) raf.length();
      fileContent = ByteBuffer.allocateDirect(fileSize);
      Assert.assertEquals(fileSize, channel.read(fileContent));
      fileContent.flip(); // ready for consumption
    }

    // compress fileContent into compressed
    ByteBuffer compressed;
    int uncompressedSize; // will be retrieved from the compressed buffer, must match fileSize
    try (GzipCompressor gzip = new GzipCompressor()) {
      int requiredSize = gzip.maxCompressedSize(fileSize);
      compressed = ByteBuffer.allocateDirect(requiredSize);
      int compressedSize = gzip.compress(fileContent, compressed);
      Assert.assertTrue(compressedSize <= requiredSize);
      Assert.assertTrue(compressedSize <= fileSize);
      uncompressedSize = compressed.getInt(compressedSize - Integer.BYTES);
      Assert.assertEquals(fileSize, uncompressedSize);
    }

    // decompress compressed into decompressed, buffer content should be the same as orig file
    ByteBuffer decompressed;
    int decompressedSize;
    try (GzipDecompressor gzip = new GzipDecompressor()) {
      int requiredSize = gzip.decompressedLength(compressed);
      Assert.assertEquals(fileSize, requiredSize);
      decompressed = ByteBuffer.allocateDirect(requiredSize);
      decompressedSize = gzip.decompress(compressed, decompressed);
      Assert.assertEquals(fileSize, decompressedSize);
      decompressed.flip();
      Assert.assertEquals(UTF_8.decode(fileContent).toString(), UTF_8.decode(decompressed).toString());
    }
  }

  private static void roundtrip(ChunkCompressor compressor, ByteBuffer rawInput)
      throws IOException {
    ByteBuffer compressedOutput = ByteBuffer.allocateDirect(compressor.maxCompressedSize(rawInput.limit()));
    compressor.compress(rawInput.slice(), compressedOutput);
    try (ChunkDecompressor decompressor = ChunkCompressorFactory.getDecompressor(compressor.compressionType())) {
      int decompressedLength = decompressor.decompressedLength(compressedOutput);
      boolean isLz4 = compressor.compressionType() == ChunkCompressionType.LZ4;
      assertTrue(isLz4 || decompressedLength > 0);
      ByteBuffer decompressedOutput = ByteBuffer.allocateDirect(isLz4 ? rawInput.limit() : decompressedLength);
      decompressor.decompress(compressedOutput, decompressedOutput);
      byte[] expected = new byte[rawInput.limit()];
      rawInput.get(expected);
      byte[] actual = new byte[decompressedOutput.limit()];
      decompressedOutput.get(actual);
      assertEquals(actual, expected, "content differs after compression roundt rip");
    }
  }
}
