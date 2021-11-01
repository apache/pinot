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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;
import org.apache.pinot.segment.spi.compression.ChunkDecompressor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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
        {ChunkCompressionType.PASS_THROUGH, buffer.slice()},
        {ChunkCompressionType.SNAPPY, buffer.slice()},
        {ChunkCompressionType.LZ4, buffer.slice()},
        {ChunkCompressionType.LZ4_LENGTH_PREFIXED, buffer.slice()},
        {ChunkCompressionType.ZSTANDARD, buffer.slice()}
    };
  }

  @Test(dataProvider = "formats")
  public void testRoundtrip(ChunkCompressionType type, ByteBuffer rawInput)
      throws IOException {
    ChunkCompressor compressor = ChunkCompressorFactory.getCompressor(type);
    assertEquals(compressor.compressionType(), type, "upgrade is opt in");
    roundtrip(compressor, rawInput);
  }

  @Test(dataProvider = "formats")
  public void testRoundtripWithUpgrade(ChunkCompressionType type, ByteBuffer rawInput)
      throws IOException {
    ChunkCompressor compressor = ChunkCompressorFactory.getCompressor(type, true);
    assertNotEquals(compressor.compressionType(), ChunkCompressionType.LZ4,
        "LZ4 compression type does not support length prefix");
    roundtrip(compressor, rawInput);
  }

  private void roundtrip(ChunkCompressor compressor, ByteBuffer rawInput)
      throws IOException {
    ByteBuffer compressedOutput = ByteBuffer.allocateDirect(compressor.maxCompressedSize(rawInput.limit()));
    compressor.compress(rawInput.slice(), compressedOutput);
    ChunkDecompressor decompressor = ChunkCompressorFactory.getDecompressor(compressor.compressionType());
    int decompressedLength = decompressor.decompressedLength(compressedOutput);
    assertTrue(compressor.compressionType() == ChunkCompressionType.LZ4 || decompressedLength > 0);
    ByteBuffer decompressedOutput = ByteBuffer.allocateDirect(
        compressor.compressionType() == ChunkCompressionType.LZ4 ? rawInput.limit() : decompressedLength);
    decompressor.decompress(compressedOutput, decompressedOutput);
    byte[] expected = new byte[rawInput.limit()];
    rawInput.get(expected);
    byte[] actual = new byte[decompressedOutput.limit()];
    decompressedOutput.get(actual);
    assertEquals(actual, expected, "content differs after compression roundt rip");
  }
}
