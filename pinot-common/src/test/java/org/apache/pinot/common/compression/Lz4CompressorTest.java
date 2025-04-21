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
package org.apache.pinot.common.compression;

import java.nio.charset.StandardCharsets;
import net.jpountz.lz4.LZ4Exception;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class Lz4CompressorTest {
  private Lz4Compressor _fastCompressor;
  private Lz4Compressor _highCompressor;

  @BeforeClass
  public void setUp() {
    _fastCompressor = Lz4Compressor.FAST_INSTANCE;
    _highCompressor = Lz4Compressor.HIGH_INSTANCE;
  }

  @Test
  public void testFastCompressionAndDecompression() {
    testCompressionAndDecompression(_fastCompressor);
  }

  @Test
  public void testHighCompressionAndDecompression() {
    testCompressionAndDecompression(_highCompressor);
  }

  private void testCompressionAndDecompression(Lz4Compressor compressor) {
    String originalString = "This is a performance-optimized LZ4 compression test.";
    byte[] originalBytes = originalString.getBytes(StandardCharsets.UTF_8);

    byte[] compressedBytes = compressor.compress(originalBytes);
    Assert.assertNotNull(compressedBytes, "Compressed data should not be null");
    Assert.assertTrue(compressedBytes.length > 4, "Compressed data should not be empty");

    byte[] decompressedBytes = compressor.decompress(compressedBytes);
    Assert.assertNotNull(decompressedBytes, "Decompressed data should not be null");
    Assert.assertEquals(decompressedBytes.length, originalBytes.length,
        "Decompressed data length should match original");
    Assert.assertEquals(new String(decompressedBytes, StandardCharsets.UTF_8), originalString,
        "Decompressed content should match original");
  }

  @Test
  public void testEmptyCompression() {
    byte[] emptyBytes = new byte[0];
    byte[] compressedBytes = _fastCompressor.compress(emptyBytes);
    Assert.assertNotNull(compressedBytes, "Compressed empty data should not be null");
    Assert.assertTrue(compressedBytes.length > 4, "Compressed data should contain length metadata");

    byte[] decompressedBytes = _fastCompressor.decompress(compressedBytes);
    Assert.assertNotNull(decompressedBytes, "Decompressed empty data should not be null");
    Assert.assertEquals(decompressedBytes.length, 0, "Decompressed empty data should be empty");
  }

  @Test(expectedExceptions = LZ4Exception.class)
  public void testInvalidDecompression() {
    byte[] invalidData = new byte[]{0, 0, 0, 10, 1, 2, 3, 4, 5}; // Invalid compressed data with fake length
    _fastCompressor.decompress(invalidData);
  }

  @Test
  public void testCompressionRatioForCompressibleData() {
    byte[] input = "REPEATING_PATTERN_LZ4_TEST_1234567890 ".repeat(1638).getBytes(StandardCharsets.UTF_8);
    byte[] compressedFast = _fastCompressor.compress(input);
    byte[] compressedHigh = _highCompressor.compress(input);

    Assert.assertTrue(compressedFast.length < input.length, "Fast compressed data should be smaller than original");
    Assert.assertTrue(compressedHigh.length <= compressedFast.length, "High compression should produce smaller output");
  }
}
