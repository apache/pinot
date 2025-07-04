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

import java.util.Random;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class ZstdCompressorTest {
  private ZstdCompressor _compressor;

  @BeforeMethod
  public void setUp() {
    _compressor = new ZstdCompressor();
  }

  @Test
  public void testRoundTripWithSampleData() {
    String inputStr = "Zstandard compression test 123";
    byte[] input = inputStr.getBytes();
    byte[] compressed = _compressor.compress(input);
    byte[] decompressed = _compressor.decompress(compressed);
    assertEquals(decompressed, input, "Decompressed data should match original");
  }

  @Test
  public void testEmptyInput() {
    byte[] input = new byte[0];
    byte[] compressed = _compressor.compress(input);
    byte[] decompressed = _compressor.decompress(compressed);
    assertEquals(decompressed, input, "Empty input should remain empty after round trip");
  }

  @Test
  public void testLargeRandomData() {
    byte[] input = new byte[10_000_000]; // 10MB
    new Random().nextBytes(input);
    byte[] compressed = _compressor.compress(input);
    byte[] decompressed = _compressor.decompress(compressed);
    assertEquals(decompressed, input, "Large random input should match after decompression");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testInvalidCompressedData() {
    byte[] invalidData = new byte[]{0x01, 0x02, 0x03};
    _compressor.decompress(invalidData);
  }

  @Test
  public void testBinaryDataRoundTrip() {
    byte[] input = new byte[65_535]; // Zstd's special case size
    new Random().nextBytes(input);
    byte[] compressed = _compressor.compress(input);
    byte[] decompressed = _compressor.decompress(compressed);
    assertEquals(decompressed, input, "Binary data should survive round trip");
  }

  @Test
  public void testCompressionRatioForRedundantData() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 5000; i++) {
      sb.append("Repeating pattern ZSTD-COMPRESSION-TEST-123 ");
    }
    byte[] input = sb.toString().getBytes();
    byte[] compressed = _compressor.compress(input);
    assertTrue(compressed.length < input.length * 0.2,
        "Compressed size should be <20% of original for redundant data");
  }

  @Test
  public void testCorruptedDataThrowsException() {
    byte[] original = "Important data for corruption test".getBytes();
    byte[] compressed = _compressor.compress(original);

    // Corrupt the middle byte
    compressed[compressed.length / 2] ^= (byte) 0xFF;

    assertThrows(RuntimeException.class, () -> _compressor.decompress(compressed));
  }

  @Test
  public void testDecompressWithTruncatedData() {
    byte[] original = "Data for truncation test".getBytes();
    byte[] compressed = _compressor.compress(original);

    // Create truncated array
    byte[] truncated = new byte[compressed.length - 5];
    System.arraycopy(compressed, 0, truncated, 0, truncated.length);

    assertThrows(RuntimeException.class, () -> {
      _compressor.decompress(truncated);
    });
  }
}
