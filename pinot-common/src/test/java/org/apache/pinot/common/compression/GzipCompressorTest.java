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
import static org.testng.Assert.assertTrue;


public class GzipCompressorTest {
  private GzipCompressor _compressor;

  @BeforeMethod
  public void setUp() {
    _compressor = new GzipCompressor();
  }

  @Test
  public void testRoundTripWithSampleData()
      throws Exception {
    String inputStr = "Test GZIP compression";
    byte[] input = inputStr.getBytes();
    byte[] compressed = _compressor.compress(input);
    byte[] decompressed = _compressor.decompress(compressed);
    assertEquals(decompressed, input, "Decompressed data should match original");
  }

  @Test
  public void testEmptyInput()
      throws Exception {
    byte[] input = new byte[0];
    byte[] compressed = _compressor.compress(input);
    byte[] decompressed = _compressor.decompress(compressed);
    assertEquals(decompressed, input, "Empty input should remain empty after round trip");
  }

  @Test
  public void testLargeInput()
      throws Exception {
    byte[] input = new byte[100_000];  // 100KB
    new Random().nextBytes(input);
    byte[] compressed = _compressor.compress(input);
    byte[] decompressed = _compressor.decompress(compressed);
    assertEquals(decompressed, input, "Large input should survive round trip");
  }

  @Test(expectedExceptions = Exception.class)
  public void testDecompressInvalidData()
      throws Exception {
    byte[] invalidPayload = {0x01, 0x02, 0x03};  // Not valid GZIP
    _compressor.decompress(invalidPayload);
  }

  @Test
  public void testBinaryData()
      throws Exception {
    byte[] input = new byte[2048];
    new Random().nextBytes(input);
    byte[] compressed = _compressor.compress(input);
    byte[] decompressed = _compressor.decompress(compressed);
    assertEquals(decompressed, input, "Random binary data should match after round trip");
  }

  @Test
  public void testCompressionReducesSizeForRepetitiveData()
      throws Exception {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      sb.append("Repeating pattern ABC123 ");
    }
    byte[] input = sb.toString().getBytes();
    byte[] compressed = _compressor.compress(input);
    assertTrue(compressed.length < input.length,
        "Compressed size should be smaller for repetitive content");
  }

  @Test
  public void testPartialReadHandling()
      throws Exception {
    // Create input that will produce multi-chunk output
    byte[] input = new byte[2048];
    new Random().nextBytes(input);

    byte[] compressed = _compressor.compress(input);
    byte[] decompressed = _compressor.decompress(compressed);
    assertEquals(decompressed, input, "Should handle multi-chunk reads properly");
  }
}
