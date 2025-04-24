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


public class SnappyCompressorTest {
  private SnappyCompressor _compressor;

  @BeforeMethod
  public void setUp() {
    _compressor = new SnappyCompressor();
  }

  @Test
  public void testRoundTripWithSampleData()
      throws Exception {
    String inputStr = "Snappy compression test 123";
    byte[] input = inputStr.getBytes();
    byte[] compressed = _compressor.compress(input);
    byte[] decompressed = _compressor.decompress(compressed);
    assertEquals(decompressed, input, "Round-trip should return original data");
  }

  @Test
  public void testEmptyInput()
      throws Exception {
    byte[] input = new byte[0];
    byte[] compressed = _compressor.compress(input);
    byte[] decompressed = _compressor.decompress(compressed);
    assertEquals(decompressed, input, "Empty input should remain empty");
  }

  @Test
  public void testLargeRandomData()
      throws Exception {
    byte[] input = new byte[100_000]; // 100KB
    new Random().nextBytes(input);
    byte[] compressed = _compressor.compress(input);
    byte[] decompressed = _compressor.decompress(compressed);
    assertEquals(decompressed, input, "Large random input should match after decompression");
  }

  @Test(expectedExceptions = Exception.class)
  public void testInvalidCompressedData()
      throws Exception {
    byte[] invalidData = new byte[]{0x01, 0x02, 0x03}; // Random invalid data
    _compressor.decompress(invalidData);
  }

  @Test
  public void testBinaryDataRoundTrip()
      throws Exception {
    byte[] input = new byte[8192]; // 8KB
    new Random().nextBytes(input);
    byte[] compressed = _compressor.compress(input);
    byte[] decompressed = _compressor.decompress(compressed);
    assertEquals(decompressed, input, "Binary data should survive round-trip");
  }

  @Test
  public void testCompressionRatioForRepeatedData()
      throws Exception {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 5000; i++) {
      sb.append("Repeating pattern ABCDEF123456 "); // Highly redundant
    }
    byte[] input = sb.toString().getBytes();
    byte[] compressed = _compressor.compress(input);

    assertTrue(compressed.length < input.length * 0.75,
        "Compressed size should be significantly smaller for redundant data");
  }
}
