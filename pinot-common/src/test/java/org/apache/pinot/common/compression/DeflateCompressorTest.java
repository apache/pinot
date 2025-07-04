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


public class DeflateCompressorTest {
  private DeflateCompressor _compressor;

  @BeforeMethod
  public void setUp() {
    _compressor = new DeflateCompressor();
  }

  @Test
  public void testRoundTripWithSampleData()
      throws Exception {
    String inputStr = "Hello, World!";
    byte[] input = inputStr.getBytes();
    byte[] compressed = _compressor.compress(input);
    byte[] decompressed = _compressor.decompress(compressed);
    assertEquals(decompressed, input, "Decompressed data should match original input");
  }

  @Test
  public void testEmptyInput()
      throws Exception {
    byte[] input = new byte[0];
    byte[] compressed = _compressor.compress(input);
    byte[] decompressed = _compressor.decompress(compressed);
    assertEquals(decompressed, input, "Decompressed empty data should be empty");
  }

  @Test
  public void testLargeInput()
      throws Exception {
    byte[] input = new byte[10000];
    new Random().nextBytes(input);
    byte[] compressed = _compressor.compress(input);
    byte[] decompressed = _compressor.decompress(compressed);
    assertEquals(decompressed, input, "Decompressed large data should match original");
  }

  @Test(expectedExceptions = Exception.class)
  public void testDecompressInvalidDataThrowsException()
      throws Exception {
    byte[] invalidData = new byte[]{0x01, 0x02, 0x03};
    _compressor.decompress(invalidData);
  }

  @Test
  public void testBinaryData()
      throws Exception {
    byte[] input = new byte[500];
    new Random().nextBytes(input);
    byte[] compressed = _compressor.compress(input);
    byte[] decompressed = _compressor.decompress(compressed);
    assertEquals(decompressed, input, "Decompressed binary data should match original");
  }

  @Test
  public void testInputSizeLargerThanBuffer()
      throws Exception {
    byte[] input = new byte[1500];
    new Random().nextBytes(input);
    byte[] compressed = _compressor.compress(input);
    byte[] decompressed = _compressor.decompress(compressed);
    assertEquals(decompressed, input, "Data larger than buffer size should decompress correctly");
  }

  @Test
  public void testCompressedDataIsSmallerForCompressibleData()
      throws Exception {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      sb.append("repeated string ");
    }
    byte[] input = sb.toString().getBytes();
    byte[] compressed = _compressor.compress(input);
    assertTrue(compressed.length < input.length, "Compressed data should be smaller for compressible input");
  }
}
