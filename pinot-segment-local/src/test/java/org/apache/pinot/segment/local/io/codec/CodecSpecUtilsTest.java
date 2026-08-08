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
package org.apache.pinot.segment.local.io.codec;

import org.apache.pinot.segment.spi.codec.CodecSpecParser;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Tests for [CodecSpecUtils].
public class CodecSpecUtilsTest {

  @Test
  public void testCompressionOnlySpecsMapToLegacyChunkCompressionType() {
    assertEquals(CodecSpecUtils.toLegacyChunkCompressionType("LZ4"), ChunkCompressionType.LZ4);
    assertEquals(CodecSpecUtils.toLegacyChunkCompressionType("SNAPPY"), ChunkCompressionType.SNAPPY);
    assertEquals(CodecSpecUtils.toLegacyChunkCompressionType("GZIP"), ChunkCompressionType.GZIP);
    assertEquals(CodecSpecUtils.toLegacyChunkCompressionType("ZSTD"), ChunkCompressionType.ZSTANDARD);
    assertEquals(CodecSpecUtils.toLegacyChunkCompressionType("ZSTD(3)"), ChunkCompressionType.ZSTANDARD);
  }

  @Test
  public void testNonLegacyCompatibleSpecsReturnNull() {
    assertNull(CodecSpecUtils.toLegacyChunkCompressionType("ZSTD(8)"));
    assertNull(CodecSpecUtils.toLegacyChunkCompressionType("CODEC(DELTA,LZ4)"));
    assertNull(CodecSpecUtils.toLegacyChunkCompressionType("CODEC(LZ4,SNAPPY)"));
  }

  @Test
  public void testHasTransform() {
    assertFalse(CodecSpecUtils.hasTransform(CodecSpecParser.parse("LZ4"), CodecRegistry.DEFAULT));
    assertTrue(CodecSpecUtils.hasTransform(CodecSpecParser.parse("CODEC(DELTA,LZ4)"), CodecRegistry.DEFAULT));
  }
}
