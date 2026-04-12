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
package org.apache.pinot.segment.spi.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Collections;
import org.apache.pinot.segment.spi.codec.ChunkCodec;
import org.apache.pinot.segment.spi.codec.ChunkCodecPipeline;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ForwardIndexConfigTest {

  @Test
  public void withEmptyConf()
      throws JsonProcessingException {
    String confStr = "{}";
    ForwardIndexConfig config = JsonUtils.stringToObject(confStr, ForwardIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getChunkCompressionType(), "Unexpected chunkCompressionType");
    assertFalse(config.isDeriveNumDocsPerChunk(), "Unexpected deriveNumDocsPerChunk");
    assertEquals(config.getRawIndexWriterVersion(), ForwardIndexConfig.getDefaultRawWriterVersion(),
        "Unexpected rawIndexWriterVersion");
  }

  @Test
  public void withDisabledNull()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": null}";
    ForwardIndexConfig config = JsonUtils.stringToObject(confStr, ForwardIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getChunkCompressionType(), "Unexpected chunkCompressionType");
    assertFalse(config.isDeriveNumDocsPerChunk(), "Unexpected deriveNumDocsPerChunk");
    assertEquals(config.getRawIndexWriterVersion(), ForwardIndexConfig.getDefaultRawWriterVersion(),
        "Unexpected rawIndexWriterVersion");
  }

  @Test
  public void withDisabledFalse()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": false}";
    ForwardIndexConfig config = JsonUtils.stringToObject(confStr, ForwardIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getChunkCompressionType(), "Unexpected chunkCompressionType");
    assertFalse(config.isDeriveNumDocsPerChunk(), "Unexpected deriveNumDocsPerChunk");
    assertEquals(config.getRawIndexWriterVersion(), ForwardIndexConfig.getDefaultRawWriterVersion(),
        "Unexpected rawIndexWriterVersion");
  }

  @Test
  public void withDisabledTrue()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": true}";
    ForwardIndexConfig config = JsonUtils.stringToObject(confStr, ForwardIndexConfig.class);

    assertTrue(config.isDisabled(), "Unexpected disabled");
    assertNull(config.getChunkCompressionType(), "Unexpected chunkCompressionType");
    assertFalse(config.isDeriveNumDocsPerChunk(), "Unexpected deriveNumDocsPerChunk");
    assertEquals(config.getRawIndexWriterVersion(), ForwardIndexConfig.getDefaultRawWriterVersion(),
        "Unexpected rawIndexWriterVersion");
  }

  @Test
  public void withNegativeTargetDocsPerChunk()
      throws JsonProcessingException {
    String confStr = "{\"targetDocsPerChunk\": \"-1\"}";
    ForwardIndexConfig config = JsonUtils.stringToObject(confStr, ForwardIndexConfig.class);

    assertEquals(config.getTargetDocsPerChunk(), -1, "Unexpected defaultTargetDocsPerChunk");
  }

  @Test
  public void withSomeData()
      throws JsonProcessingException {
    String confStr = "{\n"
        + "        \"chunkCompressionType\": \"SNAPPY\",\n"
        + "        \"deriveNumDocsPerChunk\": true,\n"
        + "        \"rawIndexWriterVersion\": 10,\n"
        + "        \"targetMaxChunkSize\": \"512K\",\n"
        + "        \"targetDocsPerChunk\": \"2000\"\n"
        + "}";
    ForwardIndexConfig config = JsonUtils.stringToObject(confStr, ForwardIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertEquals(config.getChunkCompressionType(), ChunkCompressionType.SNAPPY, "Unexpected chunkCompressionType");
    assertTrue(config.isDeriveNumDocsPerChunk(), "Unexpected deriveNumDocsPerChunk");
    assertEquals(config.getRawIndexWriterVersion(), 10, "Unexpected rawIndexWriterVersion");
    assertEquals(config.getTargetMaxChunkSizeBytes(), 512 * 1024, "Unexpected targetMaxChunkSizeBytes");
    assertEquals(config.getTargetDocsPerChunk(), 2000, "Unexpected defaultTargetDocsPerChunk");
  }

  // ===== Auto-derivation of codecPipeline from compressionCodec =====

  @Test
  public void testAutoDerivePipelineFromLz4() {
    ForwardIndexConfig config = new ForwardIndexConfig(false, CompressionCodec.LZ4,
        null, null, null, null, null, null);
    assertNotNull(config.getCodecPipeline());
    assertEquals(config.getCodecPipeline().size(), 1);
    assertEquals(config.getCodecPipeline().get(0), ChunkCodec.LZ4);
    assertEquals(config.getChunkCompressionType(), ChunkCompressionType.LZ4);
  }

  @Test
  public void testAutoDerivePipelineFromDelta() {
    // Legacy DELTA → pipeline [DELTA_LZ4] (single compound compressor)
    ForwardIndexConfig config = new ForwardIndexConfig(false, CompressionCodec.DELTA,
        null, null, null, null, null, null);
    assertNotNull(config.getCodecPipeline());
    assertEquals(config.getCodecPipeline().size(), 1);
    assertEquals(config.getCodecPipeline().get(0), ChunkCodec.DELTA_LZ4);
    assertFalse(config.getCodecPipeline().hasTransforms());
    assertEquals(config.getChunkCompressionType(), ChunkCompressionType.DELTA);
  }

  @Test
  public void testAutoDerivePipelineFromDeltaDelta() {
    // Legacy DELTADELTA → pipeline [DOUBLE_DELTA_LZ4] (single compound compressor)
    ForwardIndexConfig config = new ForwardIndexConfig(false, CompressionCodec.DELTADELTA,
        null, null, null, null, null, null);
    assertNotNull(config.getCodecPipeline());
    assertEquals(config.getCodecPipeline().size(), 1);
    assertEquals(config.getCodecPipeline().get(0), ChunkCodec.DOUBLE_DELTA_LZ4);
    assertFalse(config.getCodecPipeline().hasTransforms());
    assertEquals(config.getChunkCompressionType(), ChunkCompressionType.DELTADELTA);
  }

  @Test
  public void testNoPipelineForCLP() {
    // CLP codecs should NOT auto-derive a pipeline
    ForwardIndexConfig config = new ForwardIndexConfig(false, CompressionCodec.CLP,
        null, null, null, null, null, null);
    assertNull(config.getCodecPipeline());
  }

  @Test
  public void testNoPipelineForMvEntryDict() {
    // MV_ENTRY_DICT should NOT auto-derive a pipeline (chunkCompressionType is null)
    ForwardIndexConfig config = new ForwardIndexConfig(false, CompressionCodec.MV_ENTRY_DICT,
        null, null, null, null, null, null);
    assertNull(config.getCodecPipeline());
  }

  @Test
  public void testExplicitPipelineOverridesCompression() {
    // Explicit pipeline — compressionCodec must be null per mutual exclusivity
    ChunkCodecPipeline pipeline = new ChunkCodecPipeline(
        Collections.singletonList(ChunkCodec.ZSTANDARD));
    ForwardIndexConfig config = new ForwardIndexConfig(false, null, pipeline,
        null, null, null, null, null);
    assertNotNull(config.getCodecPipeline());
    assertEquals(config.getCodecPipeline(), pipeline);
    assertNull(config.getCompressionCodec());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testConflictCompressionCodecAndPipeline() {
    // Both compressionCodec and codecPipeline set — should throw
    ChunkCodecPipeline pipeline = new ChunkCodecPipeline(
        Collections.singletonList(ChunkCodec.ZSTANDARD));
    new ForwardIndexConfig(false, CompressionCodec.LZ4, pipeline,
        null, null, null, null, null);
  }

  @Test
  public void testJsonConflictCompressionCodecAndPipeline()
      throws JsonProcessingException {
    // JSON with both compressionCodec and codecPipeline should throw
    String confStr = "{\n"
        + "  \"compressionCodec\": \"LZ4\",\n"
        + "  \"codecPipeline\": [\"ZSTANDARD\"]\n"
        + "}";
    try {
      JsonUtils.stringToObject(confStr, ForwardIndexConfig.class);
      fail("Expected exception for conflicting compressionCodec and codecPipeline");
    } catch (Exception e) {
      // Expected
    }
  }
}
