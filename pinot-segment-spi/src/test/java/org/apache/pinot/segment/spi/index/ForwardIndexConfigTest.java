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
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.DictIdCompressionType;
import org.apache.pinot.spi.config.table.CompressionCodecSpec;
import org.apache.pinot.spi.config.table.FieldConfig;
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

  @Test
  public void withParameterizedCompressionCodec()
      throws JsonProcessingException {
    String confStr = "{\"compressionCodec\": \"zstd(3)\"}";
    ForwardIndexConfig config = JsonUtils.stringToObject(confStr, ForwardIndexConfig.class);

    assertEquals(config.getCompressionCodec(), FieldConfig.CompressionCodec.ZSTANDARD);
    assertEquals(config.getCompressionCodecSpec(), CompressionCodecSpec.of(FieldConfig.CompressionCodec.ZSTANDARD, 3));
    assertEquals(config.getChunkCompressionType(), ChunkCompressionType.ZSTANDARD);
    assertEquals(config.toJsonNode().get("compressionCodec").asText(), "ZSTANDARD(3)");
  }

  @Test
  public void withLegacyGzipChunkCompressionType()
      throws JsonProcessingException {
    String confStr = "{\"chunkCompressionType\": \"GZIP\"}";
    ForwardIndexConfig config = JsonUtils.stringToObject(confStr, ForwardIndexConfig.class);

    assertEquals(config.getCompressionCodec(), FieldConfig.CompressionCodec.GZIP);
    assertEquals(config.getChunkCompressionType(), ChunkCompressionType.GZIP);
  }

  @Test
  public void withLegacyDictIdCompressionType()
      throws JsonProcessingException {
    String confStr = "{\"dictIdCompressionType\": \"MV_ENTRY_DICT\"}";
    ForwardIndexConfig config = JsonUtils.stringToObject(confStr, ForwardIndexConfig.class);

    assertEquals(config.getCompressionCodec(), FieldConfig.CompressionCodec.MV_ENTRY_DICT);
    assertEquals(config.getCompressionCodecSpec(), CompressionCodecSpec.fromString("MV_ENTRY_DICT"));
    assertEquals(config.getDictIdCompressionType(), DictIdCompressionType.MV_ENTRY_DICT);
    assertNull(config.getChunkCompressionType());
  }

  @Test
  public void testDisabledAndLegacyCompatibilityHelpers() {
    ForwardIndexConfig disabled = ForwardIndexConfig.getDisabled();
    assertTrue(disabled.isDisabled());
    assertNull(disabled.getCompressionCodec());
    assertNull(disabled.getCompressionCodecSpec());

    assertEquals(ForwardIndexConfig.getActualCompressionCodecSpec(null, ChunkCompressionType.GZIP, null),
        CompressionCodecSpec.fromString("GZIP"));
    IllegalArgumentException mixedCompressionFields = expectThrows(IllegalArgumentException.class,
        () -> ForwardIndexConfig.getActualCompressionCodecSpec(CompressionCodecSpec.fromString("ZSTD(3)"),
            ChunkCompressionType.SNAPPY, null));
    assertTrue(mixedCompressionFields.getMessage().contains("must not be used together"));

    IllegalArgumentException invalidCombination = expectThrows(IllegalArgumentException.class,
        () -> ForwardIndexConfig.getActualCompressionCodec(null, ChunkCompressionType.SNAPPY,
            DictIdCompressionType.MV_ENTRY_DICT));
    assertTrue(invalidCombination.getMessage().contains("should not be used together"));
  }

  @Test
  public void withMixedCompressionCodecFields()
      throws JsonProcessingException {
    String confStr = "{\"compressionCodec\": \"zstd(3)\", \"chunkCompressionType\": \"SNAPPY\"}";

    JsonProcessingException mixedCompressionFields = expectThrows(JsonProcessingException.class,
        () -> JsonUtils.stringToObject(confStr, ForwardIndexConfig.class));
    assertTrue(mixedCompressionFields.getMessage().contains("must not be used together"));
  }

  @Test
  public void testBuilderPreservesParameterizedSpecAcrossLegacyUpdates() {
    ForwardIndexConfig preserved = new ForwardIndexConfig.Builder()
        .withCompressionCodecSpec(CompressionCodecSpec.fromString("LZ4(12)"))
        .withCompressionType(ChunkCompressionType.LZ4_LENGTH_PREFIXED)
        .build();
    assertEquals(preserved.getCompressionCodec(), FieldConfig.CompressionCodec.LZ4);
    assertEquals(preserved.getCompressionCodecSpec(), CompressionCodecSpec.fromString("LZ4(12)"));
    assertEquals(preserved.getChunkCompressionType(), ChunkCompressionType.LZ4);

    ForwardIndexConfig replaced = new ForwardIndexConfig.Builder()
        .withCompressionCodecSpec(CompressionCodecSpec.fromString("LZ4(12)"))
        .withCompressionType(ChunkCompressionType.SNAPPY)
        .build();
    assertEquals(replaced.getCompressionCodec(), FieldConfig.CompressionCodec.SNAPPY);
    assertEquals(replaced.getCompressionCodecSpec(), CompressionCodecSpec.fromString("SNAPPY"));

    ForwardIndexConfig dictConfig = new ForwardIndexConfig.Builder()
        .withDictIdCompressionType(DictIdCompressionType.MV_ENTRY_DICT)
        .build();
    assertEquals(dictConfig.getCompressionCodec(), FieldConfig.CompressionCodec.MV_ENTRY_DICT);
    assertEquals(dictConfig.getCompressionCodecSpec(), CompressionCodecSpec.fromString("MV_ENTRY_DICT"));
    assertEquals(dictConfig.getDictIdCompressionType(), DictIdCompressionType.MV_ENTRY_DICT);
  }
}
