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
    assertEquals(config.getRawIndexWriterVersion(), ForwardIndexConfig.DEFAULT_RAW_WRITER_VERSION,
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
    assertEquals(config.getRawIndexWriterVersion(), ForwardIndexConfig.DEFAULT_RAW_WRITER_VERSION,
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
    assertEquals(config.getRawIndexWriterVersion(), ForwardIndexConfig.DEFAULT_RAW_WRITER_VERSION,
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
    assertEquals(config.getRawIndexWriterVersion(), ForwardIndexConfig.DEFAULT_RAW_WRITER_VERSION,
        "Unexpected rawIndexWriterVersion");
  }

  @Test
  public void withSomeData()
      throws JsonProcessingException {
    String confStr = "{\n"
        + "        \"chunkCompressionType\": \"SNAPPY\",\n"
        + "        \"deriveNumDocsPerChunk\": true,\n"
        + "        \"rawIndexWriterVersion\": 10\n"
        + "}";
    ForwardIndexConfig config = JsonUtils.stringToObject(confStr, ForwardIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertEquals(config.getChunkCompressionType(), ChunkCompressionType.SNAPPY, "Unexpected chunkCompressionType");
    assertTrue(config.isDeriveNumDocsPerChunk(), "Unexpected deriveNumDocsPerChunk");
    assertEquals(config.getRawIndexWriterVersion(), 10, "Unexpected rawIndexWriterVersion");
  }
}
