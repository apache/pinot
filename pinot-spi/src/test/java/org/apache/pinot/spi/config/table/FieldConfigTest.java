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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Tests for [FieldConfig] serialization and deserialization.
///
/// Note: `codecSpec` is configured only via the `indexes.forward` block (deserialized into
/// `ForwardIndexConfig`), not as a top-level `FieldConfig` field — see `ForwardIndexConfigTest`
/// for codecSpec round-trip coverage.
public class FieldConfigTest {

  /// Verifies that the legacy top-level `compressionCodec` still deserializes correctly.
  @Test
  public void testCompressionCodecRoundTrip()
      throws JsonProcessingException {
    FieldConfig original = new FieldConfig.Builder("col")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.ZSTANDARD)
        .build();

    String json = JsonUtils.objectToString(original);
    FieldConfig deserialized = JsonUtils.stringToObject(json, FieldConfig.class);

    assertEquals(deserialized.getCompressionCodec(), FieldConfig.CompressionCodec.ZSTANDARD,
        "compressionCodec must survive JSON round-trip");
    assertEquals(deserialized.getName(), "col");
    assertEquals(deserialized.getEncodingType(), FieldConfig.EncodingType.RAW);
  }
}
