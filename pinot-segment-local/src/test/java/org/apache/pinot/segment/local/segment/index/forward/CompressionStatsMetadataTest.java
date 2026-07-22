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
package org.apache.pinot.segment.local.segment.index.forward;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.FORWARD_INDEX_DICTIONARY_ENCODED_UNCOMPRESSED_VALUE_SIZE_IN_BYTES;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.FORWARD_INDEX_RAW_CHUNK_COMPRESSION_TYPE;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.FORWARD_INDEX_RAW_UNCOMPRESSED_VALUE_SIZE_IN_BYTES;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.getKeyFor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Verifies compression-statistics metadata parsing and persistence.
public class CompressionStatsMetadataTest {
  private static final String COLUMN = "testColumn";

  @Test
  public void testEveryStateUpdatesAllKeys() {
    Map<String, String> properties = new HashMap<>();
    CompressionStatsMetadata.forDictionary(100).applyTo(properties, COLUMN);
    assertNull(properties.get(getKeyFor(COLUMN, FORWARD_INDEX_RAW_UNCOMPRESSED_VALUE_SIZE_IN_BYTES)));
    assertNull(properties.get(getKeyFor(COLUMN, FORWARD_INDEX_RAW_CHUNK_COMPRESSION_TYPE)));
    assertEquals(
        properties.get(getKeyFor(COLUMN, FORWARD_INDEX_DICTIONARY_ENCODED_UNCOMPRESSED_VALUE_SIZE_IN_BYTES)),
        "100");

    CompressionStatsMetadata.forRawForwardIndex(80, ChunkCompressionType.LZ4).applyTo(properties, COLUMN);
    assertEquals(properties.get(getKeyFor(COLUMN, FORWARD_INDEX_RAW_UNCOMPRESSED_VALUE_SIZE_IN_BYTES)), "80");
    assertEquals(properties.get(getKeyFor(COLUMN, FORWARD_INDEX_RAW_CHUNK_COMPRESSION_TYPE)), "LZ4");
    assertNull(
        properties.get(getKeyFor(COLUMN, FORWARD_INDEX_DICTIONARY_ENCODED_UNCOMPRESSED_VALUE_SIZE_IN_BYTES)));

    CompressionStatsMetadata.unavailable().applyTo(properties, COLUMN);
    assertNull(properties.get(getKeyFor(COLUMN, FORWARD_INDEX_RAW_UNCOMPRESSED_VALUE_SIZE_IN_BYTES)));
    assertNull(properties.get(getKeyFor(COLUMN, FORWARD_INDEX_RAW_CHUNK_COMPRESSION_TYPE)));
    assertNull(properties.get(getKeyFor(COLUMN, FORWARD_INDEX_DICTIONARY_ENCODED_UNCOMPRESSED_VALUE_SIZE_IN_BYTES)));
  }
}
