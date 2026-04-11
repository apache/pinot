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

import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Tests for the {@code compressionStatsEnabled} field on {@link IndexingConfig}.
 * Covers T002: serialization round-trip, default value, and backward-compatible deserialization.
 */
public class IndexingConfigCompressionFlagTest {

  @Test
  public void testDefaultValueIsFalse() {
    IndexingConfig config = new IndexingConfig();
    assertFalse(config.isCompressionStatsEnabled(),
        "Default value of compressionStatsEnabled should be false");
  }

  @Test
  public void testSetAndGet() {
    IndexingConfig config = new IndexingConfig();
    config.setCompressionStatsEnabled(true);
    assertTrue(config.isCompressionStatsEnabled(),
        "compressionStatsEnabled should be true after setting it to true");
  }

  @Test
  public void testJsonSerializationRoundTrip()
      throws Exception {
    IndexingConfig original = new IndexingConfig();
    original.setCompressionStatsEnabled(true);

    String json = JsonUtils.objectToString(original);
    IndexingConfig deserialized = JsonUtils.stringToObject(json, IndexingConfig.class);

    assertTrue(deserialized.isCompressionStatsEnabled(),
        "compressionStatsEnabled should remain true after JSON round-trip serialization");
  }

  @Test
  public void testBackwardCompatDeserialization()
      throws Exception {
    // JSON that does not contain the compressionStatsEnabled field, simulating an old config
    String oldConfigJson = "{\"loadMode\":\"MMAP\"}";

    IndexingConfig config = JsonUtils.stringToObject(oldConfigJson, IndexingConfig.class);

    assertFalse(config.isCompressionStatsEnabled(),
        "compressionStatsEnabled should default to false when missing from JSON (backward compatibility)");
  }
}
