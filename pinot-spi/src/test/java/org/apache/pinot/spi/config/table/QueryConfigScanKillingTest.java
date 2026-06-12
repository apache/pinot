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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class QueryConfigScanKillingTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testScanFieldsDefaultToNull() {
    QueryConfig config = new QueryConfig(null, null, null, null, null, null, null, null, null);
    assertNull(config.getMaxEntriesScannedInFilter());
    assertNull(config.getMaxDocsScanned());
    assertNull(config.getMaxEntriesScannedPostFilter());
  }

  @Test
  public void testScanFieldsSetExplicitly() {
    QueryConfig config = new QueryConfig(null, null, null, null, null, null,
        500_000_000L, 50_000_000L, 100_000_000L);
    assertEquals(config.getMaxEntriesScannedInFilter(), Long.valueOf(500_000_000L));
    assertEquals(config.getMaxDocsScanned(), Long.valueOf(50_000_000L));
    assertEquals(config.getMaxEntriesScannedPostFilter(), Long.valueOf(100_000_000L));
  }

  @Test
  public void testJsonSerializationWithScanFields()
      throws Exception {
    QueryConfig config = new QueryConfig(30000L, null, null, null, null, null,
        500_000_000L, 50_000_000L, null);

    String json = OBJECT_MAPPER.writeValueAsString(config);
    QueryConfig deserialized = OBJECT_MAPPER.readValue(json, QueryConfig.class);

    assertEquals(deserialized.getTimeoutMs(), Long.valueOf(30000L));
    assertEquals(deserialized.getMaxEntriesScannedInFilter(), Long.valueOf(500_000_000L));
    assertEquals(deserialized.getMaxDocsScanned(), Long.valueOf(50_000_000L));
    assertNull(deserialized.getMaxEntriesScannedPostFilter());
  }

  @Test
  public void testJsonDeserializationWithoutScanFields()
      throws Exception {
    String json = "{\"timeoutMs\": 5000}";
    QueryConfig config = OBJECT_MAPPER.readValue(json, QueryConfig.class);
    assertEquals(config.getTimeoutMs(), Long.valueOf(5000L));
    assertNull(config.getMaxEntriesScannedInFilter());
    assertNull(config.getMaxDocsScanned());
    assertNull(config.getMaxEntriesScannedPostFilter());
  }

  @Test
  public void testJsonDeserializationWithOnlyScanFields()
      throws Exception {
    String json = "{\"maxEntriesScannedInFilter\": 1000000000, \"maxDocsScanned\": 100000000}";
    QueryConfig config = OBJECT_MAPPER.readValue(json, QueryConfig.class);
    assertNull(config.getTimeoutMs());
    assertEquals(config.getMaxEntriesScannedInFilter(), Long.valueOf(1_000_000_000L));
    assertEquals(config.getMaxDocsScanned(), Long.valueOf(100_000_000L));
    assertNull(config.getMaxEntriesScannedPostFilter());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNegativeMaxEntriesScannedInFilterThrows() {
    new QueryConfig(null, null, null, null, null, null, -1L, null, null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testZeroMaxDocsScannedThrows() {
    new QueryConfig(null, null, null, null, null, null, null, 0L, null);
  }
}
