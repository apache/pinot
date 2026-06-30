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
import org.apache.pinot.spi.utils.CommonConstants.Accounting.ScanKillingMode;
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

  @Test
  public void testScanKillingModeDefaultsToNull() {
    QueryConfig config = new QueryConfig(null, null, null, null, null, null, null, null, null);
    assertNull(config.getScanKillingMode());
  }

  @Test
  public void testScanKillingModeSetExplicitly() {
    QueryConfig config = new QueryConfig(null, null, null, null, null, null, null, null, null, "enforce");
    assertEquals(config.getScanKillingMode(), "enforce");
  }

  @Test
  public void testScanKillingModeJsonRoundTrip()
      throws Exception {
    QueryConfig config = new QueryConfig(null, null, null, null, null, null, 500_000L, null, null, "logOnly");

    String json = OBJECT_MAPPER.writeValueAsString(config);
    QueryConfig deserialized = OBJECT_MAPPER.readValue(json, QueryConfig.class);

    assertEquals(deserialized.getScanKillingMode(), "logOnly");
    assertEquals(deserialized.getMaxEntriesScannedInFilter(), Long.valueOf(500_000L));
  }

  @Test
  public void testScanKillingModeDeserializesFromJson()
      throws Exception {
    String json = "{\"maxDocsScanned\": 5000000, \"scanKillingMode\": \"disabled\"}";
    QueryConfig config = OBJECT_MAPPER.readValue(json, QueryConfig.class);
    assertEquals(config.getScanKillingMode(), "disabled");
    assertEquals(config.getMaxDocsScanned(), Long.valueOf(5_000_000L));
  }

  @Test
  public void testScanKillingModeAbsentInJsonDeserializesToNull()
      throws Exception {
    String json = "{\"maxDocsScanned\": 5000000}";
    QueryConfig config = OBJECT_MAPPER.readValue(json, QueryConfig.class);
    assertNull(config.getScanKillingMode());
  }

  @Test
  public void testScanKillingModeStringParsesToCorrectEnum() {
    // Verify the production parse path: getScanKillingMode() string → ScanKillingMode enum
    QueryConfig enforce = new QueryConfig(null, null, null, null, null, null, null, null, null, "enforce");
    assertEquals(ScanKillingMode.fromConfigValue(enforce.getScanKillingMode()), ScanKillingMode.ENFORCE);

    QueryConfig logOnly = new QueryConfig(null, null, null, null, null, null, null, null, null, "logOnly");
    assertEquals(ScanKillingMode.fromConfigValue(logOnly.getScanKillingMode()), ScanKillingMode.LOG_ONLY);

    QueryConfig disabled = new QueryConfig(null, null, null, null, null, null, null, null, null, "disabled");
    assertEquals(ScanKillingMode.fromConfigValue(disabled.getScanKillingMode()), ScanKillingMode.DISABLED);
  }

  @Test
  public void testScanKillingModeCaseInsensitiveParsing() {
    // fromConfigValue is case-insensitive, so "Enforce" is valid
    QueryConfig config = new QueryConfig(null, null, null, null, null, null, null, null, null, "Enforce");
    assertEquals(ScanKillingMode.fromConfigValue(config.getScanKillingMode()), ScanKillingMode.ENFORCE);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidScanKillingModeThrowsAtConstruction() {
    // A completely unrecognized value fails at construction time, not silently at query time
    new QueryConfig(null, null, null, null, null, null, null, null, null, "enforced");
  }
}
