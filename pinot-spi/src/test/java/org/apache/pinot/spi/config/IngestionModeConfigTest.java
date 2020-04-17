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
package org.apache.pinot.spi.config;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class IngestionModeConfigTest {

  @Test
  public void testIngestionMode() {
    // test append table no.1
    IngestionModeConfig ingestionModeConfig = new IngestionModeConfig("abc", null,
        null, null, null);
    assertFalse(ingestionModeConfig.isForUpsert());

    ingestionModeConfig = new IngestionModeConfig("append", null,
        null, null, null);
    assertFalse(ingestionModeConfig.isForUpsert());

    ingestionModeConfig = new IngestionModeConfig("APPEND", null,
        null, null, null);
    assertFalse(ingestionModeConfig.isForUpsert());

    // test append table no.2
    ingestionModeConfig = new IngestionModeConfig(null, null, null,
        null, null);
    assertFalse(ingestionModeConfig.isForUpsert());

    // test regular upsert table
    ingestionModeConfig = new IngestionModeConfig("upsert", ImmutableList.of("primaryKey"),
        "offset", "validFrom", "validUntil");
    assertTrue(ingestionModeConfig.isForUpsert());
    assertEquals(1, ingestionModeConfig.getPrimaryKeys().size());
    assertEquals("primaryKey", ingestionModeConfig.getPrimaryKeys().get(0));
    assertEquals("offset", ingestionModeConfig.getOffsetKey());
    assertEquals("validFrom", ingestionModeConfig.getValidFromKey());
    assertEquals("validUntil", ingestionModeConfig.getValidUntilKey());

    ingestionModeConfig = new IngestionModeConfig("UPSERT", ImmutableList.of("primaryKey"),
        "offset", "validFrom", "validUntil");
    assertTrue(ingestionModeConfig.isForUpsert());

    // test sanity check
    try {
      ingestionModeConfig = new IngestionModeConfig("UPSERT", null,
          "offset", "validFrom", "validUntil");
      fail();
    } catch (RuntimeException ex) {}

    try {
      ingestionModeConfig = new IngestionModeConfig("UPSERT", ImmutableList.of("pk1", "pk2"),
          "offset", "validFrom", "validUntil");
      fail();
    } catch (RuntimeException ex) {}

    try {
      ingestionModeConfig = new IngestionModeConfig("UPSERT", ImmutableList.of("pk1"),
          null, "validFrom", "validUntil");
      fail();
    } catch (RuntimeException ex) {}

    try {
      ingestionModeConfig = new IngestionModeConfig("UPSERT", ImmutableList.of("pk1"),
          "offset", null, "validUntil");
      fail();
    } catch (RuntimeException ex) {}

    try {
      ingestionModeConfig = new IngestionModeConfig("UPSERT", ImmutableList.of("pk1"),
          "offset", "validFrom", null);
      fail();
    } catch (RuntimeException ex) {}
  }
}