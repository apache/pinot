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
import static org.testng.Assert.assertTrue;


public class QueryConfigOomPauseTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testOomPauseFieldsDefaultToNull() {
    QueryConfig config = new QueryConfig(null, null, null, null, null, null, null, null, null);
    assertNull(config.getOomPreQueryKillPauseDurationMs());
    assertNull(config.getOomPanicAllowPreQueryKillPause());
  }

  @Test
  public void testOomPauseFieldsSetExplicitly() {
    QueryConfig config = new QueryConfig(null, null, null, null, null, null, null, null, null, 3000L, true);
    assertEquals(config.getOomPreQueryKillPauseDurationMs(), Long.valueOf(3000L));
    assertEquals(config.getOomPanicAllowPreQueryKillPause(), Boolean.TRUE);
  }

  @Test
  public void testOomPauseDisabledExplicitlyByZero() {
    QueryConfig config = new QueryConfig(null, null, null, null, null, null, null, null, null, 0L, false);
    assertEquals(config.getOomPreQueryKillPauseDurationMs(), Long.valueOf(0L));
    assertEquals(config.getOomPanicAllowPreQueryKillPause(), Boolean.FALSE);
  }

  @Test
  public void testJsonRoundTripWithOomPauseFields()
      throws Exception {
    QueryConfig config = new QueryConfig(30000L, null, null, null, null, null, null, null, null, 3000L, true);

    String json = OBJECT_MAPPER.writeValueAsString(config);
    assertTrue(json.contains("\"oomPreQueryKillPauseDurationMs\":3000"), "json missing oomPreQueryKillPauseDurationMs");
    assertTrue(json.contains("\"oomPanicAllowPreQueryKillPause\":true"), "json missing oomPanicAllowPreQueryKillPause");

    QueryConfig deserialized = OBJECT_MAPPER.readValue(json, QueryConfig.class);
    assertEquals(deserialized.getOomPreQueryKillPauseDurationMs(), Long.valueOf(3000L));
    assertEquals(deserialized.getOomPanicAllowPreQueryKillPause(), Boolean.TRUE);
  }

  @Test
  public void testJsonDeserializationWithoutOomPauseFieldsPreservesNull()
      throws Exception {
    String json = "{\"timeoutMs\": 5000}";
    QueryConfig config = OBJECT_MAPPER.readValue(json, QueryConfig.class);
    assertEquals(config.getTimeoutMs(), Long.valueOf(5000L));
    assertNull(config.getOomPreQueryKillPauseDurationMs());
    assertNull(config.getOomPanicAllowPreQueryKillPause());
  }

  @Test
  public void testJsonDeserializationWithOnlyOomPauseFields()
      throws Exception {
    String json = "{\"oomPreQueryKillPauseDurationMs\": 3000, \"oomPanicAllowPreQueryKillPause\": true}";
    QueryConfig config = OBJECT_MAPPER.readValue(json, QueryConfig.class);
    assertNull(config.getTimeoutMs());
    assertEquals(config.getOomPreQueryKillPauseDurationMs(), Long.valueOf(3000L));
    assertEquals(config.getOomPanicAllowPreQueryKillPause(), Boolean.TRUE);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNegativeOomPauseDurationThrows() {
    new QueryConfig(null, null, null, null, null, null, null, null, null, -1L, null);
  }

  @Test
  public void testZeroOomPauseDurationAllowedAsDisableSignal() {
    // Zero is a valid "disable pause for this table" signal — the accountant treats <= 0 as disabled.
    QueryConfig config = new QueryConfig(null, null, null, null, null, null, null, null, null, 0L, null);
    assertEquals(config.getOomPreQueryKillPauseDurationMs(), Long.valueOf(0L));
  }
}
