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
package org.apache.pinot.spi.data;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class OpenStructNamingTest {

  @Test
  public void testMaterializedColumnName() {
    assertEquals(OpenStructNaming.materializedColumnName("metrics", "tenancy"), "metrics$tenancy");
  }

  @Test
  public void testSparseColumnName() {
    assertEquals(OpenStructNaming.sparseColumnName("metrics"), "metrics$__sparse__");
  }

  @Test
  public void testIsMaterializedOpenStructColumn() {
    assertTrue(OpenStructNaming.isMaterializedOpenStructColumn("metrics$tenancy"));
    assertTrue(OpenStructNaming.isMaterializedOpenStructColumn("metrics$__sparse__"));
    assertFalse(OpenStructNaming.isMaterializedOpenStructColumn("metrics"));
    assertFalse(OpenStructNaming.isMaterializedOpenStructColumn("normal_column"));
  }

  @Test
  public void testParseOpenStructColumn() {
    assertEquals(OpenStructNaming.parseOpenStructColumn("metrics$tenancy"), "metrics");
    assertEquals(OpenStructNaming.parseOpenStructColumn("m__data$key$nested"), "m__data");
  }

  @Test
  public void testParseKey() {
    assertEquals(OpenStructNaming.parseKey("metrics$tenancy"), "tenancy");
    assertEquals(OpenStructNaming.parseKey("metrics$__sparse__"), "__sparse__");
  }

  @Test
  public void testIsSparseColumn() {
    assertTrue(OpenStructNaming.isSparseColumn("metrics$__sparse__"));
    assertFalse(OpenStructNaming.isSparseColumn("metrics$tenancy"));
  }

  @Test
  public void testRoundTrip() {
    String openStructCol = "event_props";
    String key = "country_iso2";
    String materialized = OpenStructNaming.materializedColumnName(openStructCol, key);
    assertEquals(OpenStructNaming.parseOpenStructColumn(materialized), openStructCol);
    assertEquals(OpenStructNaming.parseKey(materialized), key);
    assertTrue(OpenStructNaming.isMaterializedOpenStructColumn(materialized));
    assertFalse(OpenStructNaming.isSparseColumn(materialized));
  }
}
