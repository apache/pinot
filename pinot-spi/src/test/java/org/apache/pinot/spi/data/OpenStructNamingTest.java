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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;


public class OpenStructNamingTest {

  @Test
  public void testMaterializedColumnName() {
    assertEquals(OpenStructNaming.materializedColumnName("metrics", "clicks"), "metrics$clicks");
  }

  @Test
  public void testSparseColumnName() {
    assertEquals(OpenStructNaming.sparseColumnName("metrics"), "metrics$__sparse__");
  }

  @Test
  public void testIsMaterializedOpenStructColumn() {
    assertTrue(OpenStructNaming.isMaterializedOpenStructColumn("metrics$clicks"));
    assertTrue(OpenStructNaming.isMaterializedOpenStructColumn("metrics$__sparse__"));
    assertFalse(OpenStructNaming.isMaterializedOpenStructColumn("metrics"));
    assertFalse(OpenStructNaming.isMaterializedOpenStructColumn("plain_column"));
  }

  @Test
  public void testIsSparseColumn() {
    assertTrue(OpenStructNaming.isSparseColumn("metrics$__sparse__"));
    assertFalse(OpenStructNaming.isSparseColumn("metrics$clicks"));
    assertFalse(OpenStructNaming.isSparseColumn("metrics"));
  }

  @Test
  public void testParseParentColumn() {
    assertEquals(OpenStructNaming.parseParentColumn("metrics$clicks"), "metrics");
    assertEquals(OpenStructNaming.parseParentColumn("metrics$__sparse__"), "metrics");
  }

  @Test
  public void testParseKey() {
    assertEquals(OpenStructNaming.parseKey("metrics$clicks"), "clicks");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseKeyRejectsSparse() {
    OpenStructNaming.parseKey("metrics$__sparse__");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseKeyRejectsNonMaterialized() {
    OpenStructNaming.parseKey("metrics");
  }

  /// Only the four characters ObjectName.quote backslash-escapes are escaped, plus '%' itself so the
  /// mapping stays reversible. Everything else has to survive, or keys that differ only in punctuation
  /// would collapse into one metric series.
  @Test
  public void testMetricKeyEscapesOnlyObjectNameEscapedChars() {
    assertEquals(OpenStructNaming.metricKey("metrics", "pro\"mo"), "metrics$pro%22mo");
    assertEquals(OpenStructNaming.metricKey("metrics", "pro\\mo"), "metrics$pro%5Cmo");
    assertEquals(OpenStructNaming.metricKey("metrics", "pro*mo"), "metrics$pro%2Amo");
    assertEquals(OpenStructNaming.metricKey("metrics", "pro?mo"), "metrics$pro%3Fmo");
    assertEquals(OpenStructNaming.metricKey("metrics", "pro%mo"), "metrics$pro%25mo");

    // Untouched: these are all safe inside a quoted ObjectName and in a Prometheus label value.
    assertEquals(OpenStructNaming.metricKey("metrics", "clicks.v2$promo-code"), "metrics$clicks.v2$promo-code");
    assertEquals(OpenStructNaming.metricKey("metrics", "a,b"), "metrics$a,b");
    assertEquals(OpenStructNaming.metricKey("metrics", "a=b"), "metrics$a=b");
    assertEquals(OpenStructNaming.metricKey("metrics", "a b"), "metrics$a b");
    assertEquals(OpenStructNaming.metricKey("metrics", "user_id"), "metrics$user_id");
  }

  /// The escaping must be injective. Folding the four to '_' would not be: '_' is itself a legal key
  /// character, so 'a"b' and 'a_b' would land on the same gauge and each seal would silently overwrite
  /// the other. '%' is escaped for the same reason -- without it 'a%22b' would collide with 'a"b'.
  @Test
  public void testMetricKeyEscapingIsInjective() {
    assertNotEquals(OpenStructNaming.metricKey("metrics", "a\"b"), OpenStructNaming.metricKey("metrics", "a_b"));
    assertNotEquals(OpenStructNaming.metricKey("metrics", "a\"b"), OpenStructNaming.metricKey("metrics", "a%22b"));
    assertNotEquals(OpenStructNaming.metricKey("metrics", "a\\b"), OpenStructNaming.metricKey("metrics", "a%5Cb"));

    // The four escaped characters must not collide with each other either.
    Set<String> encoded = new HashSet<>();
    for (String key : List.of("a\"b", "a\\b", "a*b", "a?b", "a%b", "a_b")) {
      assertTrue(encoded.add(OpenStructNaming.metricKey("metrics", key)), "collision on key: " + key);
    }
  }

  /// user.id / user-id / user_id must stay distinct series -- the reason escaping is narrow.
  @Test
  public void testMetricKeyKeepsPunctuationVariantsDistinct() {
    String a = OpenStructNaming.metricKey("metrics", "user.id");
    String b = OpenStructNaming.metricKey("metrics", "user-id");
    String c = OpenStructNaming.metricKey("metrics", "user_id");
    assertNotEquals(a, b);
    assertNotEquals(b, c);
    assertNotEquals(a, c);
  }

  /// A key needing no escaping must produce exactly the materialized column name, so the dense-key
  /// metric and its on-disk column stay addressable by the same string in the common case.
  @Test
  public void testMetricKeyMatchesMaterializedNameWhenNoEscapingNeeded() {
    assertEquals(OpenStructNaming.metricKey("metrics", "clicks"),
        OpenStructNaming.materializedColumnName("metrics", "clicks"));
  }

  @Test
  public void testMetricKeyHandlesEmptyAndAllEscapedKeys() {
    assertEquals(OpenStructNaming.metricKey("metrics", ""), "metrics$");
    assertEquals(OpenStructNaming.metricKey("metrics", "\"\\*?%"), "metrics$%22%5C%2A%3F%25");
  }
}
