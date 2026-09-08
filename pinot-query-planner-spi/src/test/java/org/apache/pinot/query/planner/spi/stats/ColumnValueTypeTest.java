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
package org.apache.pinot.query.planner.spi.stats;

import java.math.BigDecimal;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Covers the ordering dispatch and its degradation paths. Both matter on the query-planning path:
/// a throw here would fail a query rather than cost it an estimate.
public class ColumnValueTypeTest {

  @DataProvider(name = "orderedPairs")
  public Object[][] orderedPairs() {
    // Each row is a value that must order BELOW the next, under that type's own ordering.
    return new Object[][]{
        {ColumnValueType.LONG, "9", "10"},
        // Beyond 2^53, where a double round-trip would lose the distinction entirely.
        {ColumnValueType.LONG, "9007199254740993", "9007199254740994"},
        {ColumnValueType.DOUBLE, "1.5", "10.5"},
        {ColumnValueType.DOUBLE, "-2.5", "-1.5"},
        {ColumnValueType.BIG_DECIMAL, "9.10", "10.01"},
        // Lexical, which is exactly where a numeric ordering would disagree.
        {ColumnValueType.STRING, "10", "9"},
    };
  }

  @Test(dataProvider = "orderedPairs")
  public void testCompareOrdersByTheDeclaredType(ColumnValueType type, String lower, String higher) {
    assertTrue(type.compare(lower, higher) < 0, type + ": " + lower + " should order below " + higher);
    assertTrue(type.compare(higher, lower) > 0, type + ": " + higher + " should order above " + lower);
    assertEquals(type.compare(lower, lower), 0);
  }

  @DataProvider(name = "types")
  public Object[][] types() {
    return new Object[][]{
        {ColumnValueType.LONG}, {ColumnValueType.DOUBLE}, {ColumnValueType.BIG_DECIMAL}, {ColumnValueType.STRING}
    };
  }

  @Test(dataProvider = "types")
  public void testCompareFallsBackToLexicalOnMalformedValues(ColumnValueType type) {
    // A stored value that does not parse must degrade, not throw.
    assertEquals(type.compare("abc", "abd"), "abc".compareTo("abd"));
    assertEquals(type.compare("abc", "abc"), 0);
    // One side malformed is still enough to force the lexical path.
    assertEquals(type.compare("1", "abc"), "1".compareTo("abc"));
  }

  @Test
  public void testToComparableDeserializesByType() {
    assertEquals(ColumnValueType.LONG.toComparable("42"), 42L);
    assertEquals(ColumnValueType.DOUBLE.toComparable("42.5"), 42.5d);
    assertEquals(ColumnValueType.BIG_DECIMAL.toComparable("42.50"), new BigDecimal("42.50"));
    assertEquals(ColumnValueType.STRING.toComparable("42"), "42");
  }

  @Test(dataProvider = "types")
  public void testToComparableReturnsRawValueWhenUnparseable(ColumnValueType type) {
    assertEquals(type.toComparable("not-a-number"), "not-a-number");
  }

  @Test(dataProvider = "types")
  public void testToComparableKeepsNull(ColumnValueType type) {
    assertNull(type.toComparable(null));
  }

  @Test
  public void testFromNameResolvesEveryConstant() {
    for (ColumnValueType type : ColumnValueType.values()) {
      assertEquals(ColumnValueType.fromName(type.name()), type);
    }
  }

  @Test
  public void testFromNameYieldsNullForUnknownNames() {
    // A store written by a newer build may carry a name this one does not have. That must cost
    // precision (null means "ordering unknown"), not throw out of the planning path.
    assertNull(ColumnValueType.fromName("TIMESTAMP_WITH_TIMEZONE"));
    assertNull(ColumnValueType.fromName(""));
    assertNull(ColumnValueType.fromName("long"));
    assertNull(ColumnValueType.fromName(null));
  }
}
