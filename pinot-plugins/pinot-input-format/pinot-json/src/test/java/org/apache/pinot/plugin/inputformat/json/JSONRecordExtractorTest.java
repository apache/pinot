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
package org.apache.pinot.plugin.inputformat.json;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Tests [JSONRecordExtractor] — see its class Javadoc for the JSON source type → Java output type matrix.
public class JSONRecordExtractorTest {

  private static final String COLUMN = "col";

  // === Single value — order follows the type list in the class Javadoc ===

  @Test
  public void testBooleanPreserved() {
    Object trueResult = extract(true);
    assertEquals(trueResult, true);
    Object falseResult = extract(false);
    assertEquals(falseResult, false);
  }

  @Test
  public void testIntegerPreserved() {
    Object result = extract(42);
    assertEquals(result, 42);
  }

  @Test
  public void testLongPreserved() {
    Object result = extract(1_588_469_340_000L);
    assertEquals(result, 1_588_469_340_000L);
  }

  @Test
  public void testBigIntegerWidenedToBigDecimal() {
    // Default Jackson parses integer literals that overflow `Long` as `BigInteger`. The base widens to
    // `BigDecimal` since Pinot has no `BigInteger` type.
    BigInteger value = new BigInteger("99999999999999999999999999");
    Object result = extract(value);
    assertEquals(result, new BigDecimal(value));
  }

  @Test
  public void testDoublePreserved() {
    Object result = extract(1.5d);
    assertEquals(result, 1.5d);
  }

  @Test
  public void testStringPreserved() {
    Object result = extract("hello");
    assertEquals(result, "hello");
  }

  @Test
  public void testNullPassedThrough() {
    assertNull(extract(null));
  }

  // === List (JSON array) → Object[]; element order again follows the type list ===

  @Test
  public void testBooleanListExtractedAsArray() {
    Object[] result = (Object[]) extract(List.of(true, false, true));
    assertEquals(result, new Object[]{true, false, true});
  }

  @Test
  public void testIntegerListExtractedAsArray() {
    Object[] result = (Object[]) extract(List.of(10, 20, 30));
    assertEquals(result, new Object[]{10, 20, 30});
  }

  @Test
  public void testLongListExtractedAsArray() {
    Object[] result = (Object[]) extract(List.of(1_588_469_340_000L, 2_588_469_340_000L));
    assertEquals(result, new Object[]{1_588_469_340_000L, 2_588_469_340_000L});
  }

  @Test
  public void testDoubleListExtractedAsArray() {
    Object[] result = (Object[]) extract(List.of(1.1, 2.2, 3.3));
    assertEquals(result, new Object[]{1.1, 2.2, 3.3});
  }

  @Test
  public void testStringListExtractedAsArray() {
    Object[] result = (Object[]) extract(List.of("foo", "bar"));
    assertEquals(result, new Object[]{"foo", "bar"});
  }

  @Test
  public void testListWithNullElement() {
    // `[1, null, 3]` is valid JSON; the parsed list has null at index 1. (`List.of` rejects nulls.)
    Object[] result = (Object[]) extract(Arrays.asList(1, null, 3));
    assertEquals(result, new Object[]{1, null, 3});
  }

  @Test
  public void testEmptyListExtractedAsEmptyArray() {
    Object[] result = (Object[]) extract(List.of());
    assertEquals(result, new Object[]{});
  }

  @Test
  public void testHeterogeneousListExtractedAsArray() {
    // JSON arrays can hold mixed types (`[1, "a", true]`); each element is converted independently.
    Object[] result = (Object[]) extract(List.of(1, "a", true, 1.5d));
    assertEquals(result, new Object[]{1, "a", true, 1.5d});
  }

  // === Map (JSON object) — recursive cases ===

  @Test
  public void testMapWithStringKeys() {
    Map<?, ?> result = (Map<?, ?>) extract(Map.of(
        "k1", 1,
        "k2", "foo",
        "k3", true
    ));
    assertEquals(result.size(), 3);
    assertEquals(result.get("k1"), 1);
    assertEquals(result.get("k2"), "foo");
    assertEquals(result.get("k3"), true);
  }

  @Test
  public void testMapWithListValue() {
    // Inner List values are recursively converted to Object[].
    Map<?, ?> result = (Map<?, ?>) extract(Map.of(
        "nums", List.of(1, 2, 3),
        "words", List.of("a", "b")
    ));
    assertEquals((Object[]) result.get("nums"), new Object[]{1, 2, 3});
    assertEquals((Object[]) result.get("words"), new Object[]{"a", "b"});
  }

  @Test
  public void testNestedMap() {
    Map<?, ?> result = (Map<?, ?>) extract(Map.of("k", Map.of(
        "sub1", 1.1,
        "sub2", 1.2
    )));
    Map<?, ?> innerResult = (Map<?, ?>) result.get("k");
    assertEquals(innerResult.get("sub1"), 1.1);
    assertEquals(innerResult.get("sub2"), 1.2);
  }

  @Test
  public void testListOfMaps() {
    Object[] result = (Object[]) extract(List.of(
        Map.of(
            "one", 1,
            "two", "foo"
        ),
        Map.of(
            "one", 11,
            "two", "bar"
        )
    ));
    assertEquals(result.length, 2);
    Map<?, ?> r0 = (Map<?, ?>) result[0];
    assertEquals(r0.get("one"), 1);
    assertEquals(r0.get("two"), "foo");
    Map<?, ?> r1 = (Map<?, ?>) result[1];
    assertEquals(r1.get("one"), 11);
    assertEquals(r1.get("two"), "bar");
  }

  @Test
  public void testListOfNestedMapsAndLists() {
    // A real-world JSON shape: list of records where each record has a scalar, a nested map, and a list field.
    Object[] result = (Object[]) extract(List.of(Map.of(
        "scalar", 1,
        "nested", Map.of(
            "sub1", 1.1,
            "sub2", 1.2
        ),
        "list", List.of("a", "b")
    )));
    Map<?, ?> r0 = (Map<?, ?>) result[0];
    assertEquals(r0.get("scalar"), 1);
    assertEquals(((Map<?, ?>) r0.get("nested")).get("sub1"), 1.1);
    assertEquals((Object[]) r0.get("list"), new Object[]{"a", "b"});
  }

  // === Helpers ===

  /// Run the extractor against a single-column input map and return the extracted value.
  private static Object extract(@Nullable Object input) {
    JSONRecordExtractor extractor = new JSONRecordExtractor();
    extractor.init(null, null);
    Map<String, Object> record = new HashMap<>();
    record.put(COLUMN, input);
    GenericRow row = new GenericRow();
    extractor.extract(record, row);
    return row.getValue(COLUMN);
  }
}
