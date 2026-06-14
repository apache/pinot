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
package org.apache.pinot.segment.local.recordtransformer;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.utils.DataTypeTransformerUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class DataTypeTransformerTest {
  private static final String COLUMN = "testColumn";

  /**
   * The JSON parse-once cache: for a JSON column with a JSON index, the DataTypeTransformer caches the parsed Map on
   * the row (so the index can flatten it directly) and still serializes the column value to a string. For a JSON
   * column without a JSON index, nothing is cached.
   */
  @Test
  public void testJsonParseOnceCache() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("jsonCol", FieldSpec.DataType.JSON).build();
    Map<String, Object> doc = new HashMap<>();
    doc.put("a", 1);
    doc.put("b", "x");

    TableConfig withIndex = new TableConfigBuilder(TableType.OFFLINE).setTableName("t")
        .setJsonIndexColumns(Collections.singletonList("jsonCol")).build();
    GenericRow indexed = new GenericRow();
    indexed.putValue("jsonCol", doc);
    new DataTypeTransformer(withIndex, schema).transform(indexed);
    // The column value is serialized for the forward index...
    assertTrue(indexed.getValue("jsonCol") instanceof String);
    // ...and the parsed Map is cached for the JSON index to flatten directly.
    assertEquals(indexed.getParsedJsonValue("jsonCol"), doc);

    // No JSON index on the column -> no caching (the index, if any, would re-parse as before).
    TableConfig noIndex = new TableConfigBuilder(TableType.OFFLINE).setTableName("t").build();
    GenericRow unindexed = new GenericRow();
    unindexed.putValue("jsonCol", doc);
    new DataTypeTransformer(noIndex, schema).transform(unindexed);
    assertTrue(unindexed.getValue("jsonCol") instanceof String);
    assertNull(unindexed.getParsedJsonValue("jsonCol"));
  }

  /**
   * The common case: a JSON column is fed as JSON text (e.g. a stringified log payload). With a JSON index, the
   * transformer parses it once, caches the JsonNode for the index, and the forward-index string is byte-identical to
   * the normal conversion. A non-JSON string falls through to the normal conversion with nothing cached.
   */
  @Test
  public void testJsonParseOnceCacheFromString() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("jsonCol", FieldSpec.DataType.JSON).build();
    TableConfig withIndex = new TableConfigBuilder(TableType.OFFLINE).setTableName("t")
        .setJsonIndexColumns(Collections.singletonList("jsonCol")).build();
    TableConfig noIndex = new TableConfigBuilder(TableType.OFFLINE).setTableName("t").build();

    // Includes a fractional number (BigDecimal canonicalization) and an integer to cover both flatten paths.
    for (String json : new String[]{"{\"b\":\"x\",\"a\":1}", "{\"bd\":1234567890.5,\"n\":42}"}) {
      // Reference: the same column without a JSON index serializes via the normal PinotDataType conversion.
      GenericRow reference = new GenericRow();
      reference.putValue("jsonCol", json);
      new DataTypeTransformer(noIndex, schema).transform(reference);
      assertNull(reference.getParsedJsonValue("jsonCol"));

      GenericRow row = new GenericRow();
      row.putValue("jsonCol", json);
      new DataTypeTransformer(withIndex, schema).transform(row);
      // Forward-index value is byte-identical to the normal conversion...
      assertEquals(row.getValue("jsonCol"), reference.getValue("jsonCol"));
      // ...and the parsed node is cached for the JSON index to flatten directly.
      assertTrue(row.getParsedJsonValue("jsonCol") instanceof JsonNode);
    }

    // A non-JSON string is not cached and is serialized exactly as the normal conversion would.
    GenericRow nonJsonReference = new GenericRow();
    nonJsonReference.putValue("jsonCol", "not json");
    new DataTypeTransformer(noIndex, schema).transform(nonJsonReference);
    GenericRow nonJson = new GenericRow();
    nonJson.putValue("jsonCol", "not json");
    new DataTypeTransformer(withIndex, schema).transform(nonJson);
    assertEquals(nonJson.getValue("jsonCol"), nonJsonReference.getValue("jsonCol"));
    assertNull(nonJson.getParsedJsonValue("jsonCol"));
  }

  /**
   * A transformer that runs after DataTypeTransformer and rewrites a JSON column's value must not leave a stale parsed
   * node cached: SanitizationTransformer trims an over-length JSON string, which has to invalidate the cache so the
   * JSON index re-parses the trimmed forward value instead of flattening the original (full) document.
   */
  @Test
  public void testParsedJsonCacheInvalidatedBySanitization() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("jsonCol", FieldSpec.DataType.JSON).build();
    FieldSpec jsonSpec = schema.getFieldSpecFor("jsonCol");
    jsonSpec.setMaxLength(8);
    jsonSpec.setMaxLengthExceedStrategy(FieldSpec.MaxLengthExceedStrategy.TRIM_LENGTH);
    TableConfig withIndex = new TableConfigBuilder(TableType.OFFLINE).setTableName("t")
        .setJsonIndexColumns(Collections.singletonList("jsonCol")).build();

    GenericRow row = new GenericRow();
    row.putValue("jsonCol", "{\"a\":\"averylongvalue\",\"b\":2}");
    new DataTypeTransformer(withIndex, schema).transform(row);
    // After type transformation the full parsed node is cached for the JSON index.
    assertTrue(row.getParsedJsonValue("jsonCol") instanceof JsonNode);

    // Sanitization trims the forward-index string; the stale node must be dropped so the index re-parses the trimmed
    // value (which the forward index now holds) rather than flattening the full document.
    new SanitizationTransformer(schema).transform(row);
    assertTrue(((String) row.getValue("jsonCol")).length() <= 8);
    assertNull(row.getParsedJsonValue("jsonCol"));
  }

  @Test
  public void testStandardize() {
    /**
     * Tests for Map
     */

    // Empty Map
    Map<String, Object> map = Map.of();
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, map, true));
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, map, false));

    // Map with single entry
    String expectedValue = "testValue";
    map = Map.of("testKey", expectedValue);
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, map, true), expectedValue);
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, map, false), expectedValue);

    // Map with multiple entries
    Object[] expectedValues = new Object[]{"testValue1", "testValue2"};
    map = new HashMap<>();
    map.put("testKey1", "testValue1");
    map.put("testKey2", "testValue2");
    try {
      // Should fail because Map with multiple entries cannot be standardized as single value
      DataTypeTransformerUtils.standardize(COLUMN, map, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEqualsNoOrder((Object[]) DataTypeTransformerUtils.standardize(COLUMN, map, false), expectedValues);

    /**
     * Tests for List
     */

    // Empty List
    List<Object> list = List.of();
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, list, true));
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, list, false));

    // List with single entry
    list = List.of(expectedValue);
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, list, true), expectedValue);
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, list, false), expectedValue);

    // List with multiple entries
    list = Arrays.asList(expectedValues);
    try {
      // Should fail because List with multiple entries cannot be standardized as single value
      DataTypeTransformerUtils.standardize(COLUMN, list, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEquals((Object[]) DataTypeTransformerUtils.standardize(COLUMN, list, false), expectedValues);

    /**
     * Tests for Object[]
     */

    // Empty Object[]
    Object[] values = new Object[0];
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, values, true));
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, values, false));

    // Object[] with single entry
    values = new Object[]{expectedValue};
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, values, true), expectedValue);
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, values, false), expectedValue);

    // Object[] with multiple entries
    values = new Object[]{"testValue1", "testValue2"};
    try {
      // Should fail because Object[] with multiple entries cannot be standardized as single value
      DataTypeTransformerUtils.standardize(COLUMN, values, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEquals((Object[]) DataTypeTransformerUtils.standardize(COLUMN, values, false), expectedValues);

    /**
     * Tests for nested Map/List/Object[]
     */

    // Map with empty List
    map = Map.of("testKey", List.of());
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, map, true));
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, map, false));

    // Map with single-entry List
    map = Map.of("testKey", List.of(expectedValue));
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, map, true), expectedValue);
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, map, false), expectedValue);

    // Map with one empty Map and one single-entry Map
    map = new HashMap<>();
    map.put("testKey1", Map.of());
    map.put("testKey2", Map.of("testKey", expectedValue));
    // Can be standardized into single value because empty Map should be ignored
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, map, true), expectedValue);
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, map, false), expectedValue);

    // Map with multi-entries List
    map = Map.of("testKey", Arrays.asList(expectedValues));
    try {
      // Should fail because Map with multiple entries cannot be standardized as single value
      DataTypeTransformerUtils.standardize(COLUMN, map, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEqualsNoOrder((Object[]) DataTypeTransformerUtils.standardize(COLUMN, map, false), expectedValues);

    // Map with one empty Map, one single-entry List and one single-entry Object[]
    map = new HashMap<>();
    map.put("testKey1", Map.of());
    map.put("testKey2", List.of("testValue1"));
    map.put("testKey3", new Object[]{"testValue2"});
    try {
      // Should fail because Map with multiple entries cannot be standardized as single value
      DataTypeTransformerUtils.standardize(COLUMN, map, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEqualsNoOrder((Object[]) DataTypeTransformerUtils.standardize(COLUMN, map, false), expectedValues);

    // List with two single-entry Maps and one empty Map
    list = Arrays
        .asList(Map.of("testKey", "testValue1"), Map.of("testKey", "testValue2"),
            Map.of());
    try {
      // Should fail because List with multiple entries cannot be standardized as single value
      DataTypeTransformerUtils.standardize(COLUMN, list, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEquals((Object[]) DataTypeTransformerUtils.standardize(COLUMN, list, false), expectedValues);

    // Object[] with two single-entry Maps
    values = new Object[]{
        Map.of("testKey", "testValue1"), Map.of("testKey", "testValue2")
    };
    try {
      // Should fail because Object[] with multiple entries cannot be standardized as single value
      DataTypeTransformerUtils.standardize(COLUMN, values, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEqualsNoOrder((Object[]) DataTypeTransformerUtils.standardize(COLUMN, values, false), expectedValues);

    // Object[] with one empty Object[], one multi-entries List of nested Map/List/Object[]
    values = new Object[]{
        new Object[0], List.of(Map.of("testKey", "testValue1")),
        Map.of("testKey", Arrays.asList(new Object[]{"testValue2"}, Map.of()))
    };
    try {
      DataTypeTransformerUtils.standardize(COLUMN, values, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEqualsNoOrder((Object[]) DataTypeTransformerUtils.standardize(COLUMN, values, false), expectedValues);
  }
}
