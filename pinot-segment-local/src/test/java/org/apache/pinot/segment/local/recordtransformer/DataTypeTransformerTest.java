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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.utils.DataTypeTransformerUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;


public class DataTypeTransformerTest {
  private static final String COLUMN = "testColumn";

  @Test
  public void testStandardize() {
    /**
     * Tests for Map
     */

    // Empty Map
    Map<String, Object> map = Collections.emptyMap();
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, map, true));
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, map, false));

    // Map with single entry
    String expectedValue = "testValue";
    map = Collections.singletonMap("testKey", expectedValue);
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
    List<Object> list = Collections.emptyList();
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, list, true));
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, list, false));

    // List with single entry
    list = Collections.singletonList(expectedValue);
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
    map = Collections.singletonMap("testKey", Collections.emptyList());
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, map, true));
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, map, false));

    // Map with single-entry List
    map = Collections.singletonMap("testKey", Collections.singletonList(expectedValue));
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, map, true), expectedValue);
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, map, false), expectedValue);

    // Map with one empty Map and one single-entry Map
    map = new HashMap<>();
    map.put("testKey1", Collections.emptyMap());
    map.put("testKey2", Collections.singletonMap("testKey", expectedValue));
    // Can be standardized into single value because empty Map should be ignored
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, map, true), expectedValue);
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, map, false), expectedValue);

    // Map with multi-entries List
    map = Collections.singletonMap("testKey", Arrays.asList(expectedValues));
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
    map.put("testKey1", Collections.emptyMap());
    map.put("testKey2", Collections.singletonList("testValue1"));
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
        .asList(Collections.singletonMap("testKey", "testValue1"), Collections.singletonMap("testKey", "testValue2"),
            Collections.emptyMap());
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
        Collections.singletonMap("testKey", "testValue1"), Collections.singletonMap("testKey", "testValue2")
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
        new Object[0], Collections.singletonList(Collections.singletonMap("testKey", "testValue1")),
        Collections.singletonMap("testKey", Arrays.asList(new Object[]{"testValue2"}, Collections.emptyMap()))
    };
    try {
      DataTypeTransformerUtils.standardize(COLUMN, values, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEqualsNoOrder((Object[]) DataTypeTransformerUtils.standardize(COLUMN, values, false), expectedValues);
  }

  /**
   * Verifies that non-canonical (uppercase) UUID strings in upsert/dedup primary key columns are rejected,
   * while canonical lowercase UUIDs are accepted, and non-primary-key UUID columns are unaffected.
   */
  @Test
  public void testUuidUpsertPrimaryKeyCanonicalValidation() {
    String uuidCol = "uuidPk";
    String nonPkUuidCol = "uuidOther";
    String canonicalUuid = "550e8400-e29b-41d4-a716-446655440000";
    String uppercaseUuid = "550E8400-E29B-41D4-A716-446655440000";

    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(uuidCol, FieldSpec.DataType.UUID)
        .addSingleValueDimension(nonPkUuidCol, FieldSpec.DataType.UUID)
        .setPrimaryKeyColumns(List.of(uuidCol))
        .build();
    TableConfig upsertTableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testUpsertUuid")
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .build();

    DataTypeTransformer upsertTransformer = new DataTypeTransformer(upsertTableConfig, schema);

    // Canonical lowercase UUID primary key: accepted
    GenericRow canonicalRow = new GenericRow();
    canonicalRow.putValue(uuidCol, canonicalUuid);
    canonicalRow.putValue(nonPkUuidCol, canonicalUuid);
    upsertTransformer.transform(canonicalRow); // must not throw

    // Non-canonical uppercase UUID primary key: rejected
    GenericRow uppercaseRow = new GenericRow();
    uppercaseRow.putValue(uuidCol, uppercaseUuid);
    uppercaseRow.putValue(nonPkUuidCol, canonicalUuid);
    try {
      upsertTransformer.transform(uppercaseRow);
      fail("Expected RuntimeException for non-canonical UUID primary key in upsert table");
    } catch (RuntimeException e) {
      // Expected: DataTypeTransformer wraps the IllegalArgumentException in a RuntimeException
    }

    // Non-canonical uppercase UUID in a NON-primary-key column: accepted (no restriction)
    GenericRow nonPkUppercaseRow = new GenericRow();
    nonPkUppercaseRow.putValue(uuidCol, canonicalUuid);
    nonPkUppercaseRow.putValue(nonPkUuidCol, uppercaseUuid);
    upsertTransformer.transform(nonPkUppercaseRow); // must not throw

    // For a non-upsert table, non-canonical UUID in primary-key column is also accepted
    TableConfig offlineTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testOfflineUuid").build();
    DataTypeTransformer offlineTransformer = new DataTypeTransformer(offlineTableConfig, schema);
    GenericRow offlineUppercaseRow = new GenericRow();
    offlineUppercaseRow.putValue(uuidCol, uppercaseUuid);
    offlineUppercaseRow.putValue(nonPkUuidCol, uppercaseUuid);
    offlineTransformer.transform(offlineUppercaseRow); // must not throw
  }
}
