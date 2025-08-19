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
package org.apache.pinot.query.runtime.operator.join;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.core.data.table.Key;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class LookupTableTest {

  @DataProvider(name = "lookupTableProvider")
  public Object[][] provideLookupTables() {
    return new Object[][]{
        {new IntLookupTable(), "IntLookupTable"},
        {new LongLookupTable(), "LongLookupTable"},
        {new FloatLookupTable(), "FloatLookupTable"},
        {new DoubleLookupTable(), "DoubleLookupTable"},
        {new ObjectLookupTable(), "ObjectLookupTable"}
    };
  }

  @Test(dataProvider = "lookupTableProvider")
  public void testEmptyTable(LookupTable table, String tableName) {
    // Empty table should have unique keys by default
    assertTrue(table.isKeysUnique(), tableName + " should start with unique keys");

    // Lookup on empty table should return null
    assertNull(table.lookup(getTestKey(table, 1)), tableName + " lookup should return null for empty table");

    // ContainsKey on empty table should return false
    assertFalse(table.containsKey(getTestKey(table, 1)),
        tableName + " containsKey should return false for empty table");

    // EntrySet should be empty
    assertTrue(table.entrySet().isEmpty(), tableName + " entrySet should be empty");

    // Finish should work on empty table
    table.finish();
  }

  @Test(dataProvider = "lookupTableProvider")
  public void testSingleRowUniqueKey(LookupTable table, String tableName) {
    Object[] row1 = {"value1", 100, 1.5};
    Object key1 = getTestKey(table, 1);

    table.addRow(key1, row1);
    table.finish();

    // Should still be unique
    assertTrue(table.isKeysUnique(), tableName + " should remain unique with single row");

    // Should contain the key
    assertTrue(table.containsKey(key1), tableName + " should contain the added key");

    // Lookup should return the row directly (not as list)
    Object result = table.lookup(key1);
    assertNotNull(result, tableName + " lookup should not return null");
    assertTrue(result instanceof Object[], tableName + " lookup should return Object[] for unique keys");
    assertEquals(result, row1, tableName + " lookup should return the exact row");

    // Should not contain other keys
    assertFalse(table.containsKey(getTestKey(table, 2)), tableName + " should not contain non-added keys");

    // EntrySet should have one entry
    Set<Map.Entry<Object, Object>> entries = table.entrySet();
    assertEquals(entries.size(), 1, tableName + " entrySet should have exactly one entry");
  }

  @Test(dataProvider = "lookupTableProvider")
  public void testMultipleRowsUniqueKeys(LookupTable table, String tableName) {
    Object[] row1 = {"value1", 100, 1.5};
    Object[] row2 = {"value2", 200, 2.5};
    Object[] row3 = {"value3", 300, 3.5};

    Object key1 = getTestKey(table, 1);
    Object key2 = getTestKey(table, 2);
    Object key3 = getTestKey(table, 3);

    table.addRow(key1, row1);
    table.addRow(key2, row2);
    table.addRow(key3, row3);
    table.finish();

    // Should remain unique
    assertTrue(table.isKeysUnique(), tableName + " should remain unique with multiple unique keys");

    // Should contain all keys
    assertTrue(table.containsKey(key1), tableName + " should contain key1");
    assertTrue(table.containsKey(key2), tableName + " should contain key2");
    assertTrue(table.containsKey(key3), tableName + " should contain key3");

    // Lookups should return correct rows
    assertEquals(table.lookup(key1), row1, tableName + " should return correct row for key1");
    assertEquals(table.lookup(key2), row2, tableName + " should return correct row for key2");
    assertEquals(table.lookup(key3), row3, tableName + " should return correct row for key3");

    // EntrySet should have three entries
    assertEquals(table.entrySet().size(), 3, tableName + " entrySet should have three entries");
  }

  @Test(dataProvider = "lookupTableProvider")
  public void testDuplicateKeys(LookupTable table, String tableName) {
    Object[] row1 = {"value1", 100, 1.5};
    Object[] row2 = {"value2", 200, 2.5};
    Object[] row3 = {"value3", 300, 3.5};

    Object key1 = getTestKey(table, 1);

    table.addRow(key1, row1);
    table.addRow(key1, row2);  // Duplicate key
    table.addRow(key1, row3);  // Another duplicate
    table.finish();

    // Should no longer be unique
    assertFalse(table.isKeysUnique(), tableName + " should not be unique with duplicate keys");

    // Should contain the key
    assertTrue(table.containsKey(key1), tableName + " should contain the duplicated key");

    // Lookup should return a list of rows
    Object result = table.lookup(key1);
    assertNotNull(result, tableName + " lookup should not return null for duplicate keys");
    assertTrue(result instanceof List, tableName + " lookup should return List for duplicate keys");

    @SuppressWarnings("unchecked")
    List<Object[]> resultList = (List<Object[]>) result;
    assertEquals(resultList.size(), 3, tableName + " should return all three rows for duplicate key");

    // Should contain all three rows in order
    assertEquals(resultList.get(0), row1, tableName + " should return first row at index 0");
    assertEquals(resultList.get(1), row2, tableName + " should return second row at index 1");
    assertEquals(resultList.get(2), row3, tableName + " should return third row at index 2");

    // EntrySet should have one entry (the key) with list value
    Set<Map.Entry<Object, Object>> entries = table.entrySet();
    assertEquals(entries.size(), 1, tableName + " entrySet should have one entry for duplicate keys");
  }

  @Test(dataProvider = "lookupTableProvider")
  public void testMixedUniqueAndDuplicateKeys(LookupTable table, String tableName) {
    Object[] row1 = {"value1", 100, 1.5};
    Object[] row2 = {"value2", 200, 2.5};
    Object[] row3 = {"value3", 300, 3.5};
    Object[] row4 = {"value4", 400, 4.5};

    Object key1 = getTestKey(table, 1);
    Object key2 = getTestKey(table, 2);

    table.addRow(key1, row1);
    table.addRow(key2, row2);    // Unique key
    table.addRow(key1, row3);    // Duplicate of key1
    table.addRow(key1, row4);    // Another duplicate of key1
    table.finish();

    // Should not be unique
    assertFalse(table.isKeysUnique(), tableName + " should not be unique with mixed keys");

    // Should contain both keys
    assertTrue(table.containsKey(key1), tableName + " should contain duplicated key");
    assertTrue(table.containsKey(key2), tableName + " should contain unique key");

    // Key1 should return a list
    Object result1 = table.lookup(key1);
    assertTrue(result1 instanceof List, tableName + " should return List for duplicate key");
    @SuppressWarnings("unchecked")
    List<Object[]> resultList1 = (List<Object[]>) result1;
    assertEquals(resultList1.size(), 3, tableName + " should return three rows for duplicate key");

    // Key2 should return a single-element list (due to finish() converting single rows to lists when not unique)
    Object result2 = table.lookup(key2);
    assertTrue(result2 instanceof List, tableName + " should return List for unique key when table has duplicates");
    @SuppressWarnings("unchecked")
    List<Object[]> resultList2 = (List<Object[]>) result2;
    assertEquals(resultList2.size(), 1, tableName + " should return one row for unique key");
    assertEquals(resultList2.get(0), row2, tableName + " should return correct row for unique key");

    // EntrySet should have two entries
    assertEquals(table.entrySet().size(), 2, tableName + " entrySet should have two entries");
  }

  @Test(dataProvider = "lookupTableProvider")
  public void testNullKeyHandling(LookupTable table, String tableName) {
    Object[] row1 = {"value1", 100, 1.5};
    Object[] row2 = {"value2", 200, 2.5};

    Object key1 = getTestKey(table, 1);

    // Add normal row
    table.addRow(key1, row1);

    // Add row with null key - should be ignored
    table.addRow(null, row2);

    table.finish();

    // Should remain unique since null key is ignored
    assertTrue(table.isKeysUnique(), tableName + " should remain unique when null keys are ignored");

    // Should contain the non-null key
    assertTrue(table.containsKey(key1), tableName + " should contain non-null key");

    // Should not contain null key
    assertFalse(table.containsKey(null), tableName + " should not contain null key");

    // Lookup with null should return null
    assertNull(table.lookup(null), tableName + " lookup with null key should return null");

    // Normal lookup should work
    assertEquals(table.lookup(key1), row1, tableName + " should return correct row for non-null key");

    // EntrySet should have only one entry (null key ignored)
    assertEquals(table.entrySet().size(), 1, tableName + " entrySet should have one entry (null ignored)");
  }

  @Test(dataProvider = "lookupTableProvider")
  public void testFinishBehaviorWithUniqueKeys(LookupTable table, String tableName) {
    Object[] row1 = {"value1", 100, 1.5};
    Object[] row2 = {"value2", 200, 2.5};

    Object key1 = getTestKey(table, 1);
    Object key2 = getTestKey(table, 2);

    table.addRow(key1, row1);
    table.addRow(key2, row2);

    // Before finish, should be unique
    assertTrue(table.isKeysUnique(), tableName + " should be unique before finish");

    table.finish();

    // After finish with unique keys, should still be unique and return Object[] directly
    assertTrue(table.isKeysUnique(), tableName + " should remain unique after finish");

    Object result1 = table.lookup(key1);
    Object result2 = table.lookup(key2);

    assertTrue(result1 instanceof Object[], tableName + " should return Object[] for unique keys after finish");
    assertTrue(result2 instanceof Object[], tableName + " should return Object[] for unique keys after finish");

    assertEquals(result1, row1, tableName + " should return correct row after finish");
    assertEquals(result2, row2, tableName + " should return correct row after finish");
  }

  @Test(dataProvider = "lookupTableProvider")
  public void testFinishBehaviorWithDuplicateKeys(LookupTable table, String tableName) {
    Object[] row1 = {"value1", 100, 1.5};
    Object[] row2 = {"value2", 200, 2.5};
    Object[] row3 = {"value3", 300, 3.5};

    Object key1 = getTestKey(table, 1);
    Object key2 = getTestKey(table, 2);

    table.addRow(key1, row1);
    table.addRow(key2, row2);
    table.addRow(key1, row3);  // Create duplicate

    // Before finish, should not be unique
    assertFalse(table.isKeysUnique(), tableName + " should not be unique before finish with duplicates");

    table.finish();

    // After finish, should still not be unique
    assertFalse(table.isKeysUnique(), tableName + " should remain non-unique after finish");

    // All lookups should return Lists after finish with duplicates
    Object result1 = table.lookup(key1);
    Object result2 = table.lookup(key2);

    assertTrue(result1 instanceof List, tableName + " should return List for duplicate key after finish");
    assertTrue(result2 instanceof List,
        tableName + " should return List for unique key after finish when table has duplicates");

    @SuppressWarnings("unchecked")
    List<Object[]> resultList1 = (List<Object[]>) result1;
    @SuppressWarnings("unchecked")
    List<Object[]> resultList2 = (List<Object[]>) result2;

    assertEquals(resultList1.size(), 2, tableName + " should return two rows for duplicate key");
    assertEquals(resultList2.size(), 1, tableName + " should return one row for unique key");
  }

  @Test
  public void testObjectLookupTableWithComplexKeys() {
    ObjectLookupTable table = new ObjectLookupTable();

    // Test with composite keys (Key objects)
    Key compositeKey1 = new Key(new Object[]{"string1", 100});
    Key compositeKey2 = new Key(new Object[]{"string2", 200});
    Key compositeKey3 = new Key(new Object[]{"string1", 100}); // Same as compositeKey1

    Object[] row1 = {"value1", 1.0};
    Object[] row2 = {"value2", 2.0};
    Object[] row3 = {"value3", 3.0};

    table.addRow(compositeKey1, row1);
    table.addRow(compositeKey2, row2);
    table.addRow(compositeKey3, row3); // Should be treated as duplicate of compositeKey1

    table.finish();

    // Should not be unique due to duplicate composite keys
    assertFalse(table.isKeysUnique(), "ObjectLookupTable should not be unique with duplicate composite keys");

    // Should contain the composite keys
    assertTrue(table.containsKey(compositeKey1), "Should contain first composite key");
    assertTrue(table.containsKey(compositeKey2), "Should contain second composite key");
    assertTrue(table.containsKey(compositeKey3), "Should contain third composite key (same as first)");

    // Lookup for duplicate key should return list
    Object result1 = table.lookup(compositeKey1);
    assertTrue(result1 instanceof List, "Should return List for duplicate composite key");
    @SuppressWarnings("unchecked")
    List<Object[]> resultList1 = (List<Object[]>) result1;
    assertEquals(resultList1.size(), 2, "Should return two rows for duplicate composite key");

    // Lookup for unique key should return single-element list
    Object result2 = table.lookup(compositeKey2);
    assertTrue(result2 instanceof List, "Should return List for unique composite key when table has duplicates");
    @SuppressWarnings("unchecked")
    List<Object[]> resultList2 = (List<Object[]>) result2;
    assertEquals(resultList2.size(), 1, "Should return one row for unique composite key");
  }

  @Test
  public void testObjectLookupTableWithStringKeys() {
    ObjectLookupTable table = new ObjectLookupTable();

    Object[] row1 = {"value1", 100};
    Object[] row2 = {"value2", 200};
    Object[] row3 = {"value3", 300};

    table.addRow("key1", row1);
    table.addRow("key2", row2);
    table.addRow("key1", row3); // Duplicate string key

    table.finish();

    assertFalse(table.isKeysUnique(), "ObjectLookupTable should not be unique with duplicate string keys");

    assertTrue(table.containsKey("key1"), "Should contain key1");
    assertTrue(table.containsKey("key2"), "Should contain key2");

    Object result1 = table.lookup("key1");
    assertTrue(result1 instanceof List, "Should return List for duplicate string key");
    @SuppressWarnings("unchecked")
    List<Object[]> resultList1 = (List<Object[]>) result1;
    assertEquals(resultList1.size(), 2, "Should return two rows for duplicate string key");
  }

  @Test
  public void testIntLookupTableSpecifics() {
    IntLookupTable table = new IntLookupTable();

    Object[] row1 = {"value1", 100};
    Object[] row2 = {"value2", 200};

    table.addRow(42, row1);
    table.addRow(84, row2);
    table.finish();

    assertTrue(table.isKeysUnique(), "IntLookupTable should be unique");
    assertEquals(table.size(), 2, "IntLookupTable size should be 2");

    assertTrue(table.containsKey(42), "Should contain int key 42");
    assertTrue(table.containsKey(84), "Should contain int key 84");
    assertFalse(table.containsKey(99), "Should not contain int key 99");

    assertEquals(table.lookup(42), row1, "Should return correct row for int key 42");
    assertEquals(table.lookup(84), row2, "Should return correct row for int key 84");
    assertNull(table.lookup(99), "Should return null for non-existent int key");
  }

  @Test
  public void testLongLookupTableSpecifics() {
    LongLookupTable table = new LongLookupTable();

    Object[] row1 = {"value1", 100};
    Object[] row2 = {"value2", 200};

    table.addRow(42L, row1);
    table.addRow(84L, row2);
    table.finish();

    assertTrue(table.isKeysUnique(), "LongLookupTable should be unique");
    assertEquals(table.size(), 2, "LongLookupTable size should be 2");

    assertTrue(table.containsKey(42L), "Should contain long key 42L");
    assertTrue(table.containsKey(84L), "Should contain long key 84L");
    assertFalse(table.containsKey(99L), "Should not contain long key 99L");
  }

  @Test
  public void testFloatLookupTableSpecifics() {
    FloatLookupTable table = new FloatLookupTable();

    Object[] row1 = {"value1", 100};
    Object[] row2 = {"value2", 200};

    table.addRow(42.5f, row1);
    table.addRow(84.7f, row2);
    table.finish();

    assertTrue(table.isKeysUnique(), "FloatLookupTable should be unique");
    assertEquals(table.size(), 2, "FloatLookupTable size should be 2");

    assertTrue(table.containsKey(42.5f), "Should contain float key 42.5f");
    assertTrue(table.containsKey(84.7f), "Should contain float key 84.7f");
    assertFalse(table.containsKey(99.9f), "Should not contain float key 99.9f");
  }

  @Test
  public void testDoubleLookupTableSpecifics() {
    DoubleLookupTable table = new DoubleLookupTable();

    Object[] row1 = {"value1", 100};
    Object[] row2 = {"value2", 200};

    table.addRow(42.5, row1);
    table.addRow(84.7, row2);
    table.finish();

    assertTrue(table.isKeysUnique(), "DoubleLookupTable should be unique");
    assertEquals(table.size(), 2, "DoubleLookupTable size should be 2");

    assertTrue(table.containsKey(42.5), "Should contain double key 42.5");
    assertTrue(table.containsKey(84.7), "Should contain double key 84.7");
    assertFalse(table.containsKey(99.9), "Should not contain double key 99.9");
  }

  @Test
  public void testObjectLookupTableSize() {
    ObjectLookupTable table = new ObjectLookupTable();

    Object[] row1 = {"value1", 100};
    Object[] row2 = {"value2", 200};

    table.addRow("key1", row1);
    table.addRow("key2", row2);
    table.finish();

    assertEquals(table.size(), 2, "ObjectLookupTable size should be 2");
  }

  /**
   * Helper method to get appropriate test keys for different lookup table types
   */
  private Object getTestKey(LookupTable table, int index) {
    if (table instanceof IntLookupTable) {
      return index;
    } else if (table instanceof LongLookupTable) {
      return (long) index;
    } else if (table instanceof FloatLookupTable) {
      return (float) index + 0.5f;
    } else if (table instanceof DoubleLookupTable) {
      return (double) index + 0.5;
    } else if (table instanceof ObjectLookupTable) {
      return "key" + index;
    } else {
      throw new IllegalArgumentException("Unknown lookup table type: " + table.getClass());
    }
  }
}
