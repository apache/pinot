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
package org.apache.pinot.plugin.inputformat.protobuf;

import com.google.protobuf.Message;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pinot.plugin.inputformat.protobuf.ProtoBufTestDataGenerator.*;
import static org.testng.Assert.*;


/**
 * Comprehensive tests for {@link ProtoBufRecordExtractor} field descriptor caching and optimization behavior.
 *
 * <p>These tests verify:
 * <ul>
 *   <li>Field descriptor caching works correctly across multiple extractions</li>
 *   <li>Subset field extraction works correctly</li>
 *   <li>Schema change detection and cache invalidation</li>
 *   <li>Re-initialization behavior</li>
 *   <li>Edge cases like missing fields, empty fields set</li>
 * </ul>
 */
public class ProtoBufRecordExtractorCachingTest {

  private ProtoBufRecordExtractor _extractor;
  private GenericRow _reusableRow;

  @BeforeMethod
  public void setUp() {
    _extractor = new ProtoBufRecordExtractor();
    _reusableRow = new GenericRow();
  }

  // ==================== CACHING BEHAVIOR TESTS ====================

  @Test
  public void testCachingWithMultipleExtractions()
      throws IOException {
    // Initialize extractor for all fields
    _extractor.init(null, null);

    // Create multiple messages and verify extraction is consistent
    Message message1 = createTestMessage("test1", 100);
    Message message2 = createTestMessage("test2", 200);
    Message message3 = createTestMessage("test3", 300);

    // First extraction initializes the cache
    GenericRow row1 = _extractor.extract(message1, new GenericRow());
    assertEquals(row1.getValue(STRING_FIELD), "test1");
    assertEquals(row1.getValue(INT_FIELD), 100);

    // Second extraction uses cached descriptors
    GenericRow row2 = _extractor.extract(message2, new GenericRow());
    assertEquals(row2.getValue(STRING_FIELD), "test2");
    assertEquals(row2.getValue(INT_FIELD), 200);

    // Third extraction uses cached descriptors
    GenericRow row3 = _extractor.extract(message3, new GenericRow());
    assertEquals(row3.getValue(STRING_FIELD), "test3");
    assertEquals(row3.getValue(INT_FIELD), 300);
  }

  @Test
  public void testCachingWithReusableGenericRow()
      throws IOException {
    _extractor.init(null, null);

    Message message1 = createTestMessage("reuse1", 111);
    Message message2 = createTestMessage("reuse2", 222);

    // Extract first message
    _reusableRow.clear();
    _extractor.extract(message1, _reusableRow);
    assertEquals(_reusableRow.getValue(STRING_FIELD), "reuse1");
    assertEquals(_reusableRow.getValue(INT_FIELD), 111);

    // Extract second message into same row (after clear)
    _reusableRow.clear();
    _extractor.extract(message2, _reusableRow);
    assertEquals(_reusableRow.getValue(STRING_FIELD), "reuse2");
    assertEquals(_reusableRow.getValue(INT_FIELD), 222);
  }

  // ==================== SUBSET FIELD EXTRACTION TESTS ====================

  @Test
  public void testSubsetFieldExtraction()
      throws IOException {
    Set<String> subsetFields = new HashSet<>(Arrays.asList(STRING_FIELD, INT_FIELD, LONG_FIELD));
    _extractor.init(subsetFields, null);

    Message message = createTestMessage("subset", 999);
    GenericRow row = _extractor.extract(message, new GenericRow());

    // Verify only requested fields are extracted
    assertEquals(row.getValue(STRING_FIELD), "subset");
    assertEquals(row.getValue(INT_FIELD), 999);
    assertNotNull(row.getValue(LONG_FIELD));

    // Verify the row contains exactly the requested fields
    assertEquals(row.getFieldToValueMap().size(), 3);
    assertTrue(row.getFieldToValueMap().containsKey(STRING_FIELD));
    assertTrue(row.getFieldToValueMap().containsKey(INT_FIELD));
    assertTrue(row.getFieldToValueMap().containsKey(LONG_FIELD));
  }

  @Test
  public void testSubsetFieldExtractionWithMultipleMessages()
      throws IOException {
    Set<String> subsetFields = new HashSet<>(Arrays.asList(STRING_FIELD, DOUBLE_FIELD));
    _extractor.init(subsetFields, null);

    // Multiple extractions with subset fields
    for (int i = 0; i < 10; i++) {
      Message message = createTestMessage("iter_" + i, i * 100);
      _reusableRow.clear();
      _extractor.extract(message, _reusableRow);

      assertEquals(_reusableRow.getValue(STRING_FIELD), "iter_" + i);
      assertNotNull(_reusableRow.getValue(DOUBLE_FIELD));
      assertEquals(_reusableRow.getFieldToValueMap().size(), 2);
    }
  }

  @Test
  public void testSubsetWithNonExistentField()
      throws IOException {
    // Include a field that doesn't exist in the schema
    Set<String> subsetFields = new HashSet<>(Arrays.asList(STRING_FIELD, "non_existent_field"));
    _extractor.init(subsetFields, null);

    Message message = createTestMessage("test", 100);
    GenericRow row = _extractor.extract(message, new GenericRow());

    // Existing field should be extracted
    assertEquals(row.getValue(STRING_FIELD), "test");
    // Non-existent field should be null
    assertNull(row.getValue("non_existent_field"));
  }

  // ==================== SCHEMA CHANGE DETECTION TESTS ====================

  @Test
  public void testSchemaChangeDetectionWithDifferentMessageTypes()
      throws IOException {
    _extractor.init(null, null);

    // Extract from ComplexTypes.TestMessage
    Message complexMessage = createTestMessage("complex", 100);
    GenericRow row1 = _extractor.extract(complexMessage, new GenericRow());
    assertEquals(row1.getValue(STRING_FIELD), "complex");

    // Extract from Sample.SampleRecord (different message type)
    Message sampleMessage = getSampleRecordMessage();
    GenericRow row2 = _extractor.extract(sampleMessage, new GenericRow());

    // Should have re-initialized cache for different message type
    assertEquals(row2.getValue("name"), "Alice");
    assertEquals(row2.getValue("email"), "foobar@hello.com");
    assertEquals(row2.getValue("id"), 18);
  }

  @Test
  public void testExtractorReinitialization()
      throws IOException {
    // First initialization with all fields
    _extractor.init(null, null);
    Message message1 = createTestMessage("init1", 100);
    GenericRow row1 = _extractor.extract(message1, new GenericRow());
    int allFieldsCount = row1.getFieldToValueMap().size();
    assertTrue(allFieldsCount > 5); // All fields from schema (27 total)

    // Re-initialize with subset fields
    Set<String> subsetFields = new HashSet<>(Arrays.asList(STRING_FIELD, INT_FIELD));
    _extractor.init(subsetFields, null);

    Message message2 = createTestMessage("init2", 200);
    GenericRow row2 = _extractor.extract(message2, new GenericRow());
    assertEquals(row2.getFieldToValueMap().size(), subsetFields.size()); // Only subset
    assertEquals(row2.getValue(STRING_FIELD), "init2");
    assertEquals(row2.getValue(INT_FIELD), 200);
    // Verify all fields count was greater than subset
    assertTrue(allFieldsCount > subsetFields.size());
  }

  // ==================== EDGE CASES ====================

  @Test
  public void testEmptyFieldsSet()
      throws IOException {
    // Empty set should behave like null (extract all)
    _extractor.init(new HashSet<>(), null);

    Message message = createTestMessage("empty_set", 100);
    GenericRow row = _extractor.extract(message, new GenericRow());

    // Should extract all fields when empty set provided
    assertTrue(row.getFieldToValueMap().size() > 5);
    assertEquals(row.getValue(STRING_FIELD), "empty_set");
  }

  @Test
  public void testNullFieldsSet()
      throws IOException {
    _extractor.init(null, null);

    Message message = createTestMessage("null_set", 100);
    GenericRow row = _extractor.extract(message, new GenericRow());

    // Should extract all fields
    assertTrue(row.getFieldToValueMap().size() > 5);
    assertEquals(row.getValue(STRING_FIELD), "null_set");
  }

  @Test
  public void testSingleFieldExtraction()
      throws IOException {
    Set<String> singleField = new HashSet<>(Arrays.asList(STRING_FIELD));
    _extractor.init(singleField, null);

    Message message = createTestMessage("single", 100);
    GenericRow row = _extractor.extract(message, new GenericRow());

    assertEquals(row.getFieldToValueMap().size(), 1);
    assertEquals(row.getValue(STRING_FIELD), "single");
  }

  @Test
  public void testAllFieldTypes()
      throws IOException {
    _extractor.init(null, null);

    Map<String, Object> record = createComplexTypeRecord();
    Message message = getComplexTypeObject(record);
    GenericRow row = _extractor.extract(message, new GenericRow());

    // Verify various field types
    assertEquals(row.getValue(STRING_FIELD), "hello");
    assertEquals(row.getValue(INT_FIELD), 10);
    assertEquals(row.getValue(LONG_FIELD), 100L);
    assertEquals(row.getValue(DOUBLE_FIELD), 1.1);
    assertEquals(row.getValue(FLOAT_FIELD), 2.2f);
    // Note: convertSingleValue converts booleans to strings via toString()
    assertEquals(row.getValue(BOOL_FIELD), "true");
    assertNotNull(row.getValue(BYTES_FIELD)); // ByteString converted to byte[]
    assertEquals(row.getValue(ENUM_FIELD), "GAMMA");

    // Verify repeated field
    Object repeatedStrings = row.getValue(REPEATED_STRINGS);
    assertNotNull(repeatedStrings);
    assertTrue(repeatedStrings instanceof Object[]);
    Object[] repeatedArray = (Object[]) repeatedStrings;
    assertEquals(repeatedArray.length, 3);
    assertEquals(repeatedArray[0], "aaa");

    // Verify nested message
    Object nestedMessage = row.getValue(NESTED_MESSAGE);
    assertNotNull(nestedMessage);
    assertTrue(nestedMessage instanceof Map);

    // Verify map fields
    Object simpleMap = row.getValue(SIMPLE_MAP);
    assertNotNull(simpleMap);
    assertTrue(simpleMap instanceof Map);

    Object complexMap = row.getValue(COMPLEX_MAP);
    assertNotNull(complexMap);
    assertTrue(complexMap instanceof Map);
  }

  @Test
  public void testConsistencyAcrossManyExtractions()
      throws IOException {
    _extractor.init(null, null);

    // Run many extractions to verify caching consistency
    for (int i = 0; i < 1000; i++) {
      Message message = createTestMessage("consistency_" + i, i);
      _reusableRow.clear();
      _extractor.extract(message, _reusableRow);

      assertEquals(_reusableRow.getValue(STRING_FIELD), "consistency_" + i);
      assertEquals(_reusableRow.getValue(INT_FIELD), i);
    }
  }

  @Test
  public void testSubsetFieldOrderIndependence()
      throws IOException {
    // Test that the order of fields in subset doesn't affect results
    Set<String> fields1 = new HashSet<>();
    fields1.add(STRING_FIELD);
    fields1.add(INT_FIELD);
    fields1.add(LONG_FIELD);

    Set<String> fields2 = new HashSet<>();
    fields2.add(LONG_FIELD);
    fields2.add(STRING_FIELD);
    fields2.add(INT_FIELD);

    ProtoBufRecordExtractor extractor1 = new ProtoBufRecordExtractor();
    extractor1.init(fields1, null);

    ProtoBufRecordExtractor extractor2 = new ProtoBufRecordExtractor();
    extractor2.init(fields2, null);

    Message message = createTestMessage("order_test", 999);

    GenericRow row1 = extractor1.extract(message, new GenericRow());
    GenericRow row2 = extractor2.extract(message, new GenericRow());

    // Both should have same values regardless of initialization order
    assertEquals(row1.getValue(STRING_FIELD), row2.getValue(STRING_FIELD));
    assertEquals(row1.getValue(INT_FIELD), row2.getValue(INT_FIELD));
    assertEquals(row1.getValue(LONG_FIELD), row2.getValue(LONG_FIELD));
    assertEquals(row1.getFieldToValueMap().size(), row2.getFieldToValueMap().size());
  }

  // ==================== HELPER METHODS ====================

  private Message createTestMessage(String stringValue, int intValue)
      throws IOException {
    Map<String, Object> record = new HashMap<>();
    record.put(STRING_FIELD, stringValue);
    record.put(INT_FIELD, intValue);
    record.put(LONG_FIELD, (long) intValue * 10);
    record.put(DOUBLE_FIELD, intValue * 1.1);
    record.put(FLOAT_FIELD, intValue * 0.5f);
    record.put(BOOL_FIELD, String.valueOf(intValue % 2 == 0));
    record.put(BYTES_FIELD, ("bytes_" + intValue).getBytes(UTF_8));
    record.put(REPEATED_STRINGS, Arrays.asList("a", "b", "c"));
    record.put(NESTED_MESSAGE, getNestedMap(NESTED_STRING_FIELD, "nested_" + intValue, NESTED_INT_FIELD, intValue));
    record.put(REPEATED_NESTED_MESSAGES, Arrays.asList(getNestedMap(NESTED_STRING_FIELD, "rn1", NESTED_INT_FIELD, 1),
        getNestedMap(NESTED_STRING_FIELD, "rn2", NESTED_INT_FIELD, 2)));
    record.put(COMPLEX_MAP, getNestedMap("key1", getNestedMap(NESTED_STRING_FIELD, "val1", NESTED_INT_FIELD, 1), "key2",
        getNestedMap(NESTED_STRING_FIELD, "val2", NESTED_INT_FIELD, 2)));
    record.put(SIMPLE_MAP, getNestedMap("k1", 1, "k2", 2));
    record.put(ENUM_FIELD, "ALPHA");
    return getComplexTypeObject(record);
  }
}
