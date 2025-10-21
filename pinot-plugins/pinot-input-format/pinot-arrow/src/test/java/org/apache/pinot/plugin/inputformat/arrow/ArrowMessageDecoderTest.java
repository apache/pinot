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
package org.apache.pinot.plugin.inputformat.arrow;

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.plugin.inputformat.arrow.util.ArrowTestDataUtil;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ArrowMessageDecoderTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowMessageDecoderTest.class);

  @Test
  public void testArrowMessageDecoderWithDifferentAllocatorLimits()
      throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();

    // Test with custom allocator limit
    Map<String, String> props = new HashMap<>();
    props.put(ArrowMessageDecoder.ARROW_ALLOCATOR_LIMIT, "67108864"); // 64MB

    Set<String> fieldsToRead = Sets.newHashSet("field1");
    String topicName = "test-topic-custom";

    decoder.init(props, fieldsToRead, topicName);
    decoder.close();

    // Test with default allocator limit
    ArrowMessageDecoder decoder2 = new ArrowMessageDecoder();
    Map<String, String> props2 = new HashMap<>(); // No allocator limit set

    decoder2.init(props2, fieldsToRead, topicName);
    decoder2.close();
  }

  @Test
  public void testArrowMessageDecoderMultipleInits()
      throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();

    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = Sets.newHashSet("id");
    String topicName = "test-multiple-init";

    // Test multiple initializations (should work without issues)
    decoder.init(props, fieldsToRead, topicName);
    decoder.init(props, fieldsToRead, topicName);

    decoder.close();
  }

  @Test
  public void testArrowMessageDecodingWithInvalidData()
      throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();

    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = Sets.newHashSet("id", "name", "age");
    String topicName = "test-arrow-topic";

    decoder.init(props, fieldsToRead, topicName);

    // Test various invalid data scenarios
    byte[] invalidData1 = "invalid arrow data".getBytes();
    byte[] invalidData2 = new byte[]{1, 2, 3, 4, 5};
    byte[] emptyData = new byte[0];

    GenericRow destination = new GenericRow();

    // Should return null for all invalid data types and null
    assertNull(decoder.decode(null, destination));
    assertNull(decoder.decode(invalidData1, destination));
    assertNull(decoder.decode(invalidData2, destination));
    assertNull(decoder.decode(emptyData, destination));

    // Test with null destination
    assertNull(decoder.decode(invalidData1, null));

    // Clean up
    decoder.close();
  }

  @Test
  public void testArrowMessageDecoderCloseMultipleTimes()
      throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();

    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = Sets.newHashSet("id");
    String topicName = "test-multiple-close";

    decoder.init(props, fieldsToRead, topicName);

    // Close multiple times should not cause issues
    decoder.close();
    decoder.close();
    decoder.close();
  }

  @Test
  public void testArrowMessageDecoderWithArrowDataAndDestination()
      throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();

    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = Sets.newHashSet("id", "name");
    String topicName = "test-real-arrow-with-destination";

    decoder.init(props, fieldsToRead, topicName);

    // Create real Arrow IPC data
    byte[] realArrowData = ArrowTestDataUtil.createValidArrowIpcData(1);

    // Test with provided destination containing existing data
    GenericRow destination = new GenericRow();
    destination.putValue("existing_field", "existing_value");

    GenericRow result = decoder.decode(realArrowData, destination);

    // Should return the same destination object (testing ArrowToGenericRowConverter destination
    // handling)
    assertSame(destination, result);

    // Should preserve existing data
    assertEquals("existing_value", result.getValue("existing_field"));

    // Should contain new converted Arrow data
    @SuppressWarnings("unchecked")
    List<GenericRow> rows = (List<GenericRow>) result.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    assertNotNull(rows);
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getValue("id"));
    assertEquals("name_1", rows.get(0).getValue("name"));

    decoder.close();
  }

  @Test
  public void testArrowMessageDecoderWithEmptyData()
      throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();

    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = Sets.newHashSet("id", "name");
    String topicName = "test-empty-arrow-data";

    decoder.init(props, fieldsToRead, topicName);

    // Test with empty Arrow data (zero batches)
    byte[] emptyArrowData = ArrowTestDataUtil.createEmptyArrowIpcData();
    GenericRow result = decoder.decode(emptyArrowData, null);

    // Should handle empty data gracefully - might return null or empty result
    // This tests the edge case of zero batches
    if (result != null) {
      @SuppressWarnings("unchecked")
      List<GenericRow> rows = (List<GenericRow>) result.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
      if (rows != null) {
        assertEquals(0, rows.size());
      }
    }

    decoder.close();
  }

  @Test
  public void testArrowMessageDecoderWithMultipleDataTypes()
      throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();

    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = Sets.newHashSet("id", "name", "price", "active", "timestamp");
    String topicName = "test-multi-type-arrow-data";

    decoder.init(props, fieldsToRead, topicName);

    // Create Arrow data with multiple data types
    byte[] multiTypeArrowData = ArrowTestDataUtil.createMultiTypeArrowIpcData(3);
    GenericRow result = decoder.decode(multiTypeArrowData, null);

    assertNotNull(result);
    @SuppressWarnings("unchecked")
    List<GenericRow> rows = (List<GenericRow>) result.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    assertNotNull(rows);
    assertEquals(3, rows.size());

    // Verify different data types are correctly handled
    GenericRow row0 = rows.get(0);
    assertEquals(1, row0.getValue("id"));
    assertEquals("product_1", row0.getValue("name").toString());
    assertEquals(10.99, (Double) row0.getValue("price"), 0.01);
    assertEquals(true, row0.getValue("active")); // BitVector returns boolean
    assertNotNull(row0.getValue("timestamp")); // Timestamp should be present

    GenericRow row1 = rows.get(1);
    assertEquals(2, row1.getValue("id"));
    assertEquals("product_2", row1.getValue("name").toString());
    assertEquals(15.99, (Double) row1.getValue("price"), 0.01);
    assertEquals(false, row1.getValue("active"));

    decoder.close();
  }

  @Test
  public void testArrowMessageDecoderWithBatchContainingMultipleRows()
      throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();

    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = Sets.newHashSet("id", "batch_num", "value");
    String topicName = "test-multi-batch-arrow-data";

    decoder.init(props, fieldsToRead, topicName);

    // Create Arrow data with multiple batches - but note: ArrowMessageDecoder processes one batch
    // per decode() call
    // So we test with a single batch containing multiple rows instead
    byte[] multiBatchArrowData =
        ArrowTestDataUtil.createMultiBatchArrowIpcData(1, 3); // 1 batch, 3 rows
    GenericRow result = decoder.decode(multiBatchArrowData, null);

    assertNotNull(result);
    @SuppressWarnings("unchecked")
    List<GenericRow> rows = (List<GenericRow>) result.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    assertNotNull(rows);
    assertEquals(3, rows.size()); // 1 batch × 3 rows = 3 total rows

    // Verify data from the batch
    GenericRow row0 = rows.get(0);
    assertEquals(1, row0.getValue("id"));
    assertEquals(0, row0.getValue("batch_num"));
    assertEquals("batch_0_row_0", row0.getValue("value").toString());

    GenericRow row1 = rows.get(1);
    assertEquals(2, row1.getValue("id"));
    assertEquals(0, row1.getValue("batch_num"));
    assertEquals("batch_0_row_1", row1.getValue("value").toString());

    GenericRow row2 = rows.get(2);
    assertEquals(3, row2.getValue("id"));
    assertEquals(0, row2.getValue("batch_num"));
    assertEquals("batch_0_row_2", row2.getValue("value").toString());

    decoder.close();
  }

  @Test
  public void testArrowMessageDecoderWithDictionaryEncodedData()
      throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();

    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = Sets.newHashSet("id", "category", "price");
    String topicName = "test-dictionary-encoded-arrow-data";

    decoder.init(props, fieldsToRead, topicName);

    // Create Arrow data with real dictionary encoding
    byte[] dictionaryArrowData = ArrowTestDataUtil.createDictionaryEncodedArrowIpcData(8);
    GenericRow result = decoder.decode(dictionaryArrowData, null);

    assertNotNull(result);
    @SuppressWarnings("unchecked")
    List<GenericRow> rows = (List<GenericRow>) result.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    assertNotNull(rows);
    assertEquals(8, rows.size());

    // Verify dictionary-encoded values are properly decoded by ArrowToGenericRowConverter
    // Dictionary: id=1 -> "Electronics", id=2 -> "Books", id=3 -> "Clothing", id=4 -> "Home"
    // Data cycles through indices 0,1,2,3,0,1,2,3 which should be resolved to string values

    GenericRow row0 = rows.get(0);
    assertEquals(1, row0.getValue("id"));
    assertEquals("Electronics", row0.getValue("category"));
    assertEquals(19.99, (Double) row0.getValue("price"), 0.01);

    GenericRow row1 = rows.get(1);
    assertEquals(2, row1.getValue("id"));
    assertEquals("Books", row1.getValue("category"));
    assertEquals(29.99, (Double) row1.getValue("price"), 0.01);

    GenericRow row2 = rows.get(2);
    assertEquals(3, row2.getValue("id"));
    assertEquals("Clothing", row2.getValue("category"));
    assertEquals(39.99, (Double) row2.getValue("price"), 0.01);

    GenericRow row3 = rows.get(3);
    assertEquals(4, row3.getValue("id"));
    assertEquals("Home", row3.getValue("category"));
    assertEquals(49.99, (Double) row3.getValue("price"), 0.01);

    // Verify cycling continues - row 4 should have same category as row 0
    GenericRow row4 = rows.get(4);
    assertEquals(5, row4.getValue("id"));
    assertEquals("Electronics", row4.getValue("category"));
    assertEquals(59.99, (Double) row4.getValue("price"), 0.01);

    decoder.close();
  }

  @Test
  public void testArrowDataTypeCompatibility()
      throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();

    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = Sets.newHashSet("id", "name", "price", "active", "timestamp");
    String topicName = "test-data-type-compatibility";

    decoder.init(props, fieldsToRead, topicName);

    // Create Arrow data with multiple data types to verify compatibility
    byte[] multiTypeArrowData = ArrowTestDataUtil.createMultiTypeArrowIpcData(3);
    GenericRow result = decoder.decode(multiTypeArrowData, null);

    assertNotNull(result);
    @SuppressWarnings("unchecked")
    List<GenericRow> rows = (List<GenericRow>) result.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    assertNotNull(rows);
    assertEquals(3, rows.size());

    // Check the actual data types returned by Arrow and verify Pinot compatibility
    GenericRow row0 = rows.get(0);

    // Verify each field's type and compatibility
    Object idValue = row0.getValue("id");
    assertNotNull(idValue, "ID should not be null");
    assertTrue(idValue instanceof Integer, "ID should be Integer compatible");

    Object nameValue = row0.getValue("name");
    assertNotNull(nameValue, "Name should not be null");
    // After conversion, Arrow Text should be converted to String for Pinot compatibility
    assertTrue(nameValue instanceof String, "Name should be String after conversion");
    assertEquals("product_1", nameValue);
    LOGGER.info("Arrow name field successfully converted to String: {}", nameValue);

    Object priceValue = row0.getValue("price");
    assertNotNull(priceValue, "Price should not be null");
    assertTrue(priceValue instanceof Double, "Price should be Double compatible");

    Object activeValue = row0.getValue("active");
    assertNotNull(activeValue, "Active should not be null");
    // BitVector.getObject() returns Boolean
    assertTrue(activeValue instanceof Boolean, "Active should be Boolean compatible");

    Object timestampValue = row0.getValue("timestamp");
    assertNotNull(timestampValue, "Timestamp should not be null");
    // After conversion, Arrow LocalDateTime should be converted to java.sql.Timestamp for Pinot
    // compatibility
    assertTrue(
        timestampValue instanceof java.sql.Timestamp,
        "Timestamp should be java.sql.Timestamp after conversion");
    java.sql.Timestamp ts = (java.sql.Timestamp) timestampValue;
    assertTrue(ts.getTime() > 0, "Timestamp should be a positive value");
    LOGGER.info(
        "Arrow timestamp field successfully converted to java.sql.Timestamp: {}", timestampValue);

    decoder.close();
  }

  @Test
  public void testArrowMessageDecoderWithListVectors()
      throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();

    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = Sets.newHashSet("id", "numbers", "tags");
    String topicName = "test-list-vectors";

    decoder.init(props, fieldsToRead, topicName);

    // Create Arrow data with List vectors
    byte[] listArrowData = ArrowTestDataUtil.createListArrowIpcData(3);
    GenericRow result = decoder.decode(listArrowData, null);

    assertNotNull(result);
    @SuppressWarnings("unchecked")
    List<GenericRow> rows = (List<GenericRow>) result.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    assertNotNull(rows);
    assertEquals(3, rows.size());

    // Verify first row - should have 1 number and 2 tags
    GenericRow row0 = rows.get(0);
    assertEquals(1, row0.getValue("id"));
    Object numbersValue0 = row0.getValue("numbers");
    assertNotNull(numbersValue0, "Numbers should not be null");
    assertTrue(numbersValue0 instanceof List);
    @SuppressWarnings("unchecked")
    List<Object> numbersList0 = (List<Object>) numbersValue0;
    assertEquals(1, numbersList0.size());
    assertEquals(10, numbersList0.get(0));

    Object tagsValue0 = row0.getValue("tags");
    assertNotNull(tagsValue0, "Tags should not be null");
    assertTrue(tagsValue0 instanceof List);
    @SuppressWarnings("unchecked")
    List<Object> tagsList0 = (List<Object>) tagsValue0;
    assertEquals(2, tagsList0.size());
    assertEquals("tag_0_0", tagsList0.get(0).toString());
    assertEquals("tag_0_1", tagsList0.get(1).toString());

    // Verify second row - should have 2 numbers and 2 tags
    GenericRow row1 = rows.get(1);
    assertEquals(2, row1.getValue("id"));
    Object numbersValue1 = row1.getValue("numbers");
    assertNotNull(numbersValue1);
    @SuppressWarnings("unchecked")
    List<Object> numbersList1 = (List<Object>) numbersValue1;
    assertEquals(2, numbersList1.size());
    assertEquals(20, numbersList1.get(0));
    assertEquals(21, numbersList1.get(1));

    // Verify third row - should have 3 numbers
    GenericRow row2 = rows.get(2);
    assertEquals(3, row2.getValue("id"));
    Object numbersValue2 = row2.getValue("numbers");
    @SuppressWarnings("unchecked")
    List<Object> numbersList2 = (List<Object>) numbersValue2;
    assertEquals(3, numbersList2.size());
    assertEquals(30, numbersList2.get(0));
    assertEquals(31, numbersList2.get(1));
    assertEquals(32, numbersList2.get(2));

    LOGGER.info("List vector test completed successfully with {} rows", rows.size());
    decoder.close();
  }

  @Test
  public void testArrowMessageDecoderWithStructVectors()
      throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();

    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = Sets.newHashSet("id", "person");
    String topicName = "test-struct-vectors";

    decoder.init(props, fieldsToRead, topicName);

    // Create Arrow data with Struct vectors
    byte[] structArrowData = ArrowTestDataUtil.createStructArrowIpcData(2);
    GenericRow result = decoder.decode(structArrowData, null);

    assertNotNull(result);
    @SuppressWarnings("unchecked")
    List<GenericRow> rows = (List<GenericRow>) result.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    assertNotNull(rows);
    assertEquals(2, rows.size());

    // Verify first row with nested struct
    GenericRow row0 = rows.get(0);
    assertEquals(1, row0.getValue("id"));
    Object personValue0 = row0.getValue("person");
    assertNotNull(personValue0);
    assertTrue(personValue0 instanceof Map);
    @SuppressWarnings("unchecked")
    Map<String, Object> personMap0 = (Map<String, Object>) personValue0;
    assertEquals("Person_1", personMap0.get("name").toString());
    assertEquals(25, personMap0.get("age"));
    @SuppressWarnings("unchecked")
    Map<String, Object> address0 = (Map<String, Object>) personMap0.get("address");
    assertEquals("1 Main St", address0.get("street").toString());
    assertEquals("City_1", address0.get("city").toString());

    // Verify second row
    GenericRow row1 = rows.get(1);
    assertEquals(2, row1.getValue("id"));
    Object personValue1 = row1.getValue("person");
    assertNotNull(personValue1);
    assertTrue(personValue1 instanceof Map);
    @SuppressWarnings("unchecked")
    Map<String, Object> personMap1 = (Map<String, Object>) personValue1;
    assertEquals("Person_2", personMap1.get("name").toString());
    assertEquals(26, personMap1.get("age"));
    @SuppressWarnings("unchecked")
    Map<String, Object> address1 = (Map<String, Object>) personMap1.get("address");
    assertEquals("2 Main St", address1.get("street").toString());
    assertEquals("City_2", address1.get("city").toString());

    LOGGER.info("Struct vector test completed successfully with {} rows", rows.size());
    decoder.close();
  }

  @Test
  public void testArrowMessageDecoderWithMapVectors()
      throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();

    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = Sets.newHashSet("id", "metadata");
    String topicName = "test-map-vectors";

    decoder.init(props, fieldsToRead, topicName);

    // Create Arrow data with Map vectors
    byte[] mapArrowData = ArrowTestDataUtil.createMapArrowIpcData(2);
    GenericRow result = decoder.decode(mapArrowData, null);

    assertNotNull(result);
    @SuppressWarnings("unchecked")
    List<GenericRow> rows = (List<GenericRow>) result.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    assertNotNull(rows);
    assertEquals(2, rows.size());

    // Verify first row with map data
    GenericRow row0 = rows.get(0);
    assertEquals(1, row0.getValue("id"));
    Object metadataValue0 = row0.getValue("metadata");
    assertNotNull(metadataValue0);
    assertTrue(metadataValue0 instanceof Map);
    @SuppressWarnings("unchecked")
    Map<String, Object> meta0 = (Map<String, Object>) metadataValue0;
    assertTrue(meta0.values().contains(100));
    assertTrue(meta0.values().contains(101));

    // Verify second row - should have 3 entries (2 + (1%2) = 3)
    GenericRow row1 = rows.get(1);
    assertEquals(2, row1.getValue("id"));
    Object metadataValue1 = row1.getValue("metadata");
    assertNotNull(metadataValue1);
    assertTrue(metadataValue1 instanceof Map);
    @SuppressWarnings("unchecked")
    Map<String, Object> meta1 = (Map<String, Object>) metadataValue1;
    assertTrue(meta1.values().contains(200));
    assertTrue(meta1.values().contains(201));
    assertTrue(meta1.values().contains(202));

    LOGGER.info("Map vector test completed successfully with {} rows", rows.size());
    decoder.close();
  }

  @Test
  public void testArrowMessageDecoderWithNestedMapValues()
      throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();

    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = Sets.newHashSet("id", "metadata");
    String topicName = "test-nested-map-values";

    decoder.init(props, fieldsToRead, topicName);

    // Create Arrow data with Map values that are themselves Maps
    byte[] nestedMapArrowData = ArrowTestDataUtil.createNestedMapArrowIpcData(2);
    GenericRow result = decoder.decode(nestedMapArrowData, null);

    assertNotNull(result);
    @SuppressWarnings("unchecked")
    List<GenericRow> rows = (List<GenericRow>) result.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    assertNotNull(rows);
    assertEquals(2, rows.size());

    // Verify first row: metadata is a Map<String, Map<String, Integer>>
    GenericRow row0 = rows.get(0);
    assertEquals(1, row0.getValue("id"));
    Object metadataValue0 = row0.getValue("metadata");
    assertNotNull(metadataValue0);
    assertTrue(metadataValue0 instanceof Map);
    @SuppressWarnings("unchecked")
    Map<String, Object> outer0 = (Map<String, Object>) metadataValue0;
    assertTrue(outer0.size() >= 2);
    for (Object innerMapObj : outer0.values()) {
      assertTrue(innerMapObj instanceof Map);
      @SuppressWarnings("unchecked")
      Map<String, Object> inner = (Map<String, Object>) innerMapObj;
      assertTrue(inner.size() >= 2);
      // Values should be integers from generator
      for (Object v : inner.values()) {
        assertTrue(v instanceof Integer);
      }
    }

    // Verify second row similarly
    GenericRow row1 = rows.get(1);
    assertEquals(2, row1.getValue("id"));
    Object metadataValue1 = row1.getValue("metadata");
    assertNotNull(metadataValue1);
    assertTrue(metadataValue1 instanceof Map);
    @SuppressWarnings("unchecked")
    Map<String, Object> outer1 = (Map<String, Object>) metadataValue1;
    assertTrue(outer1.size() >= 2);
    boolean sawThreeInner = false;
    for (Object innerMapObj : outer1.values()) {
      assertTrue(innerMapObj instanceof Map);
      @SuppressWarnings("unchecked")
      Map<String, Object> inner = (Map<String, Object>) innerMapObj;
      if (inner.size() == 3) {
        sawThreeInner = true;
      }
    }
    assertTrue(sawThreeInner);

    decoder.close();
  }

  @Test
  public void testArrowMessageDecoderWithNestedListStruct()
      throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();

    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = Sets.newHashSet("id", "items");
    String topicName = "test-nested-list-struct";

    decoder.init(props, fieldsToRead, topicName);

    // Create Arrow data with nested List of Structs
    byte[] nestedArrowData = ArrowTestDataUtil.createNestedListStructArrowIpcData(3);
    GenericRow result = decoder.decode(nestedArrowData, null);

    assertNotNull(result);
    @SuppressWarnings("unchecked")
    List<GenericRow> rows = (List<GenericRow>) result.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    assertNotNull(rows);
    assertEquals(3, rows.size());

    // Verify first row - should have 1 item (1 + (0%3) = 1)
    GenericRow row0 = rows.get(0);
    assertEquals(1, row0.getValue("id"));
    Object itemsValue0 = row0.getValue("items");
    assertNotNull(itemsValue0);
    assertTrue(itemsValue0 instanceof List);
    @SuppressWarnings("unchecked")
    List<Object> items0 = (List<Object>) itemsValue0;
    assertEquals(1, items0.size());
    @SuppressWarnings("unchecked")
    Map<String, Object> item00 = (Map<String, Object>) items0.get(0);
    assertEquals("item_0_0", item00.get("item_name").toString());
    assertEquals(10.99, (Double) item00.get("item_price"), 0.01);

    // Verify second row - should have 2 items (1 + (1%3) = 2)
    GenericRow row1 = rows.get(1);
    assertEquals(2, row1.getValue("id"));
    Object itemsValue1 = row1.getValue("items");
    assertNotNull(itemsValue1);
    @SuppressWarnings("unchecked")
    List<Object> items1 = (List<Object>) itemsValue1;
    assertEquals(2, items1.size());
    @SuppressWarnings("unchecked")
    Map<String, Object> item10 = (Map<String, Object>) items1.get(0);
    assertEquals("item_1_0", item10.get("item_name").toString());
    assertEquals(15.99, (Double) item10.get("item_price"), 0.01);
    @SuppressWarnings("unchecked")
    Map<String, Object> item11 = (Map<String, Object>) items1.get(1);
    assertEquals("item_1_1", item11.get("item_name").toString());
    assertEquals(16.99, (Double) item11.get("item_price"), 0.01);

    // Verify third row - should have 3 items (1 + (2%3) = 3)
    GenericRow row2 = rows.get(2);
    assertEquals(3, row2.getValue("id"));
    Object itemsValue2 = row2.getValue("items");
    assertNotNull(itemsValue2);
    @SuppressWarnings("unchecked")
    List<Object> items2 = (List<Object>) itemsValue2;
    assertEquals(3, items2.size());

    LOGGER.info("Nested List-Struct test completed successfully with {} rows", rows.size());
    decoder.close();
  }

  @Test
  public void testArrowNestedStructureCompatibilityWithPinot()
      throws Exception {
    ArrowMessageDecoder decoder = new ArrowMessageDecoder();

    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = Sets.newHashSet("id", "numbers", "person", "metadata", "items");
    String topicName = "test-nested-compatibility";

    decoder.init(props, fieldsToRead, topicName);

    // Test each nested structure type individually for compatibility
    // Test List compatibility
    byte[] listData = ArrowTestDataUtil.createListArrowIpcData(1);
    GenericRow listResult = decoder.decode(listData, null);
    assertNotNull(listResult, "List data should be decodable");

    // Test Struct compatibility
    byte[] structData = ArrowTestDataUtil.createStructArrowIpcData(1);
    GenericRow structResult = decoder.decode(structData, null);
    assertNotNull(structResult, "Struct data should be decodable");

    // Test Map compatibility
    byte[] mapData = ArrowTestDataUtil.createMapArrowIpcData(1);
    GenericRow mapResult = decoder.decode(mapData, null);
    assertNotNull(mapResult, "Map data should be decodable");

    // Test complex nested structures
    byte[] nestedData = ArrowTestDataUtil.createNestedListStructArrowIpcData(1);
    GenericRow nestedResult = decoder.decode(nestedData, null);
    assertNotNull(nestedResult, "Nested List-Struct data should be decodable");

    // Verify that all simulated nested structures produce valid GenericRow objects
    @SuppressWarnings("unchecked")
    List<GenericRow> listRows =
        (List<GenericRow>) listResult.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    assertNotNull(listRows, "List result should contain rows");
    assertTrue(listRows.size() > 0, "List result should have at least one row");

    // Verify nested list data is accessible
    GenericRow firstListRow = listRows.get(0);
    assertNotNull(firstListRow.getValue("numbers"), "List row should have numbers");

    @SuppressWarnings("unchecked")
    List<GenericRow> structRows =
        (List<GenericRow>) structResult.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    assertNotNull(structRows, "Struct result should contain rows");
    assertTrue(structRows.size() > 0, "Struct result should have at least one row");

    // Verify struct data is accessible
    GenericRow firstStructRow = structRows.get(0);
    assertNotNull(firstStructRow.getValue("person"), "Struct row should have person");

    @SuppressWarnings("unchecked")
    List<GenericRow> mapRows =
        (List<GenericRow>) mapResult.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    assertNotNull(mapRows, "Map result should contain rows");
    assertTrue(mapRows.size() > 0, "Map result should have at least one row");

    // Verify map data is accessible
    GenericRow firstMapRow = mapRows.get(0);
    assertNotNull(firstMapRow.getValue("metadata"), "Map row should have metadata");

    @SuppressWarnings("unchecked")
    List<GenericRow> nestedRows =
        (List<GenericRow>) nestedResult.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    assertNotNull(nestedRows, "Nested result should contain rows");
    assertTrue(nestedRows.size() > 0, "Nested result should have at least one row");

    // Verify nested list-struct data is accessible
    GenericRow firstNestedRow = nestedRows.get(0);
    assertNotNull(firstNestedRow.getValue("items"), "Nested row should have items");

    LOGGER.info(
        "All nested structure types are compatible with ArrowMessageDecoder and produce valid GenericRow objects");
    decoder.close();
  }
}
