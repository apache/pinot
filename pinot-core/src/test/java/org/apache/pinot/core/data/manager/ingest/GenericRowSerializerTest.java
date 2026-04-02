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
package org.apache.pinot.core.data.manager.ingest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link GenericRowSerializer} round-trip serialization.
 */
public class GenericRowSerializerTest {

  @Test
  public void testSerializeDeserializeSingleRow()
      throws IOException {
    GenericRow row = new GenericRow();
    row.putValue("name", "Alice");
    row.putValue("age", 30);
    row.putValue("score", 95.5);
    row.putValue("active", true);

    byte[] serialized = GenericRowSerializer.serialize(row);
    assertNotNull(serialized);
    assertTrue(serialized.length > 0);

    GenericRow deserialized = GenericRowSerializer.deserialize(serialized);
    assertEquals(deserialized.getValue("name"), "Alice");
    assertEquals(deserialized.getValue("age"), 30);
    assertEquals(deserialized.getValue("score"), 95.5);
    assertEquals(deserialized.getValue("active"), true);
  }

  @Test
  public void testSerializeDeserializeNullValues()
      throws IOException {
    GenericRow row = new GenericRow();
    row.putValue("name", "Bob");
    row.putDefaultNullValue("age", 0);

    byte[] serialized = GenericRowSerializer.serialize(row);
    GenericRow deserialized = GenericRowSerializer.deserialize(serialized);

    assertEquals(deserialized.getValue("name"), "Bob");
    assertEquals(deserialized.getValue("age"), 0);
    assertTrue(deserialized.isNullValue("age"));
  }

  @Test
  public void testSerializeDeserializeBinaryValue()
      throws IOException {
    GenericRow row = new GenericRow();
    byte[] binaryData = new byte[]{1, 2, 3, 4, 5};
    row.putValue("data", binaryData);

    byte[] serialized = GenericRowSerializer.serialize(row);
    GenericRow deserialized = GenericRowSerializer.deserialize(serialized);

    Object value = deserialized.getValue("data");
    assertNotNull(value);
    assertTrue(value instanceof byte[]);
    assertEquals((byte[]) value, binaryData);
  }

  @Test
  public void testSerializeDeserializeMultiValueField()
      throws IOException {
    GenericRow row = new GenericRow();
    Object[] tags = new Object[]{"a", "b", "c"};
    row.putValue("tags", tags);

    byte[] serialized = GenericRowSerializer.serialize(row);
    GenericRow deserialized = GenericRowSerializer.deserialize(serialized);

    Object value = deserialized.getValue("tags");
    assertNotNull(value);
    assertTrue(value instanceof Object[]);
    Object[] result = (Object[]) value;
    assertEquals(result.length, 3);
    assertEquals(result[0], "a");
    assertEquals(result[1], "b");
    assertEquals(result[2], "c");
  }

  @Test
  public void testSerializeDeserializeMultipleRows()
      throws IOException {
    GenericRow row1 = new GenericRow();
    row1.putValue("id", 1);
    row1.putValue("name", "Alice");

    GenericRow row2 = new GenericRow();
    row2.putValue("id", 2);
    row2.putValue("name", "Bob");

    GenericRow row3 = new GenericRow();
    row3.putValue("id", 3);
    row3.putDefaultNullValue("name", "");

    List<GenericRow> rows = Arrays.asList(row1, row2, row3);
    byte[] serialized = GenericRowSerializer.serializeRows(rows);
    assertNotNull(serialized);

    List<GenericRow> deserialized = GenericRowSerializer.deserializeRows(serialized);
    assertEquals(deserialized.size(), 3);
    assertEquals(deserialized.get(0).getValue("id"), 1);
    assertEquals(deserialized.get(0).getValue("name"), "Alice");
    assertEquals(deserialized.get(1).getValue("id"), 2);
    assertEquals(deserialized.get(1).getValue("name"), "Bob");
    assertEquals(deserialized.get(2).getValue("id"), 3);
    assertTrue(deserialized.get(2).isNullValue("name"));
  }

  @Test
  public void testSerializeDeserializeLongValue()
      throws IOException {
    GenericRow row = new GenericRow();
    row.putValue("bigId", 9999999999L);

    byte[] serialized = GenericRowSerializer.serialize(row);
    GenericRow deserialized = GenericRowSerializer.deserialize(serialized);

    assertEquals(deserialized.getValue("bigId"), 9999999999L);
  }

  @Test
  public void testSerializeDeserializeEmptyRow()
      throws IOException {
    GenericRow row = new GenericRow();
    byte[] serialized = GenericRowSerializer.serialize(row);
    GenericRow deserialized = GenericRowSerializer.deserialize(serialized);
    assertTrue(deserialized.getFieldToValueMap().isEmpty());
  }

  @Test
  public void testSerializeDeserializeNullFieldValue()
      throws IOException {
    GenericRow row = new GenericRow();
    row.putValue("field", null);

    byte[] serialized = GenericRowSerializer.serialize(row);
    GenericRow deserialized = GenericRowSerializer.deserialize(serialized);

    assertNull(deserialized.getValue("field"));
  }
}
