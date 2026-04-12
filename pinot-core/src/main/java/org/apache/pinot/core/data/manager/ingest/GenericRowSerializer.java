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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Serializes and deserializes {@link GenericRow} instances to and from JSON byte arrays.
 *
 * <p>This implementation uses JSON for v1 simplicity. The format stores field values along with
 * null-value field metadata. Binary ({@code byte[]}) values are Base64-encoded.
 *
 * <p>This class is stateless and thread-safe. All methods are static.
 */
public final class GenericRowSerializer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String FIELDS_KEY = "fields";
  private static final String NULL_FIELDS_KEY = "nullFields";
  private static final String BINARY_PREFIX = "__b64:";

  private GenericRowSerializer() {
  }

  /**
   * Serializes a {@link GenericRow} to a JSON byte array.
   *
   * @param row the row to serialize
   * @return the serialized byte array
   * @throws IOException if serialization fails
   */
  public static byte[] serialize(GenericRow row)
      throws IOException {
    ObjectNode root = MAPPER.createObjectNode();
    ObjectNode fieldsNode = MAPPER.createObjectNode();

    for (Map.Entry<String, Object> entry : row.getFieldToValueMap().entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      fieldsNode.set(key, serializeValue(value));
    }
    root.set(FIELDS_KEY, fieldsNode);

    Set<String> nullFields = row.getNullValueFields();
    if (!nullFields.isEmpty()) {
      ArrayNode nullArray = MAPPER.createArrayNode();
      for (String field : nullFields) {
        nullArray.add(field);
      }
      root.set(NULL_FIELDS_KEY, nullArray);
    }

    return MAPPER.writeValueAsBytes(root);
  }

  /**
   * Deserializes a JSON byte array back to a {@link GenericRow}.
   *
   * @param data the serialized byte array
   * @return the deserialized row
   * @throws IOException if deserialization fails
   */
  public static GenericRow deserialize(byte[] data)
      throws IOException {
    JsonNode root = MAPPER.readTree(data);
    GenericRow row = new GenericRow();

    // Read null fields first so we can use putDefaultNullValue appropriately
    Set<String> nullFields = new HashSet<>();
    if (root.has(NULL_FIELDS_KEY)) {
      for (JsonNode fieldNode : root.get(NULL_FIELDS_KEY)) {
        nullFields.add(fieldNode.asText());
      }
    }

    JsonNode fieldsNode = root.get(FIELDS_KEY);
    if (fieldsNode != null) {
      Iterator<Map.Entry<String, JsonNode>> it = fieldsNode.fields();
      while (it.hasNext()) {
        Map.Entry<String, JsonNode> entry = it.next();
        String key = entry.getKey();
        Object value = deserializeValue(entry.getValue());
        if (nullFields.contains(key)) {
          row.putDefaultNullValue(key, value);
        } else {
          row.putValue(key, value);
        }
      }
    }

    return row;
  }

  /**
   * Serializes a list of {@link GenericRow} instances to a single byte array.
   *
   * @param rows the rows to serialize
   * @return the serialized byte array
   * @throws IOException if serialization fails
   */
  public static byte[] serializeRows(List<GenericRow> rows)
      throws IOException {
    ArrayNode arrayNode = MAPPER.createArrayNode();
    for (GenericRow row : rows) {
      ObjectNode root = MAPPER.createObjectNode();
      ObjectNode fieldsNode = MAPPER.createObjectNode();

      for (Map.Entry<String, Object> entry : row.getFieldToValueMap().entrySet()) {
        fieldsNode.set(entry.getKey(), serializeValue(entry.getValue()));
      }
      root.set(FIELDS_KEY, fieldsNode);

      Set<String> nullFields = row.getNullValueFields();
      if (!nullFields.isEmpty()) {
        ArrayNode nullArray = MAPPER.createArrayNode();
        for (String field : nullFields) {
          nullArray.add(field);
        }
        root.set(NULL_FIELDS_KEY, nullArray);
      }
      arrayNode.add(root);
    }
    return MAPPER.writeValueAsBytes(arrayNode);
  }

  /**
   * Deserializes a byte array back to a list of {@link GenericRow} instances.
   *
   * @param data the serialized byte array
   * @return the deserialized rows
   * @throws IOException if deserialization fails
   */
  public static List<GenericRow> deserializeRows(byte[] data)
      throws IOException {
    JsonNode arrayNode = MAPPER.readTree(data);
    List<GenericRow> rows = new ArrayList<>();
    for (JsonNode node : arrayNode) {
      GenericRow row = new GenericRow();

      Set<String> nullFields = new HashSet<>();
      if (node.has(NULL_FIELDS_KEY)) {
        for (JsonNode fieldNode : node.get(NULL_FIELDS_KEY)) {
          nullFields.add(fieldNode.asText());
        }
      }

      JsonNode fieldsNode = node.get(FIELDS_KEY);
      if (fieldsNode != null) {
        Iterator<Map.Entry<String, JsonNode>> it = fieldsNode.fields();
        while (it.hasNext()) {
          Map.Entry<String, JsonNode> entry = it.next();
          String key = entry.getKey();
          Object value = deserializeValue(entry.getValue());
          if (nullFields.contains(key)) {
            row.putDefaultNullValue(key, value);
          } else {
            row.putValue(key, value);
          }
        }
      }

      rows.add(row);
    }
    return rows;
  }

  private static JsonNode serializeValue(Object value) {
    if (value == null) {
      return MAPPER.nullNode();
    }
    if (value instanceof byte[]) {
      return MAPPER.getNodeFactory().textNode(BINARY_PREFIX + Base64.getEncoder().encodeToString((byte[]) value));
    }
    if (value instanceof Object[]) {
      ArrayNode arr = MAPPER.createArrayNode();
      for (Object elem : (Object[]) value) {
        arr.add(serializeValue(elem));
      }
      return arr;
    }
    if (value instanceof List) {
      ArrayNode arr = MAPPER.createArrayNode();
      for (Object elem : (List<?>) value) {
        arr.add(serializeValue(elem));
      }
      return arr;
    }
    return MAPPER.valueToTree(value);
  }

  private static Object deserializeValue(JsonNode node) {
    if (node.isNull()) {
      return null;
    }
    if (node.isTextual()) {
      String text = node.asText();
      if (text.startsWith(BINARY_PREFIX)) {
        return Base64.getDecoder().decode(text.substring(BINARY_PREFIX.length()));
      }
      return text;
    }
    if (node.isInt()) {
      return node.intValue();
    }
    if (node.isLong()) {
      return node.longValue();
    }
    if (node.isFloat()) {
      return node.floatValue();
    }
    if (node.isDouble()) {
      return node.doubleValue();
    }
    if (node.isBoolean()) {
      return node.booleanValue();
    }
    if (node.isArray()) {
      Object[] arr = new Object[node.size()];
      for (int i = 0; i < node.size(); i++) {
        arr[i] = deserializeValue(node.get(i));
      }
      return arr;
    }
    // Fallback: return as string
    return node.toString();
  }
}
