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
package org.apache.pinot.spi.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class JsonUtils {
  private JsonUtils() {
  }

  // For flattening
  public static final String KEY_SEPARATOR = ".";
  public static final String ARRAY_VALUE_KEY = "";
  public static final String ARRAY_INDEX_KEY = "$index";

  // NOTE: Do not expose the ObjectMapper to prevent configuration change
  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();
  public static final ObjectReader DEFAULT_READER = DEFAULT_MAPPER.reader();
  public static final ObjectWriter DEFAULT_WRITER = DEFAULT_MAPPER.writer();
  public static final ObjectWriter DEFAULT_PRETTY_WRITER = DEFAULT_MAPPER.writerWithDefaultPrettyPrinter();

  public static <T> T stringToObject(String jsonString, Class<T> valueType)
      throws IOException {
    return DEFAULT_READER.forType(valueType).readValue(jsonString);
  }

  public static <T> T stringToObject(String jsonString, TypeReference<T> valueTypeRef)
      throws IOException {
    return DEFAULT_READER.forType(valueTypeRef).readValue(jsonString);
  }

  public static JsonNode stringToJsonNode(String jsonString)
      throws IOException {
    return DEFAULT_READER.readTree(jsonString);
  }

  public static <T> T fileToObject(File jsonFile, Class<T> valueType)
      throws IOException {
    return DEFAULT_READER.forType(valueType).readValue(jsonFile);
  }

  public static <T> List<T> fileToList(File jsonFile, Class<T> valueType)
      throws IOException {
    return DEFAULT_READER.forType(DEFAULT_MAPPER.getTypeFactory().constructCollectionType(List.class, valueType))
        .readValue(jsonFile);
  }

  public static JsonNode fileToJsonNode(File jsonFile)
      throws IOException {
    try (InputStream inputStream = new FileInputStream(jsonFile)) {
      return DEFAULT_READER.readTree(inputStream);
    }
  }

  public static <T> T inputStreamToObject(InputStream jsonInputStream, Class<T> valueType)
      throws IOException {
    return DEFAULT_READER.forType(valueType).readValue(jsonInputStream);
  }

  public static JsonNode inputStreamToJsonNode(InputStream jsonInputStream)
      throws IOException {
    return DEFAULT_READER.readTree(jsonInputStream);
  }

  public static <T> T bytesToObject(byte[] jsonBytes, Class<T> valueType)
      throws IOException {
    return DEFAULT_READER.forType(valueType).readValue(jsonBytes);
  }

  public static JsonNode bytesToJsonNode(byte[] jsonBytes)
      throws IOException {
    return DEFAULT_READER.readTree(new ByteArrayInputStream(jsonBytes));
  }

  public static <T> T jsonNodeToObject(JsonNode jsonNode, Class<T> valueType)
      throws IOException {
    return DEFAULT_READER.forType(valueType).readValue(jsonNode);
  }

  public static <T> T jsonNodeToObject(JsonNode jsonNode, TypeReference<T> valueTypeRef)
      throws IOException {
    return DEFAULT_READER.forType(valueTypeRef).readValue(jsonNode);
  }

  public static String objectToString(Object object)
      throws JsonProcessingException {
    return DEFAULT_WRITER.writeValueAsString(object);
  }

  public static String objectToPrettyString(Object object)
      throws JsonProcessingException {
    return DEFAULT_PRETTY_WRITER.writeValueAsString(object);
  }

  public static byte[] objectToBytes(Object object)
      throws JsonProcessingException {
    return DEFAULT_WRITER.writeValueAsBytes(object);
  }

  public static JsonNode objectToJsonNode(Object object) {
    return DEFAULT_MAPPER.valueToTree(object);
  }

  public static ObjectNode newObjectNode() {
    return JsonNodeFactory.instance.objectNode();
  }

  public static ArrayNode newArrayNode() {
    return JsonNodeFactory.instance.arrayNode();
  }

  public static Object extractValue(@Nullable JsonNode jsonValue, FieldSpec fieldSpec) {
    if (fieldSpec.isSingleValueField()) {
      if (jsonValue != null && !jsonValue.isNull()) {
        return extractSingleValue(jsonValue, fieldSpec.getDataType());
      } else {
        return null;
      }
    } else {
      if (jsonValue != null && !jsonValue.isNull()) {
        if (jsonValue.isArray()) {
          int numValues = jsonValue.size();
          if (numValues != 0) {
            Object[] values = new Object[numValues];
            for (int i = 0; i < numValues; i++) {
              values[i] = extractSingleValue(jsonValue.get(i), fieldSpec.getDataType());
            }
            return values;
          } else {
            return null;
          }
        } else {
          return new Object[]{extractSingleValue(jsonValue, fieldSpec.getDataType())};
        }
      } else {
        return null;
      }
    }
  }

  private static Object extractSingleValue(JsonNode jsonValue, DataType dataType) {
    Preconditions.checkArgument(jsonValue.isValueNode());
    switch (dataType) {
      case INT:
        return jsonValue.asInt();
      case LONG:
        return jsonValue.asLong();
      case FLOAT:
        return (float) jsonValue.asDouble();
      case DOUBLE:
        return jsonValue.asDouble();
      case BOOLEAN:
        return jsonValue.asBoolean();
      case TIMESTAMP:
      case STRING:
      case JSON:
        return jsonValue.asText();
      case BYTES:
        try {
          return jsonValue.binaryValue();
        } catch (IOException e) {
          throw new IllegalArgumentException("Failed to extract binary value");
        }
      default:
        throw new IllegalArgumentException(String.format("Unsupported data type %s", dataType));
    }
  }

  /**
   * Flattens the given json node.
   * <p>Json array will be flattened into multiple records, where each record has a special key to store the index of
   * the element. Multi-dimensional array is not supported.
   * <pre>
   * E.g.
   * {
   *   "name": "adam",
   *   "addresses": [
   *     {
   *       "country": "us",
   *       "street": "main st",
   *       "number": 1
   *     },
   *     {
   *       "country": "ca",
   *       "street": "second st",
   *       "number": 2
   *     }
   *   ]
   * }
   * -->
   * [
   *   {
   *     "name": "adam",
   *     "addresses.$index": "0",
   *     "addresses.country": "us",
   *     "addresses.street": "main st",
   *     "addresses.number": "1"
   *   },
   *   {
   *     "name": "adam",
   *     "addresses.$index": "1",
   *     "addresses.country": "ca",
   *     "addresses.street": "second st",
   *     "addresses.number": "2"
   *   }
   * ]
   * </pre>
   */
  public static List<Map<String, String>> flatten(JsonNode node) {
    if (node.isNull()) {
      return Collections.emptyList();
    }
    Preconditions.checkState(!node.isValueNode(), "Cannot flatten value node: %s", node);

    if (node.isArray()) {
      // Array node

      List<Map<String, String>> results = new ArrayList<>();
      int numChildren = node.size();
      for (int i = 0; i < numChildren; i++) {
        JsonNode childNode = node.get(i);
        Preconditions.checkState(!childNode.isArray(), "Do not support multi-dimensional array: %s", node);

        // Skip null node
        if (childNode.isNull()) {
          continue;
        }

        String arrayIndexValue = Integer.toString(i);
        if (childNode.isValueNode()) {
          Map<String, String> result = new HashMap<>();
          result.put(ARRAY_VALUE_KEY, childNode.asText());
          result.put(ARRAY_INDEX_KEY, Integer.toString(i));
          results.add(result);
        } else {
          assert childNode.isObject();
          List<Map<String, String>> childResults = flatten(childNode);
          for (Map<String, String> childResult : childResults) {
            childResult.put(ARRAY_INDEX_KEY, arrayIndexValue);
            results.add(childResult);
          }
        }
      }
      return results;
    } else {
      // Object node
      assert node.isObject();

      Map<String, String> nonNestedResult = new HashMap<>();
      List<List<Map<String, String>>> nestedResultsList = new ArrayList<>();
      Iterator<Map.Entry<String, JsonNode>> fieldIterator = node.fields();
      while (fieldIterator.hasNext()) {
        Map.Entry<String, JsonNode> fieldEntry = fieldIterator.next();
        String fieldName = fieldEntry.getKey();
        JsonNode childNode = fieldEntry.getValue();

        // Skip null node
        if (childNode.isNull()) {
          continue;
        }

        if (childNode.isValueNode()) {
          nonNestedResult.put(fieldName, childNode.asText());
        } else {
          String prefix = fieldName + KEY_SEPARATOR;
          List<Map<String, String>> childResults = flatten(childNode);
          int numChildResults = childResults.size();

          // Skip empty array
          if (numChildResults == 0) {
            continue;
          }

          if (numChildResults == 1) {
            Map<String, String> childResult = childResults.get(0);
            for (Map.Entry<String, String> entry : childResult.entrySet()) {
              String key = entry.getKey();
              String value = entry.getValue();
              if (key.equals(ARRAY_VALUE_KEY)) {
                nonNestedResult.put(fieldName, value);
              } else {
                nonNestedResult.put(prefix + key, value);
              }
            }
          } else {
            // If the flattened child node contains multiple records, add prefix to all the keys and put the results
            // into a list to be processed later.
            for (int i = 0; i < numChildResults; i++) {
              Map<String, String> childResult = childResults.get(i);
              Map<String, String> prefixedResult = new HashMap<>();
              for (Map.Entry<String, String> entry : childResult.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (key.equals(ARRAY_VALUE_KEY)) {
                  prefixedResult.put(fieldName, value);
                } else {
                  prefixedResult.put(prefix + key, value);
                }
              }
              childResults.set(i, prefixedResult);
            }
            nestedResultsList.add(childResults);
          }
        }
      }
      int nestedResultsListSize = nestedResultsList.size();
      if (nestedResultsListSize == 0) {
        return Collections.singletonList(nonNestedResult);
      } else if (nestedResultsListSize == 1) {
        List<Map<String, String>> nestedResults = nestedResultsList.get(0);
        for (Map<String, String> nestedResult : nestedResults) {
          nestedResult.putAll(nonNestedResult);
        }
        return nestedResults;
      } else {
        // If there are multiple child nodes with multiple records, calculate each combination of them as a new record.
        List<Map<String, String>> results = new ArrayList<>();
        unnestResults(nestedResultsList.get(0), nestedResultsList, 1, nonNestedResult, results);
        return results;
      }
    }
  }

  private static void unnestResults(List<Map<String, String>> currentResults,
      List<List<Map<String, String>>> nestedResultsList, int index, Map<String, String> nonNestedResult,
      List<Map<String, String>> outputResults) {
    int nestedResultsListSize = nestedResultsList.size();
    if (nestedResultsListSize == index) {
      for (Map<String, String> currentResult : currentResults) {
        currentResult.putAll(nonNestedResult);
        outputResults.add(currentResult);
      }
    } else {
      List<Map<String, String>> nestedResults = nestedResultsList.get(index);
      int numCurrentResults = currentResults.size();
      int numNestedResults = nestedResults.size();
      List<Map<String, String>> newCurrentResults = new ArrayList<>(numCurrentResults * numNestedResults);
      for (Map<String, String> currentResult : currentResults) {
        for (Map<String, String> nestedResult : nestedResults) {
          Map<String, String> newCurrentResult = new HashMap<>(currentResult);
          newCurrentResult.putAll(nestedResult);
          newCurrentResults.add(newCurrentResult);
        }
      }
      unnestResults(newCurrentResults, nestedResultsList, index + 1, nonNestedResult, outputResults);
    }
  }
}
