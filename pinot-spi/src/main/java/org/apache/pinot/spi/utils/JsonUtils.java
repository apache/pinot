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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;


public class JsonUtils {
  private JsonUtils() {
  }

  // For flattening
  public static final String VALUE_KEY = "";
  public static final String KEY_SEPARATOR = ".";
  public static final String ARRAY_PATH = "[*]";
  public static final String ARRAY_INDEX_KEY = ".$index";
  public static final String SKIPPED_VALUE_REPLACEMENT = "$SKIPPED$";
  public static final int MAX_COMBINATIONS = 100_000;
  private static final List<Map<String, String>> SKIPPED_FLATTENED_RECORD =
      Collections.singletonList(Collections.singletonMap(VALUE_KEY, SKIPPED_VALUE_REPLACEMENT));

  // For querying
  public static final String WILDCARD = "*";

  // NOTE: Do not expose the ObjectMapper to prevent configuration change
  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();
  public static final ObjectReader DEFAULT_READER = DEFAULT_MAPPER.reader();
  public static final ObjectWriter DEFAULT_WRITER = DEFAULT_MAPPER.writer();
  public static final ObjectWriter DEFAULT_PRETTY_WRITER = DEFAULT_MAPPER.writerWithDefaultPrettyPrinter();
  public static final ObjectReader READER_WITH_BIG_DECIMAL =
      new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).reader();

  public static final TypeReference<HashMap<String, Object>> MAP_TYPE_REFERENCE =
      new TypeReference<HashMap<String, Object>>() {
      };

  public static <T> T stringToObject(String jsonString, Class<T> valueType)
      throws JsonProcessingException {
    return DEFAULT_READER.forType(valueType).readValue(jsonString);
  }

  public static <T> Pair<T, Map<String, Object>> inputStreamToObjectAndUnrecognizedProperties(
      InputStream jsonInputStream, Class<T> valueType)
      throws IOException {
    String jsonString = IOUtils.toString(jsonInputStream, StandardCharsets.UTF_8);
    return stringToObjectAndUnrecognizedProperties(jsonString, valueType);
  }

  public static <T> Pair<T, Map<String, Object>> stringToObjectAndUnrecognizedProperties(String jsonString,
      Class<T> valueType)
      throws IOException {
    T instance = DEFAULT_READER.forType(valueType).readValue(jsonString);
    Map<String, Object> inputJsonMap = flatten(DEFAULT_MAPPER.readValue(jsonString, MAP_TYPE_REFERENCE));

    String instanceJson = DEFAULT_MAPPER.writeValueAsString(instance);
    Map<String, Object> instanceJsonMap = flatten(DEFAULT_MAPPER.readValue(instanceJson, MAP_TYPE_REFERENCE));

    MapDifference<String, Object> difference = Maps.difference(inputJsonMap, instanceJsonMap);
    return Pair.of(instance, difference.entriesOnlyOnLeft());
  }

  private static Map<String, Object> flatten(Map<String, Object> map) {
    return map.entrySet().stream().flatMap(JsonUtils::flatten)
        .collect(LinkedHashMap::new, (m, e) -> m.put("/" + e.getKey(), e.getValue()), LinkedHashMap::putAll);
  }

  private static Stream<Map.Entry<String, Object>> flatten(Map.Entry<String, Object> entry) {
    if (entry == null) {
      return Stream.empty();
    }

    if (entry.getValue() instanceof Map<?, ?>) {
      return ((Map<?, ?>) entry.getValue()).entrySet().stream()
          .flatMap(e -> flatten(new AbstractMap.SimpleEntry<>(entry.getKey() + "/" + e.getKey(), e.getValue())));
    }

    if (entry.getValue() instanceof List<?>) {
      List<?> list = (List<?>) entry.getValue();
      return IntStream.range(0, list.size())
          .mapToObj(i -> new AbstractMap.SimpleEntry<String, Object>(entry.getKey() + "/" + i, list.get(i)))
          .flatMap(JsonUtils::flatten);
    }

    return Stream.of(entry);
  }

  public static <T> T stringToObject(String jsonString, TypeReference<T> valueTypeRef)
      throws IOException {
    return DEFAULT_READER.forType(valueTypeRef).readValue(jsonString);
  }

  public static JsonNode stringToJsonNode(String jsonString)
      throws IOException {
    return DEFAULT_READER.readTree(jsonString);
  }

  public static JsonNode stringToJsonNodeWithBigDecimal(String jsonString)
      throws IOException {
    return READER_WITH_BIG_DECIMAL.readTree(jsonString);
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

  /**
   * Reads the first json object from the file that can contain multiple objects
   */
  @Nullable
  public static JsonNode fileToFirstJsonNode(File jsonFile)
      throws IOException {
    JsonFactory jf = new JsonFactory();
    try (InputStream inputStream = new FileInputStream(jsonFile); JsonParser jp = jf.createParser(inputStream)) {
      jp.setCodec(DEFAULT_MAPPER);
      jp.nextToken();
      if (jp.hasCurrentToken()) {
        return DEFAULT_MAPPER.readTree(jp);
      }
      return null;
    }
  }

  public static <T> T inputStreamToObject(InputStream jsonInputStream, Class<T> valueType)
      throws IOException {
    return DEFAULT_READER.forType(valueType).readValue(jsonInputStream);
  }

  public static <T> T inputStreamToObject(InputStream jsonInputStream, TypeReference<T> valueTypeRef)
      throws IOException {
    return DEFAULT_READER.forType(valueTypeRef).readValue(jsonInputStream);
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

  public static Map<String, Object> jsonNodeToMap(JsonNode jsonNode)
      throws IOException {
    return DEFAULT_READER.forType(MAP_TYPE_REFERENCE).readValue(jsonNode);
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
   * the element.
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
   *     ".name": "adam",
   *     ".addresses.$index": "0",
   *     ".addresses..country": "us",
   *     ".addresses..street": "main st",
   *     ".addresses..number": "1"
   *   },
   *   {
   *     ".name": "adam",
   *     ".addresses.$index": "1",
   *     ".addresses..country": "ca",
   *     ".addresses..street": "second st",
   *     ".addresses..number": "2"
   *   }
   * ]
   * </pre>
   */
  protected static List<Map<String, String>> flatten(JsonNode node, JsonIndexConfig jsonIndexConfig) {
    try {
      return flatten(node, jsonIndexConfig, 0, "$", false);
    } catch (OutOfMemoryError oom) {
      throw new OutOfMemoryError(
          String.format("Flattening JSON node: %s with config: %s requires too much memory, please adjust the config",
              node, jsonIndexConfig));
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Caught exception while flattening JSON node: %s with config: %s", node, jsonIndexConfig), e);
    }
  }

  private static List<Map<String, String>> flatten(JsonNode node, JsonIndexConfig jsonIndexConfig, int level,
      String path, boolean includePathMatched) {
    // Null
    if (node.isNull()) {
      return Collections.emptyList();
    }

    // Value
    if (node.isValueNode()) {
      String valueAsText = node.asText();
      int maxValueLength = jsonIndexConfig.getMaxValueLength();
      if (0 < maxValueLength && maxValueLength < valueAsText.length()) {
        valueAsText = SKIPPED_VALUE_REPLACEMENT;
      }
      return Collections.singletonList(Collections.singletonMap(VALUE_KEY, valueAsText));
    }

    Preconditions.checkArgument(node.isArray() || node.isObject(), "Unexpected node type: %s", node.getNodeType());

    // Do not flatten further for array and object when max level reached
    int maxLevels = jsonIndexConfig.getMaxLevels();
    if (maxLevels > 0 && level == maxLevels) {
      return Collections.emptyList();
    }

    // Array
    if (node.isArray()) {
      if (jsonIndexConfig.isExcludeArray()) {
        return Collections.emptyList();
      }
      int numChildren = node.size();
      if (numChildren == 0) {
        return Collections.emptyList();
      }
      String childPath = path + ARRAY_PATH;
      IncludeResult includeResult =
          includePathMatched ? IncludeResult.MATCH : shouldInclude(jsonIndexConfig, childPath);
      if (!includeResult._shouldInclude) {
        return Collections.emptyList();
      }
      List<Map<String, String>> results = new ArrayList<>(numChildren);
      for (int i = 0; i < numChildren; i++) {
        JsonNode childNode = node.get(i);
        String arrayIndexValue = Integer.toString(i);
        List<Map<String, String>> childResults =
            flatten(childNode, jsonIndexConfig, level + 1, childPath, includeResult._includePathMatched);
        for (Map<String, String> childResult : childResults) {
          Map<String, String> result = new TreeMap<>();
          for (Map.Entry<String, String> entry : childResult.entrySet()) {
            result.put(KEY_SEPARATOR + entry.getKey(), entry.getValue());
          }
          result.put(ARRAY_INDEX_KEY, arrayIndexValue);
          results.add(result);
        }
      }
      return results;
    }

    // Object
    assert node.isObject();

    // Merge all non-nested results into a single map
    Map<String, String> nonNestedResult = new TreeMap<>();
    // Put all nested results (from array) into a list to be processed later
    List<List<Map<String, String>>> nestedResultsList = new ArrayList<>();

    Iterator<Map.Entry<String, JsonNode>> fieldIterator = node.fields();
    while (fieldIterator.hasNext()) {
      Map.Entry<String, JsonNode> fieldEntry = fieldIterator.next();
      String field = fieldEntry.getKey();
      Set<String> excludeFields = jsonIndexConfig.getExcludeFields();
      if (excludeFields != null && excludeFields.contains(field)) {
        continue;
      }
      String childPath = path + KEY_SEPARATOR + field;
      IncludeResult includeResult =
          includePathMatched ? IncludeResult.MATCH : shouldInclude(jsonIndexConfig, childPath);
      if (!includeResult._shouldInclude) {
        continue;
      }
      JsonNode childNode = fieldEntry.getValue();
      List<Map<String, String>> childResults =
          flatten(childNode, jsonIndexConfig, level + 1, childPath, includeResult._includePathMatched);
      int numChildResults = childResults.size();

      // Empty list - skip
      if (numChildResults == 0) {
        continue;
      }

      // Single map - put all key-value pairs into the non-nested result map
      String prefix = KEY_SEPARATOR + field;
      if (numChildResults == 1) {
        Map<String, String> childResult = childResults.get(0);
        for (Map.Entry<String, String> entry : childResult.entrySet()) {
          nonNestedResult.put(prefix + entry.getKey(), entry.getValue());
        }
        continue;
      }

      // Multiple maps - put the results into a list to be processed later
      List<Map<String, String>> prefixedResults = new ArrayList<>(numChildResults);
      for (Map<String, String> childResult : childResults) {
        Map<String, String> prefixedResult = new TreeMap<>();
        for (Map.Entry<String, String> entry : childResult.entrySet()) {
          prefixedResult.put(prefix + entry.getKey(), entry.getValue());
        }
        prefixedResults.add(prefixedResult);
      }
      nestedResultsList.add(prefixedResults);
    }

    // Merge non-nested results and nested results
    int nestedResultsListSize = nestedResultsList.size();
    if (nestedResultsListSize == 0) {
      if (nonNestedResult.isEmpty()) {
        return Collections.emptyList();
      } else {
        return Collections.singletonList(nonNestedResult);
      }
    }
    if (nestedResultsListSize == 1) {
      List<Map<String, String>> nestedResults = nestedResultsList.get(0);
      for (Map<String, String> nestedResult : nestedResults) {
        nestedResult.putAll(nonNestedResult);
      }
      return nestedResults;
    }
    // Multiple child nodes with multiple records
    if (jsonIndexConfig.isDisableCrossArrayUnnest()) {
      // Add each array individually
      int numResults = 0;
      for (List<Map<String, String>> nestedResults : nestedResultsList) {
        numResults += nestedResults.size();
      }
      List<Map<String, String>> results = new ArrayList<>(numResults);
      for (List<Map<String, String>> nestedResults : nestedResultsList) {
        for (Map<String, String> nestedResult : nestedResults) {
          nestedResult.putAll(nonNestedResult);
          results.add(nestedResult);
        }
      }
      return results;
    } else {
      // Calculate each combination of them as a new record
      long numResults = 1;
      for (List<Map<String, String>> nestedResults : nestedResultsList) {
        numResults *= nestedResults.size();
        Preconditions.checkState(numResults < MAX_COMBINATIONS, "Got too many combinations");
      }
      List<Map<String, String>> results = new ArrayList<>((int) numResults);
      unnestResults(nestedResultsList.get(0), nestedResultsList, 1, nonNestedResult, results);
      return results;
    }
  }

  private static IncludeResult shouldInclude(JsonIndexConfig jsonIndexConfig, String path) {
    Set<String> includePaths = jsonIndexConfig.getIncludePaths();
    if (includePaths != null) {
      if (includePaths.contains(path)) {
        return IncludeResult.MATCH;
      }
      for (String includePath : includePaths) {
        if (includePath.startsWith(path)) {
          return IncludeResult.POTENTIAL_MATCH;
        }
      }
      return IncludeResult.NOT_MATCH;
    }
    Set<String> excludePaths = jsonIndexConfig.getExcludePaths();
    if (excludePaths != null && excludePaths.contains(path)) {
      return IncludeResult.NOT_MATCH;
    }
    return IncludeResult.POTENTIAL_MATCH;
  }

  private enum IncludeResult {
    MATCH(true, true), POTENTIAL_MATCH(true, false), NOT_MATCH(false, false);

    final boolean _shouldInclude;
    final boolean _includePathMatched;

    IncludeResult(boolean shouldInclude, boolean includePathMatched) {
      _shouldInclude = shouldInclude;
      _includePathMatched = includePathMatched;
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
          Map<String, String> newCurrentResult = new TreeMap<>(currentResult);
          newCurrentResult.putAll(nestedResult);
          newCurrentResults.add(newCurrentResult);
        }
      }
      unnestResults(newCurrentResults, nestedResultsList, index + 1, nonNestedResult, outputResults);
    }
  }

  public static Schema getPinotSchemaFromJsonFile(File jsonFile,
      @Nullable Map<String, FieldSpec.FieldType> fieldTypeMap, @Nullable TimeUnit timeUnit,
      @Nullable List<String> fieldsToUnnest, String delimiter,
      ComplexTypeConfig.CollectionNotUnnestedToJson collectionNotUnnestedToJson)
      throws IOException {
    JsonNode jsonNode = fileToFirstJsonNode(jsonFile);
    if (fieldsToUnnest == null) {
      fieldsToUnnest = new ArrayList<>();
    }
    Preconditions.checkNotNull(jsonNode, "the JSON data shall be an object but it is null");
    Preconditions.checkState(jsonNode.isObject(), "the JSON data shall be an object");
    return getPinotSchemaFromJsonNode(jsonNode, fieldTypeMap, timeUnit, fieldsToUnnest, delimiter,
        collectionNotUnnestedToJson);
  }

  public static Schema getPinotSchemaFromJsonNode(JsonNode jsonNode,
      @Nullable Map<String, FieldSpec.FieldType> fieldTypeMap, @Nullable TimeUnit timeUnit, List<String> fieldsToUnnest,
      String delimiter, ComplexTypeConfig.CollectionNotUnnestedToJson collectionNotUnnestedToJson) {
    Schema pinotSchema = new Schema();
    Iterator<Map.Entry<String, JsonNode>> fieldIterator = jsonNode.fields();
    while (fieldIterator.hasNext()) {
      Map.Entry<String, JsonNode> fieldEntry = fieldIterator.next();
      JsonNode childNode = fieldEntry.getValue();
      inferPinotSchemaFromJsonNode(childNode, pinotSchema, fieldEntry.getKey(), fieldTypeMap, timeUnit, fieldsToUnnest,
          delimiter, collectionNotUnnestedToJson);
    }
    return pinotSchema;
  }

  private static void inferPinotSchemaFromJsonNode(JsonNode jsonNode, Schema pinotSchema, String path,
      @Nullable Map<String, FieldSpec.FieldType> fieldTypeMap, @Nullable TimeUnit timeUnit, List<String> fieldsToUnnest,
      String delimiter, ComplexTypeConfig.CollectionNotUnnestedToJson collectionNotUnnestedToJson) {
    if (jsonNode.isNull()) {
      // do nothing
      return;
    } else if (jsonNode.isValueNode()) {
      DataType dataType = valueOf(jsonNode);
      addFieldToPinotSchema(pinotSchema, dataType, path, true, fieldTypeMap, timeUnit);
    } else if (jsonNode.isArray()) {
      int numChildren = jsonNode.size();
      if (numChildren == 0) {
        // do nothing
        return;
      }
      JsonNode childNode = jsonNode.get(0);

      if (fieldsToUnnest.contains(path)) {
        inferPinotSchemaFromJsonNode(childNode, pinotSchema, path, fieldTypeMap, timeUnit, fieldsToUnnest, delimiter,
            collectionNotUnnestedToJson);
      } else if (shallConvertToJson(collectionNotUnnestedToJson, childNode)) {
        addFieldToPinotSchema(pinotSchema, DataType.STRING, path, true, fieldTypeMap, timeUnit);
      } else if (collectionNotUnnestedToJson == ComplexTypeConfig.CollectionNotUnnestedToJson.NON_PRIMITIVE
          && childNode.isValueNode()) {
        addFieldToPinotSchema(pinotSchema, valueOf(childNode), path, false, fieldTypeMap, timeUnit);
      }
      // do not include the node for other cases
    } else if (jsonNode.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> fieldIterator = jsonNode.fields();
      while (fieldIterator.hasNext()) {
        Map.Entry<String, JsonNode> fieldEntry = fieldIterator.next();
        JsonNode childNode = fieldEntry.getValue();
        inferPinotSchemaFromJsonNode(childNode, pinotSchema, String.join(delimiter, path, fieldEntry.getKey()),
            fieldTypeMap, timeUnit, fieldsToUnnest, delimiter, collectionNotUnnestedToJson);
      }
    } else {
      throw new IllegalArgumentException(String.format("Unsupported json node type for class %s", jsonNode.getClass()));
    }
  }

  private static boolean shallConvertToJson(ComplexTypeConfig.CollectionNotUnnestedToJson collectionNotUnnestedToJson,
      JsonNode childNode) {
    switch (collectionNotUnnestedToJson) {
      case ALL:
        return true;
      case NONE:
        return false;
      case NON_PRIMITIVE:
        return !childNode.isValueNode();
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported collectionNotUnnestedToJson %s", collectionNotUnnestedToJson));
    }
  }

  /**
   * Returns the data type stored in Pinot that is associated with the given Avro type.
   */
  public static DataType valueOf(JsonNode jsonNode) {
    if (jsonNode.isInt()) {
      return DataType.INT;
    } else if (jsonNode.isLong()) {
      return DataType.LONG;
    } else if (jsonNode.isFloat()) {
      return DataType.FLOAT;
    } else if (jsonNode.isDouble()) {
      return DataType.DOUBLE;
    } else if (jsonNode.isBoolean()) {
      return DataType.BOOLEAN;
    } else if (jsonNode.isBinary()) {
      return DataType.BYTES;
    } else if (jsonNode.isBigDecimal()) {
      return DataType.BIG_DECIMAL;
    } else {
      return DataType.STRING;
    }
  }

  private static void addFieldToPinotSchema(Schema pinotSchema, DataType dataType, String name,
      boolean isSingleValueField, @Nullable Map<String, FieldSpec.FieldType> fieldTypeMap,
      @Nullable TimeUnit timeUnit) {
    if (fieldTypeMap == null) {
      pinotSchema.addField(new DimensionFieldSpec(name, dataType, isSingleValueField));
    } else {
      FieldSpec.FieldType fieldType = fieldTypeMap.getOrDefault(name, FieldSpec.FieldType.DIMENSION);
      Preconditions.checkNotNull(fieldType, "Field type not specified for field: %s", name);
      switch (fieldType) {
        case DIMENSION:
          pinotSchema.addField(new DimensionFieldSpec(name, dataType, isSingleValueField));
          break;
        case METRIC:
          Preconditions.checkState(isSingleValueField, "Metric field: %s cannot be multi-valued", name);
          pinotSchema.addField(new MetricFieldSpec(name, dataType));
          break;
        case DATE_TIME:
          Preconditions.checkState(isSingleValueField, "Time field: %s cannot be multi-valued", name);
          Preconditions.checkNotNull(timeUnit, "Time unit cannot be null");
          // TODO: Switch to new format after releasing 0.11.0
          //       "EPOCH|" + timeUnit.name()
          String format = "1:" + timeUnit.name() + ":EPOCH";
          String granularity = "1:" + timeUnit.name();
          pinotSchema.addField(new DateTimeFieldSpec(name, dataType, format, granularity));
          break;
        default:
          throw new UnsupportedOperationException("Unsupported field type: " + fieldType + " for field: " + name);
      }
    }
  }

  public static List<Map<String, String>> flatten(String jsonString, JsonIndexConfig jsonIndexConfig)
      throws IOException {
    JsonNode jsonNode;
    try {
      jsonNode = JsonUtils.stringToJsonNode(jsonString);
    } catch (JsonProcessingException e) {
      if (jsonIndexConfig.getSkipInvalidJson()) {
        return SKIPPED_FLATTENED_RECORD;
      } else {
        throw e;
      }
    }
    return JsonUtils.flatten(jsonNode, jsonIndexConfig);
  }
}
