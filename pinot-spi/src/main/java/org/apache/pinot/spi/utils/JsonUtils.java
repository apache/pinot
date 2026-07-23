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
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalTimeSerializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.ArrayList;
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
import org.apache.commons.lang3.StringUtils;
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
  public static final String GLOBAL_WILDCARD = "**"; // represent all the fields in the current or below levels
  public static final String ARRAY_PATH = "[*]";
  public static final String ARRAY_INDEX_KEY = ".$index";
  public static final String SKIPPED_VALUE_REPLACEMENT = "$SKIPPED$";
  public static final int MAX_COMBINATIONS = 100_000;
  public static final List<Map<String, String>> SKIPPED_FLATTENED_RECORD =
      List.of(Map.of(VALUE_KEY, SKIPPED_VALUE_REPLACEMENT));

  // For querying
  public static final String WILDCARD = "*";


  // NOTE: Do not expose the ObjectMapper to prevent configuration change.
  //
  // JavaTimeModule is registered so the LocalDate / LocalTime values produced by RecordExtractor (per the
  // contract on org.apache.pinot.spi.data.readers.RecordExtractor) serialize correctly when they reach
  // Jackson via PinotDataType.toJson. Each java.time type is bound to an explicit ISO-8601 formatter so the
  // output is independent of SerializationFeature.WRITE_DATES_AS_TIMESTAMPS — the global flag stays at its
  // default (true), so any java.util.Date / java.sql.Timestamp continues to serialize as numeric epoch
  // millis (Timestamp.toString is JVM-timezone-dependent JDBC escape format, not ISO-8601, so a string form
  // would be neither portable nor consistent across paths).
  // Single shared JSR-310 module instance with ISO-8601 serializers for LocalDate / LocalTime. Safe to
  // share across ObjectMappers — Jackson copies the module's handlers into each mapper at registration
  // time, and the serializers themselves are stateless.
  private static final JavaTimeModule JAVA_TIME_MODULE = buildJavaTimeModule();
  private static final ObjectMapper DEFAULT_MAPPER = newObjectMapperWithJavaTime();
  public static final ObjectReader DEFAULT_READER = DEFAULT_MAPPER.reader();
  public static final ObjectWriter DEFAULT_WRITER = DEFAULT_MAPPER.writer();
  public static final ObjectWriter DEFAULT_PRETTY_WRITER = DEFAULT_MAPPER.writerWithDefaultPrettyPrinter();
  public static final ObjectReader READER_WITH_BIG_DECIMAL =
      newObjectMapperWithJavaTime().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).reader();

  /// Returns a fresh [ObjectMapper] with the JSR-310 [JavaTimeModule] registered (ISO-8601 serializers for
  /// `LocalDate` / `LocalTime`). Callers needing additional configuration (e.g. sorted map entries) can
  /// chain `configure(...)` on the returned instance.
  public static ObjectMapper newObjectMapperWithJavaTime() {
    return new ObjectMapper().registerModule(JAVA_TIME_MODULE);
  }

  private static JavaTimeModule buildJavaTimeModule() {
    JavaTimeModule module = new JavaTimeModule();
    module.addSerializer(LocalDate.class, new LocalDateSerializer(DateTimeFormatter.ISO_LOCAL_DATE));
    module.addSerializer(LocalTime.class, new LocalTimeSerializer(DateTimeFormatter.ISO_LOCAL_TIME));
    return module;
  }

  public static final TypeReference<HashMap<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() {
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

  public static JsonNode bytesToJsonNode(byte[] jsonBytes, int offset, int length)
      throws IOException {
    return DEFAULT_READER.readTree(new ByteArrayInputStream(jsonBytes, offset, length));
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

  public static Map<String, Object> stringToMap(String jsonString)
      throws JsonProcessingException {
    return DEFAULT_READER.forType(MAP_TYPE_REFERENCE).readValue(jsonString);
  }

  public static Map<String, Object> bytesToMap(byte[] jsonBytes)
      throws IOException {
    return DEFAULT_READER.forType(MAP_TYPE_REFERENCE).readValue(jsonBytes);
  }

  public static Map<String, Object> bytesToMap(byte[] jsonBytes, int offset, int length)
      throws IOException {
    return DEFAULT_READER.forType(MAP_TYPE_REFERENCE).readValue(jsonBytes, offset, length);
  }

  public static String objectToString(Object object)
      throws JsonProcessingException {
    return DEFAULT_WRITER.writeValueAsString(object);
  }

  public static void objectToOutputStream(Object object, OutputStream outputStream)
      throws IOException {
    DEFAULT_WRITER.writeValue(outputStream, object);
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
        throw new IllegalArgumentException("Unsupported data type " + dataType);
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
      return flatten(node, jsonIndexConfig, 0, "$", false, createTree(jsonIndexConfig));
    } catch (OutOfMemoryError oom) {
      throw new OutOfMemoryError("Flattening JSON node: " + node + " with config: " + jsonIndexConfig + " requires too "
          + "much memory, please adjust the config");
    } catch (Exception e) {
      throw new IllegalArgumentException("Caught exception while flattening JSON node: " + node + " with config: "
          + jsonIndexConfig, e);
    }
  }

  private static List<Map<String, String>> flatten(JsonNode node, JsonIndexConfig jsonIndexConfig, int level,
      String path, boolean includePathMatched, JsonSchemaTreeNode indexPathNode) {
    // Null
    if (node.isNull() || node.isMissingNode() || indexPathNode == null) {
      return List.of();
    }

    // Value
    if (node.isValueNode()) {
      String valueAsText = node.asText();
      int maxValueLength = jsonIndexConfig.getMaxValueLength();
      if (0 < maxValueLength && maxValueLength < valueAsText.length()) {
        valueAsText = SKIPPED_VALUE_REPLACEMENT;
      }
      return List.of(Map.of(VALUE_KEY, valueAsText));
    }

    Preconditions.checkArgument(node.isArray() || node.isObject(), "Unexpected node type: %s", node.getNodeType());

    // Do not flatten further for array and object when max level reached
    int maxLevels = jsonIndexConfig.getMaxLevels();
    if (maxLevels > 0 && level == maxLevels) {
      return List.of();
    }

    // Array
    if (node.isArray()) {
      if (jsonIndexConfig.isExcludeArray()) {
        return List.of();
      }
      int numChildren = node.size();
      if (numChildren == 0) {
        return List.of();
      }
      String childPath = path + ARRAY_PATH;
      IncludeResult includeResult =
          includePathMatched ? IncludeResult.MATCH : shouldInclude(jsonIndexConfig, childPath);
      if (!includeResult._shouldInclude) {
        return List.of();
      }
      List<Map<String, String>> results = new ArrayList<>(numChildren);
      for (int i = 0; i < numChildren; i++) {
        JsonNode childNode = node.get(i);
        String arrayIndexValue = Integer.toString(i);
        List<Map<String, String>> childResults =
            flatten(childNode, jsonIndexConfig, level + 1, childPath, includeResult._includePathMatched,
                indexPathNode.getChild(""));
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

    for (Map.Entry<String, JsonNode> fieldEntry : node.properties()) {
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
          flatten(childNode, jsonIndexConfig, level + 1, childPath, includeResult._includePathMatched,
              indexPathNode.getChild(field));
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
        return List.of();
      } else {
        return List.of(nonNestedResult);
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
    for (Map.Entry<String, JsonNode> fieldEntry : jsonNode.properties()) {
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
      for (Map.Entry<String, JsonNode> fieldEntry : jsonNode.properties()) {
        JsonNode childNode = fieldEntry.getValue();
        inferPinotSchemaFromJsonNode(childNode, pinotSchema, String.join(delimiter, path, fieldEntry.getKey()),
            fieldTypeMap, timeUnit, fieldsToUnnest, delimiter, collectionNotUnnestedToJson);
      }
    } else {
      throw new IllegalArgumentException("Unsupported json node type for class " + jsonNode.getClass());
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
        throw new IllegalArgumentException("Unsupported collectionNotUnnestedToJson " + collectionNotUnnestedToJson);
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
          String format = "EPOCH|" + timeUnit.name();
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
      return JsonUtils.flatten(jsonNode, jsonIndexConfig);
    } catch (Exception e) {
      if (jsonIndexConfig.getSkipInvalidJson()) {
        return SKIPPED_FLATTENED_RECORD;
      } else {
        throw e;
      }
    }
  }

  /// Flattens an already-parsed JSON value ({@link Map} / {@link List} / {@link JsonNode}) for the JSON index, avoiding
  /// the string tokenization that {@link #flatten(String, JsonIndexConfig)} performs. Used by the realtime JSON index
  /// when the source value is already a parsed object (e.g. cached on the `GenericRow` before it is serialized to a
  /// string for the forward index), so the document is parsed once at ingestion instead of being serialized and
  /// re-parsed here. The result must match {@link #flatten(String, JsonIndexConfig)} on the serialized form (both go
  /// through `DEFAULT_MAPPER`). A {@link String} input is delegated to {@link #flatten(String, JsonIndexConfig)}.
  public static List<Map<String, String>> flattenParsed(Object jsonValue, JsonIndexConfig jsonIndexConfig)
      throws IOException {
    if (jsonValue instanceof String) {
      return flatten((String) jsonValue, jsonIndexConfig);
    }
    JsonNode jsonNode = jsonValue instanceof JsonNode ? (JsonNode) jsonValue : DEFAULT_MAPPER.valueToTree(jsonValue);
    int classification = classifyForFlatten(jsonNode);
    if (classification == FLATTEN_UNSAFE) {
      // A leaf renders differently than the string path (e.g. a Float or byte[] from a non-JSON RecordReader). Fall
      // back to serialize+reparse so the flattened records are byte-for-byte identical. Rare: the JSON decoders never
      // produce these types.
      return flatten(objectToString(jsonValue), jsonIndexConfig);
    }
    try {
      // A DecimalNode (a BigDecimal float, e.g. from stringToJsonNodeWithBigDecimal) renders its plain value, but the
      // string path re-parses it: an integral value becomes an int/long, a fractional one a double (possibly in
      // scientific notation). Re-parse just those leaves the same way so flatten matches, without re-tokenizing the
      // whole document as the serialize+reparse fallback would.
      JsonNode toFlatten =
          classification == FLATTEN_SAFE_WITH_BIG_DECIMAL ? normalizeBigDecimalNodes(jsonNode) : jsonNode;
      return JsonUtils.flatten(toFlatten, jsonIndexConfig);
    } catch (Exception e) {
      if (jsonIndexConfig.getSkipInvalidJson()) {
        return SKIPPED_FLATTENED_RECORD;
      } else {
        throw e;
      }
    }
  }

  private static final int FLATTEN_UNSAFE = 0;
  private static final int FLATTEN_SAFE = 1;
  private static final int FLATTEN_SAFE_WITH_BIG_DECIMAL = 2;

  /// Classifies whether a parsed value can be flattened directly so the records are byte-for-byte identical to
  /// flattening its serialized string (both paths render each leaf via {@link JsonNode#asText()}). {@code Integer},
  /// {@code Long}, {@code Double} and {@code BigInteger} nodes render identically to the string path
  /// ({@link #FLATTEN_SAFE}); a {@code DecimalNode} (a {@code BigDecimal} float) renders its plain value while the
  /// string path re-parses it (an integral value as an int/long, a fractional one as a double), so it is re-parsed the
  /// same way first ({@link #FLATTEN_SAFE_WITH_BIG_DECIMAL}, e.g. {@code "2.0"} -> {@code "2"},
  /// {@code "1234567890.5"} -> {@code "1.2345678905E9"}); a {@code FloatNode} or a binary / POJO node does not
  /// round-trip and forces the serialize+reparse fallback ({@link #FLATTEN_UNSAFE}). The JSON decoders only produce
  /// String / Integer / Long / Double / BigInteger / BigDecimal, so a parsed {@link JsonNode} is never
  /// {@code FLATTEN_UNSAFE}; the fallback only applies to Float / byte[] leaves from a non-JSON RecordReader.
  private static int classifyForFlatten(JsonNode node) {
    switch (node.getNodeType()) {
      case NUMBER:
        if (node.isFloat()) {
          return FLATTEN_UNSAFE;
        }
        return node.isBigDecimal() ? FLATTEN_SAFE_WITH_BIG_DECIMAL : FLATTEN_SAFE;
      case OBJECT:
      case ARRAY: {
        int result = FLATTEN_SAFE;
        for (JsonNode child : node) {
          int childClassification = classifyForFlatten(child);
          if (childClassification == FLATTEN_UNSAFE) {
            return FLATTEN_UNSAFE;
          }
          if (childClassification == FLATTEN_SAFE_WITH_BIG_DECIMAL) {
            result = FLATTEN_SAFE_WITH_BIG_DECIMAL;
          }
        }
        return result;
      }
      case STRING:
      case BOOLEAN:
      case NULL:
        return FLATTEN_SAFE;
      default:
        // BINARY, POJO, MISSING
        return FLATTEN_UNSAFE;
    }
  }

  /// Returns a copy of the tree with every {@code DecimalNode} re-parsed the way the string path does (serialize via
  /// {@code toString}, parse with the default reader), so {@link #flatten}'s {@code asText()} is identical -- whether
  /// the value renders as an integer, a double, or in scientific notation -- while sharing all other leaf nodes. Only
  /// called when the tree actually contains a {@code BigDecimal} node. Using {@code doubleValue()} instead would be
  /// wrong: an integral decimal ({@code 2.0}) re-parses to an integer ({@code "2"}, not {@code "2.0"}) and values past
  /// 2^53 would lose precision.
  private static JsonNode normalizeBigDecimalNodes(JsonNode node)
      throws IOException {
    if (node.isBigDecimal()) {
      return stringToJsonNode(node.toString());
    }
    if (node.isObject()) {
      ObjectNode result = JsonNodeFactory.instance.objectNode();
      Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        result.set(field.getKey(), normalizeBigDecimalNodes(field.getValue()));
      }
      return result;
    }
    if (node.isArray()) {
      ArrayNode result = JsonNodeFactory.instance.arrayNode(node.size());
      for (JsonNode child : node) {
        result.add(normalizeBigDecimalNodes(child));
      }
      return result;
    }
    return node;
  }

  /**
   * Generates the JsonSchemaTreeNode tree from the given json index config indexPaths to represent which path we
   * should flatten/index in the json record.
   * @param jsonIndexConfig
   * @return the root node of the json index paths tree
   * @throws IllegalArgumentException
   */
  private static JsonSchemaTreeNode createTree(JsonIndexConfig jsonIndexConfig)
      throws IllegalArgumentException {
    Set<String> indexPaths = jsonIndexConfig.getIndexPaths();
    JsonSchemaTreeNode rootNode = new JsonSchemaTreeNode("");
    // if no index paths are provided, return a global wildcard node
    if (indexPaths == null || indexPaths.isEmpty()) {
      rootNode.getAndCreateChild(GLOBAL_WILDCARD);
      return rootNode;
    }

    for (String indexPath : indexPaths) {
      String[] paths = StringUtils.splitPreserveAllTokens(indexPath, KEY_SEPARATOR);
      JsonSchemaTreeNode currentNode = rootNode;
      for (String key : paths) {
        currentNode = currentNode.getAndCreateChild(key);
        if (GLOBAL_WILDCARD.equals(key)) {
          break;
        }
      }
    }

    return rootNode;
  }
}

/**
 * JsonSchemaTreeNode represents the tree node when we construct the json schema tree.
 * This tree is used to represent how we want to flatten/index the json according to the {@link JsonIndexConfig}.
 * The node could be either leaf node or non-leaf node. Both types of node could hold the volume to indicate whether
 * we should flatten/index the json at this node.
 * For example, the config with *.*, a.b.*.*, a.b.x, a.b.c.**, a.b.d.*.*, e.f.g will have the following tree
 * structure:
 * root -- * -- *
 *      -- a -- b -- * -- *
 *                -- c -- **
 *                -- d -- * -- *
 *      -- e -- f -- g
 * The path structure is defined as:
 *  each key is separated by '.'
 *  the key without wildcard represents single value field
 *  the key "*" represents any types of single node
 *  the key "**" represents any types of leaf node OR a subtree with all its children
 * When multiple conditions are matched, e.g. a.b.c, it would match with priority:
 * 1. **
 * 2. exact match (either single value or array value)
 * 3. *
 */
class JsonSchemaTreeNode {
  private Map<String, JsonSchemaTreeNode> _children;
  private JsonSchemaTreeNode _gloabalWildcardChild;
  private JsonSchemaTreeNode _wildcardChild;
  private String _key;

  public JsonSchemaTreeNode(String key) {
    _key = key;
    _children = new HashMap<>();
  }

  /**
   * If does not have the child node, add a child node to the current node and return the child node.
   * If the child node already exists, return the existing child node.
   * @param key
   * @return child
   */
  public JsonSchemaTreeNode getAndCreateChild(String key) {
    // if .** is already added, no need to add any child
    if (_gloabalWildcardChild != null) {
      return _gloabalWildcardChild;
    }
    switch (key) {
      case JsonUtils.GLOBAL_WILDCARD:
        if (_gloabalWildcardChild == null) {
          _gloabalWildcardChild = new JsonSchemaTreeNode(key);
        }
        return _gloabalWildcardChild;
      case JsonUtils.WILDCARD:
        if (_wildcardChild == null) {
          _wildcardChild = new JsonSchemaTreeNode(key);
        }
        return _wildcardChild;
      default:
        JsonSchemaTreeNode child = _children.get(key);
        if (child == null) {
          child = new JsonSchemaTreeNode(key);
          _children.put(key, child);
        }
        return child;
    }
  }

  @Nullable
  public JsonSchemaTreeNode getChild(String key) {
    if (JsonUtils.GLOBAL_WILDCARD.equals(_key)) {
      return this;
    }
    if (_gloabalWildcardChild != null) {
      return _gloabalWildcardChild;
    }
    if (_children.containsKey(key)) {
      return _children.get(key);
    }
    if (_wildcardChild != null) {
      return _wildcardChild;
    }
    return null;
  }
}
