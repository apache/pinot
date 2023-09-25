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

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.AbstractRecordExtractorTest;
import org.apache.pinot.spi.data.readers.RecordReader;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Tests for the {@link ProtoBufRecordExtractor}
 */
public class ProtoBufRecordExtractorTest extends AbstractRecordExtractorTest {

  private final File _dataFile = new File(_tempDir, "test_complex_proto.data");

  private static final String DESCRIPTOR_FILE = "complex_types.desc";

  private static final String STRING_FIELD = "string_field";
  private static final String INT_FIELD = "int_field";
  private static final String LONG_FIELD = "long_field";
  private static final String DOUBLE_FIELD = "double_field";
  private static final String FLOAT_FIELD = "float_field";
  private static final String BOOL_FIELD = "bool_field";
  private static final String BYTES_FIELD = "bytes_field";
  private static final String NULLABLE_STRING_FIELD = "nullable_string_field";
  private static final String NULLABLE_INT_FIELD = "nullable_int_field";
  private static final String NULLABLE_LONG_FIELD = "nullable_long_field";
  private static final String NULLABLE_DOUBLE_FIELD = "nullable_double_field";
  private static final String NULLABLE_FLOAT_FIELD = "nullable_float_field";
  private static final String NULLABLE_BOOL_FIELD = "nullable_bool_field";
  private static final String NULLABLE_BYTES_FIELD = "nullable_bytes_field";
  private static final String REPEATED_STRINGS = "repeated_strings";
  private static final String NESTED_MESSAGE = "nested_message";
  private static final String REPEATED_NESTED_MESSAGES = "repeated_nested_messages";
  private static final String COMPLEX_MAP = "complex_map";
  private static final String SIMPLE_MAP = "simple_map";
  private static final String ENUM_FIELD = "enum_field";
  private static final String NESTED_INT_FIELD = "nested_int_field";
  private static final String NESTED_STRING_FIELD = "nested_string_field";

  @Override
  protected List<Map<String, Object>> getInputRecords() {
    return Arrays.asList(createRecord1(), createRecord2());
  }

  @Override
  protected Set<String> getSourceFields() {
    return Sets.newHashSet(STRING_FIELD, INT_FIELD, LONG_FIELD, DOUBLE_FIELD, FLOAT_FIELD, BOOL_FIELD, BYTES_FIELD,
        NULLABLE_STRING_FIELD, NULLABLE_INT_FIELD, NULLABLE_LONG_FIELD, NULLABLE_DOUBLE_FIELD, NULLABLE_FLOAT_FIELD,
        NULLABLE_BOOL_FIELD, NULLABLE_BYTES_FIELD, REPEATED_STRINGS, NESTED_MESSAGE, REPEATED_NESTED_MESSAGES,
        COMPLEX_MAP, SIMPLE_MAP, ENUM_FIELD);
  }

  /**
   * Creates a ProtoBufRecordReader
   */
  @Override
  protected RecordReader createRecordReader(Set<String> fieldsToRead)
      throws IOException {
    ProtoBufRecordReader protoBufRecordReader = new ProtoBufRecordReader();
    protoBufRecordReader.init(_dataFile, fieldsToRead, getProtoRecordReaderConfig());
    return protoBufRecordReader;
  }

  /**
   * Create a data input file using input records containing various ProtoBuf types
   */
  @Override
  protected void createInputFile()
      throws IOException {
    for (Map<String, Object> inputRecord : _inputRecords) {
      ComplexTypes.TestMessage.Builder messageBuilder = ComplexTypes.TestMessage.newBuilder();
      messageBuilder.setStringField((String) inputRecord.get(STRING_FIELD));
      messageBuilder.setIntField((Integer) inputRecord.get(INT_FIELD));
      messageBuilder.setLongField((Long) inputRecord.get(LONG_FIELD));
      messageBuilder.setDoubleField((Double) inputRecord.get(DOUBLE_FIELD));
      messageBuilder.setFloatField((Float) inputRecord.get(FLOAT_FIELD));
      messageBuilder.setBoolField(Boolean.parseBoolean((String) inputRecord.get(BOOL_FIELD)));
      messageBuilder.setBytesField(ByteString.copyFrom((byte[]) inputRecord.get(BYTES_FIELD)));

      if (inputRecord.get(NULLABLE_STRING_FIELD) != null) {
        messageBuilder.setNullableStringField((String) inputRecord.get(NULLABLE_STRING_FIELD));
      }
      if (inputRecord.get(NULLABLE_INT_FIELD) != null) {
        messageBuilder.setNullableIntField((Integer) inputRecord.get(NULLABLE_INT_FIELD));
      }
      if (inputRecord.get(NULLABLE_LONG_FIELD) != null) {
        messageBuilder.setNullableLongField((Long) inputRecord.get(NULLABLE_LONG_FIELD));
      }
      if (inputRecord.get(NULLABLE_DOUBLE_FIELD) != null) {
        messageBuilder.setNullableDoubleField((Double) inputRecord.get(NULLABLE_DOUBLE_FIELD));
      }
      if (inputRecord.get(NULLABLE_FLOAT_FIELD) != null) {
        messageBuilder.setNullableFloatField((Float) inputRecord.get(NULLABLE_FLOAT_FIELD));
      }
      if (inputRecord.get(NULLABLE_BOOL_FIELD) != null) {
        messageBuilder.setNullableBoolField(Boolean.parseBoolean((String) inputRecord.get(NULLABLE_BOOL_FIELD)));
      }
      if (inputRecord.get(NULLABLE_BYTES_FIELD) != null) {
        messageBuilder.setNullableBytesField(ByteString.copyFrom((byte[]) inputRecord.get(NULLABLE_BYTES_FIELD)));
      }

      messageBuilder.addAllRepeatedStrings((List) inputRecord.get(REPEATED_STRINGS));
      messageBuilder.setNestedMessage(createNestedMessage((Map<String, Object>) inputRecord.get(NESTED_MESSAGE)));

      List<Map<String, Object>> nestedMessagesValues =
          (List<Map<String, Object>>) inputRecord.get(REPEATED_NESTED_MESSAGES);
      for (Map<String, Object> nestedMessage : nestedMessagesValues) {
        messageBuilder.addRepeatedNestedMessages(createNestedMessage(nestedMessage));
      }

      Map<String, Map<String, Object>> complexMapValues =
          (Map<String, Map<String, Object>>) inputRecord.get(COMPLEX_MAP);
      for (Map.Entry<String, Map<String, Object>> mapEntry : complexMapValues.entrySet()) {
        messageBuilder.putComplexMap(mapEntry.getKey(), createNestedMessage(mapEntry.getValue()));
      }
      messageBuilder.putAllSimpleMap((Map<String, Integer>) inputRecord.get(SIMPLE_MAP));
      messageBuilder.setEnumField(ComplexTypes.TestMessage.TestEnum.valueOf((String) inputRecord.get(ENUM_FIELD)));

      try (FileOutputStream output = new FileOutputStream(_dataFile, true)) {
        messageBuilder.build().writeDelimitedTo(output);
      }
    }
  }

  private ComplexTypes.TestMessage.NestedMessage createNestedMessage(Map<String, Object> nestedMessageFields) {
    ComplexTypes.TestMessage.NestedMessage.Builder nestedMessage = ComplexTypes.TestMessage.NestedMessage.newBuilder();
    return nestedMessage.setNestedIntField((Integer) nestedMessageFields.get(NESTED_INT_FIELD))
        .setNestedStringField((String) nestedMessageFields.get(NESTED_STRING_FIELD)).build();
  }

  private Map<String, Object> createRecord1() {
    Map<String, Object> record = new HashMap<>();
    record.put(STRING_FIELD, "hello");
    record.put(INT_FIELD, 10);
    record.put(LONG_FIELD, 100L);
    record.put(DOUBLE_FIELD, 1.1);
    record.put(FLOAT_FIELD, 2.2f);
    record.put(BOOL_FIELD, "true");
    record.put(BYTES_FIELD, "hello world!".getBytes(UTF_8));
    record.put(NULLABLE_STRING_FIELD, "");
    record.put(NULLABLE_INT_FIELD, 0);
    record.put(NULLABLE_LONG_FIELD, 0L);
    record.put(NULLABLE_DOUBLE_FIELD, 0.0);
    record.put(NULLABLE_FLOAT_FIELD, 0.0f);
    record.put(NULLABLE_BOOL_FIELD, "false");
    record.put(NULLABLE_BYTES_FIELD, "".getBytes(UTF_8));
    record.put(REPEATED_STRINGS, Arrays.asList("aaa", "bbb", "ccc"));
    record.put(NESTED_MESSAGE, getNestedMap(NESTED_STRING_FIELD, "ice cream", NESTED_INT_FIELD, 9));
    record.put(REPEATED_NESTED_MESSAGES, Arrays
        .asList(getNestedMap(NESTED_STRING_FIELD, "vanilla", NESTED_INT_FIELD, 3),
            getNestedMap(NESTED_STRING_FIELD, "chocolate", NESTED_INT_FIELD, 5)));
    record.put(COMPLEX_MAP,
        getNestedMap("fruit1", getNestedMap(NESTED_STRING_FIELD, "apple", NESTED_INT_FIELD, 1), "fruit2",
            getNestedMap(NESTED_STRING_FIELD, "orange", NESTED_INT_FIELD, 2)));
    record.put(SIMPLE_MAP, getNestedMap("Tuesday", 3, "Wednesday", 4));
    record.put(ENUM_FIELD, "GAMMA");
    return record;
  }

  private Map<String, Object> createRecord2() {
    Map<String, Object> record = new HashMap<>();
    record.put(STRING_FIELD, "world");
    record.put(INT_FIELD, 20);
    record.put(LONG_FIELD, 200L);
    record.put(DOUBLE_FIELD, 3.3);
    record.put(FLOAT_FIELD, 4.4f);
    record.put(BOOL_FIELD, "true");
    record.put(BYTES_FIELD, "goodbye world!".getBytes(UTF_8));
    record.put(NULLABLE_STRING_FIELD, null);
    record.put(NULLABLE_INT_FIELD, null);
    record.put(NULLABLE_LONG_FIELD, null);
    record.put(NULLABLE_DOUBLE_FIELD, null);
    record.put(NULLABLE_FLOAT_FIELD, null);
    record.put(NULLABLE_BOOL_FIELD, null);
    record.put(NULLABLE_BYTES_FIELD, null);
    record.put(REPEATED_STRINGS, Arrays.asList("ddd", "eee", "fff"));
    record.put(NESTED_MESSAGE, getNestedMap(NESTED_STRING_FIELD, "Starbucks", NESTED_INT_FIELD, 100));
    record.put(REPEATED_NESTED_MESSAGES, Arrays
        .asList(getNestedMap(NESTED_STRING_FIELD, "coffee", NESTED_INT_FIELD, 10),
            getNestedMap(NESTED_STRING_FIELD, "tea", NESTED_INT_FIELD, 20)));
    record.put(COMPLEX_MAP,
        getNestedMap("food3", getNestedMap(NESTED_STRING_FIELD, "pizza", NESTED_INT_FIELD, 1), "food4",
            getNestedMap(NESTED_STRING_FIELD, "hamburger", NESTED_INT_FIELD, 2)));
    record.put(SIMPLE_MAP, getNestedMap("Sunday", 1, "Monday", 2));
    record.put(ENUM_FIELD, "BETA");
    return record;
  }

  private Map<String, Object> getNestedMap(String key1, Object value1, String key2, Object value2) {
    Map<String, Object> nestedMap = new HashMap<>(2);
    nestedMap.put(key1, value1);
    nestedMap.put(key2, value2);
    return nestedMap;
  }

  private ProtoBufRecordReaderConfig getProtoRecordReaderConfig()
      throws IOException {
    ProtoBufRecordReaderConfig config = new ProtoBufRecordReaderConfig();
    URI descriptorFile;
    try {
      descriptorFile = getClass().getClassLoader().getResource(DESCRIPTOR_FILE).toURI();
    } catch (URISyntaxException e) {
      throw new IOException("Could not load descriptor file: " + DESCRIPTOR_FILE, e.getCause());
    }
    config.setDescriptorFile(descriptorFile);
    return config;
  }
}
