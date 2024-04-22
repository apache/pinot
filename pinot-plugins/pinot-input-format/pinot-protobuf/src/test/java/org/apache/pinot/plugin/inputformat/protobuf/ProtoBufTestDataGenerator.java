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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ProtoBufTestDataGenerator {

  private ProtoBufTestDataGenerator() {
  }

  public static final String STRING_FIELD = "string_field";
  public static final String INT_FIELD = "int_field";
  public static final String LONG_FIELD = "long_field";
  public static final String DOUBLE_FIELD = "double_field";
  public static final String FLOAT_FIELD = "float_field";
  public static final String BOOL_FIELD = "bool_field";
  public static final String BYTES_FIELD = "bytes_field";
  public static final String NESTED_MESSAGE = "nested_message";
  public static final String REPEATED_NESTED_MESSAGES = "repeated_nested_messages";
  public static final String COMPLEX_MAP = "complex_map";
  public static final String SIMPLE_MAP = "simple_map";
  public static final String ENUM_FIELD = "enum_field";
  public static final String NESTED_INT_FIELD = "nested_int_field";
  public static final String NESTED_STRING_FIELD = "nested_string_field";
  public static final String NULLABLE_STRING_FIELD = "nullable_string_field";
  public static final String NULLABLE_INT_FIELD = "nullable_int_field";
  public static final String NULLABLE_LONG_FIELD = "nullable_long_field";
  public static final String NULLABLE_DOUBLE_FIELD = "nullable_double_field";
  public static final String NULLABLE_FLOAT_FIELD = "nullable_float_field";
  public static final String NULLABLE_BOOL_FIELD = "nullable_bool_field";
  public static final String NULLABLE_BYTES_FIELD = "nullable_bytes_field";

  public static final String REPEATED_STRINGS = "repeated_strings";
  public static final String REPEATED_INTS = "repeated_ints";
  public static final String REPEATED_LONGS = "repeated_longs";
  public static final String REPEATED_DOUBLES = "repeated_doubles";
  public static final String REPEATED_FLOATS = "repeated_floats";
  public static final String REPEATED_BOOLS = "repeated_bools";
  public static final String REPEATED_BYTES = "repeated_bytes";
  public static final String REPEATED_ENUMS = "repeated_enums";

  public static ComplexTypes.TestMessage getComplexTypeObject(Map<String, Object> inputRecord)
      throws IOException {
    ComplexTypes.TestMessage.Builder messageBuilder = ComplexTypes.TestMessage.newBuilder();
    messageBuilder.setStringField((String) inputRecord.get(STRING_FIELD));
    messageBuilder.setIntField((Integer) inputRecord.get(INT_FIELD));
    messageBuilder.setLongField((Long) inputRecord.get(LONG_FIELD));
    messageBuilder.setDoubleField((Double) inputRecord.get(DOUBLE_FIELD));
    messageBuilder.setFloatField((Float) inputRecord.get(FLOAT_FIELD));
    messageBuilder.setBoolField(Boolean.parseBoolean((String) inputRecord.get(BOOL_FIELD)));
    messageBuilder.setBytesField(ByteString.copyFrom((byte[]) inputRecord.get(BYTES_FIELD)));
    messageBuilder.addAllRepeatedStrings((List) inputRecord.get(REPEATED_STRINGS));
    messageBuilder.setNestedMessage(createNestedMessage((Map<String, Object>) inputRecord.get(NESTED_MESSAGE)));

    List<Map<String, Object>> nestedMessagesValues =
        (List<Map<String, Object>>) inputRecord.get(REPEATED_NESTED_MESSAGES);
    for (Map<String, Object> nestedMessage : nestedMessagesValues) {
      messageBuilder.addRepeatedNestedMessages(createNestedMessage(nestedMessage));
    }

    Map<String, Map<String, Object>> complexMapValues = (Map<String, Map<String, Object>>) inputRecord.get(COMPLEX_MAP);
    for (Map.Entry<String, Map<String, Object>> mapEntry : complexMapValues.entrySet()) {
      messageBuilder.putComplexMap(mapEntry.getKey(), createNestedMessage(mapEntry.getValue()));
    }
    messageBuilder.putAllSimpleMap((Map<String, Integer>) inputRecord.get(SIMPLE_MAP));
    messageBuilder.setEnumField(ComplexTypes.TestMessage.TestEnum.valueOf((String) inputRecord.get(ENUM_FIELD)));

    return messageBuilder.build();
  }

  public static ComplexTypes.TestMessage.NestedMessage createNestedMessage(Map<String, Object> nestedMessageFields) {
    ComplexTypes.TestMessage.NestedMessage.Builder nestedMessage = ComplexTypes.TestMessage.NestedMessage.newBuilder();
    return nestedMessage.setNestedIntField((Integer) nestedMessageFields.get(NESTED_INT_FIELD))
        .setNestedStringField((String) nestedMessageFields.get(NESTED_STRING_FIELD)).build();
  }

  public static Map<String, Object> createComplexTypeRecord() {
    Map<String, Object> record = new HashMap<>();
    record.put(STRING_FIELD, "hello");
    record.put(INT_FIELD, 10);
    record.put(LONG_FIELD, 100L);
    record.put(DOUBLE_FIELD, 1.1);
    record.put(FLOAT_FIELD, 2.2f);
    record.put(BOOL_FIELD, "true");
    record.put(BYTES_FIELD, "hello world!".getBytes(UTF_8));
    record.put(REPEATED_STRINGS, Arrays.asList("aaa", "bbb", "ccc"));
    record.put(NESTED_MESSAGE, getNestedMap(NESTED_STRING_FIELD, "ice cream", NESTED_INT_FIELD, 9));
    record.put(REPEATED_NESTED_MESSAGES,
        Arrays.asList(getNestedMap(NESTED_STRING_FIELD, "vanilla", NESTED_INT_FIELD, 3),
            getNestedMap(NESTED_STRING_FIELD, "chocolate", NESTED_INT_FIELD, 5)));
    record.put(COMPLEX_MAP,
        getNestedMap("fruit1", getNestedMap(NESTED_STRING_FIELD, "apple", NESTED_INT_FIELD, 1), "fruit2",
            getNestedMap(NESTED_STRING_FIELD, "orange", NESTED_INT_FIELD, 2)));
    record.put(SIMPLE_MAP, getNestedMap("Tuesday", 3, "Wednesday", 4));
    record.put(ENUM_FIELD, "GAMMA");
    return record;
  }

  public static Map<String, Object> getNestedMap(String key1, Object value1, String key2, Object value2) {
    Map<String, Object> nestedMap = new HashMap<>(2);
    nestedMap.put(key1, value1);
    nestedMap.put(key2, value2);
    return nestedMap;
  }

  public static Set<String> getSourceFieldsForComplexType() {
    return Sets.newHashSet(STRING_FIELD, INT_FIELD, LONG_FIELD, DOUBLE_FIELD, FLOAT_FIELD, BOOL_FIELD, BYTES_FIELD,
        REPEATED_STRINGS, NESTED_MESSAGE, REPEATED_NESTED_MESSAGES, COMPLEX_MAP, SIMPLE_MAP, ENUM_FIELD);
  }

  public static ImmutableSet<String> getFieldsInSampleRecord() {
    return ImmutableSet.of("email", "name", "id", "friends");
  }

  public static Sample.SampleRecord getSampleRecordMessage() {
    return Sample.SampleRecord.newBuilder().setEmail("foobar@hello.com").setName("Alice").setId(18).addFriends("Eve")
        .addFriends("Adam").build();
  }
}
