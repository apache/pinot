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
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class ProtoBufMessageDecoderTest {

  private static final String STRING_FIELD = "string_field";
  private static final String INT_FIELD = "int_field";
  private static final String LONG_FIELD = "long_field";
  private static final String DOUBLE_FIELD = "double_field";
  private static final String FLOAT_FIELD = "float_field";
  private static final String BOOL_FIELD = "bool_field";
  private static final String BYTES_FIELD = "bytes_field";
  private static final String REPEATED_STRINGS = "repeated_strings";
  private static final String NESTED_MESSAGE = "nested_message";
  private static final String REPEATED_NESTED_MESSAGES = "repeated_nested_messages";
  private static final String COMPLEX_MAP = "complex_map";
  private static final String SIMPLE_MAP = "simple_map";
  private static final String ENUM_FIELD = "enum_field";
  private static final String NESTED_INT_FIELD = "nested_int_field";
  private static final String NESTED_STRING_FIELD = "nested_string_field";

  @Test
  public void testHappyCase()
      throws Exception {
    Map<String, String> decoderProps = new HashMap<>();
    URL descriptorFile = getClass().getClassLoader().getResource("sample.desc");
    decoderProps.put("descriptorFile", descriptorFile.toURI().toString());
    ProtoBufMessageDecoder messageDecoder = new ProtoBufMessageDecoder();
    messageDecoder.init(decoderProps, getFieldsInSampleRecord(), "");
    Sample.SampleRecord sampleRecord = getSampleRecordMessage();
    GenericRow destination = new GenericRow();
    messageDecoder.decode(sampleRecord.toByteArray(), destination);
    assertNotNull(destination.getValue("email"));
    assertNotNull(destination.getValue("name"));
    assertNotNull(destination.getValue("id"));

    assertEquals(destination.getValue("email"), "foobar@hello.com");
    assertEquals(destination.getValue("name"), "Alice");
    assertEquals(destination.getValue("id"), 18);
  }

  private ImmutableSet<String> getFieldsInSampleRecord() {
    return ImmutableSet.of("email", "name", "id", "friends");
  }

  @Test
  public void testWithClassName()
      throws Exception {
    Map<String, String> decoderProps = new HashMap<>();
    URL descriptorFile = getClass().getClassLoader().getResource("sample.desc");
    decoderProps.put("descriptorFile", descriptorFile.toURI().toString());
    decoderProps.put("protoClassName", "SampleRecord");
    ProtoBufMessageDecoder messageDecoder = new ProtoBufMessageDecoder();
    messageDecoder.init(decoderProps, getFieldsInSampleRecord(), "");
    Sample.SampleRecord sampleRecord = getSampleRecordMessage();
    GenericRow destination = new GenericRow();
    messageDecoder.decode(sampleRecord.toByteArray(), destination);
    assertNotNull(destination.getValue("email"));
    assertNotNull(destination.getValue("name"));
    assertNotNull(destination.getValue("id"));

    assertEquals(destination.getValue("email"), "foobar@hello.com");
    assertEquals(destination.getValue("name"), "Alice");
    assertEquals(destination.getValue("id"), 18);
  }

  private Sample.SampleRecord getSampleRecordMessage() {
    return Sample.SampleRecord.newBuilder().setEmail("foobar@hello.com").setName("Alice").setId(18).addFriends("Eve")
        .addFriends("Adam").build();
  }

  @Test
  public void testComplexClass()
      throws Exception {
    Map<String, String> decoderProps = new HashMap<>();
    URL descriptorFile = getClass().getClassLoader().getResource("complex_types.desc");
    decoderProps.put("descriptorFile", descriptorFile.toURI().toString());
    ProtoBufMessageDecoder messageDecoder = new ProtoBufMessageDecoder();
    messageDecoder.init(decoderProps, getSourceFieldsForComplexType(), "");
    Map<String, Object> inputRecord = createComplexTypeRecord();
    GenericRow destination = new GenericRow();
    messageDecoder.decode(getComplexTypeObject(inputRecord).toByteArray(), destination);

    for (String col : getSourceFieldsForComplexType()) {
      assertNotNull(destination.getValue(col));
    }

    for (String col : getSourceFieldsForComplexType()) {
      if (col.contains("field")) {
        assertEquals(destination.getValue(col), inputRecord.get(col));
      }
    }
  }

  @Test
  public void testNestedMessageClass()
      throws Exception {
    Map<String, String> decoderProps = new HashMap<>();
    URL descriptorFile = getClass().getClassLoader().getResource("complex_types.desc");
    decoderProps.put("descriptorFile", descriptorFile.toURI().toString());
    decoderProps.put("protoClassName", "TestMessage.NestedMessage");
    ProtoBufMessageDecoder messageDecoder = new ProtoBufMessageDecoder();

    ComplexTypes.TestMessage.NestedMessage nestedMessage =
        ComplexTypes.TestMessage.NestedMessage.newBuilder().setNestedStringField("hello").setNestedIntField(42).build();

    messageDecoder.init(decoderProps, ImmutableSet.of(NESTED_STRING_FIELD, NESTED_INT_FIELD), "");
    GenericRow destination = new GenericRow();
    messageDecoder.decode(nestedMessage.toByteArray(), destination);

    assertNotNull(destination.getValue(NESTED_STRING_FIELD));
    assertNotNull(destination.getValue(NESTED_INT_FIELD));

    assertEquals(destination.getValue(NESTED_STRING_FIELD), "hello");
    assertEquals(destination.getValue(NESTED_INT_FIELD), 42);
  }

  @Test
  public void testCompositeMessage()
      throws Exception {
    Map<String, String> decoderProps = new HashMap<>();
    URL descriptorFile = getClass().getClassLoader().getResource("composite_types.desc");
    decoderProps.put("descriptorFile", descriptorFile.toURI().toString());
    ProtoBufMessageDecoder messageDecoder = new ProtoBufMessageDecoder();
    Set<String> sourceFields = getSourceFieldsForComplexType();
    sourceFields.addAll(getFieldsInSampleRecord());

    messageDecoder.init(decoderProps, ImmutableSet.of("test_message", "sample_record"), "");
    Map<String, Object> inputRecord = createComplexTypeRecord();
    GenericRow destination = new GenericRow();
    ComplexTypes.TestMessage testMessage = getComplexTypeObject(inputRecord);
    Sample.SampleRecord sampleRecord = getSampleRecordMessage();
    CompositeTypes.CompositeMessage compositeMessage =
        CompositeTypes.CompositeMessage.newBuilder().setTestMessage(testMessage).setSampleRecord(sampleRecord).build();
    messageDecoder.decode(compositeMessage.toByteArray(), destination);

    assertNotNull(destination.getValue("test_message"));

    for (String col : getSourceFieldsForComplexType()) {
      assertNotNull(((Map<String, Object>) destination.getValue("test_message")).get(col));
    }

    assertNotNull(((Map<String, Object>) destination.getValue("sample_record")).get("email"));
    assertNotNull(((Map<String, Object>) destination.getValue("sample_record")).get("name"));
    assertNotNull(((Map<String, Object>) destination.getValue("sample_record")).get("id"));

    assertEquals(((Map<String, Object>) destination.getValue("sample_record")).get("email"), "foobar@hello.com");
    assertEquals(((Map<String, Object>) destination.getValue("sample_record")).get("name"), "Alice");
    assertEquals(((Map<String, Object>) destination.getValue("sample_record")).get("id"), 18);
  }

  protected ComplexTypes.TestMessage getComplexTypeObject(Map<String, Object> inputRecord)
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

  private ComplexTypes.TestMessage.NestedMessage createNestedMessage(Map<String, Object> nestedMessageFields) {
    ComplexTypes.TestMessage.NestedMessage.Builder nestedMessage = ComplexTypes.TestMessage.NestedMessage.newBuilder();
    return nestedMessage.setNestedIntField((Integer) nestedMessageFields.get(NESTED_INT_FIELD))
        .setNestedStringField((String) nestedMessageFields.get(NESTED_STRING_FIELD)).build();
  }

  private Map<String, Object> createComplexTypeRecord() {
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

  private Map<String, Object> getNestedMap(String key1, Object value1, String key2, Object value2) {
    Map<String, Object> nestedMap = new HashMap<>(2);
    nestedMap.put(key1, value1);
    nestedMap.put(key2, value2);
    return nestedMap;
  }

  private Set<String> getSourceFieldsForComplexType() {
    return Sets.newHashSet(STRING_FIELD, INT_FIELD, LONG_FIELD, DOUBLE_FIELD, FLOAT_FIELD, BOOL_FIELD, BYTES_FIELD,
        REPEATED_STRINGS, NESTED_MESSAGE, REPEATED_NESTED_MESSAGES, COMPLEX_MAP, SIMPLE_MAP, ENUM_FIELD);
  }
}
