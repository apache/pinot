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
import com.google.protobuf.Descriptors;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.plugin.inputformat.protobuf.ProtoBufCodeGenMessageDecoder.PROTOBUF_JAR_FILE_PATH;
import static org.apache.pinot.plugin.inputformat.protobuf.ProtoBufCodeGenMessageDecoder.PROTO_CLASS_NAME;
import static org.apache.pinot.plugin.inputformat.protobuf.ProtoBufTestDataGenerator.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class ProtoBufCodeGenMessageDecoderTest {
  @Test
  public void testComplexClass()
      throws Exception {
    ProtoBufCodeGenMessageDecoder messageDecoder = setupDecoder("complex_types.jar",
        "org.apache.pinot.plugin.inputformat.protobuf.ComplexTypes$TestMessage", getSourceFieldsForComplexType());
    Map<String, Object> inputRecord = createComplexTypeRecord();
    GenericRow destination = new GenericRow();
    messageDecoder.decode(getComplexTypeObject(inputRecord).toByteArray(), destination);

    ComplexTypes.TestMessage msg = getComplexTypeObject(inputRecord);
    msg.getComplexMapMap().forEach((k, v) -> {
      v.getAllFields().forEach((fieldDescriptor, value) -> {
        assertEquals(
            ((Map<String, Object>) ((Map<String, Object>) destination.getValue("complex_map"))
                .get(k)).get(fieldDescriptor.getName()),
            value);
      });
    });
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
  public void testHappyCase()
      throws Exception {
    ProtoBufCodeGenMessageDecoder messageDecoder =
        setupDecoder("sample.jar", "org.apache.pinot.plugin.inputformat.protobuf.Sample$SampleRecord",
            getFieldsInSampleRecord());
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

  @Test
  public void testWithClassName()
      throws Exception {
    ProtoBufCodeGenMessageDecoder messageDecoder =
        setupDecoder("sample.jar", "org.apache.pinot.plugin.inputformat.protobuf.Sample$SampleRecord",
            getFieldsInSampleRecord());
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

  @Test
  public void testNestedMessageClass()
      throws Exception {
    ProtoBufCodeGenMessageDecoder messageDecoder = setupDecoder("complex_types.jar",
        "org.apache.pinot.plugin.inputformat.protobuf.ComplexTypes$TestMessage$NestedMessage",
        ImmutableSet.of(NESTED_STRING_FIELD, NESTED_INT_FIELD));

    ComplexTypes.TestMessage.NestedMessage nestedMessage =
        ComplexTypes.TestMessage.NestedMessage.newBuilder().setNestedStringField("hello").setNestedIntField(42).build();

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
    ProtoBufCodeGenMessageDecoder messageDecoder = setupDecoder("composite_types.jar",
        "org.apache.pinot.plugin.inputformat.protobuf.CompositeTypes$CompositeMessage",
        ImmutableSet.of("test_message", "sample_record"));
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

  /**
   * For each case, we have:
   * <ol>
   *   <li>The name of the field to change</li>
   *   <li>A valid protobuf value which is not the default field value</li>
   *   <li>The expected pinot value</li>
   * </ol>
   */
  @DataProvider(name = "normalCases")
  public Object[][] normalCases() {
    return new Object[][]{
        new Object[] {STRING_FIELD, "some text", "some text"},
        new Object[] {NULLABLE_STRING_FIELD, "some text", "some text"},

        new Object[] {INT_FIELD, 123, 123},
        new Object[] {NULLABLE_INT_FIELD, 123, 123},

        new Object[] {LONG_FIELD, 123L, 123L},
        new Object[] {NULLABLE_LONG_FIELD, 123L, 123L},

        new Object[] {DOUBLE_FIELD, 0.5d, 0.5d},
        new Object[] {NULLABLE_DOUBLE_FIELD, 0.5d, 0.5d},

        new Object[] {FLOAT_FIELD, 0.5f, 0.5f},
        new Object[] {NULLABLE_FLOAT_FIELD, 0.5f, 0.5f},

        new Object[] {BYTES_FIELD, ByteString.copyFrom(new byte[] {0, 1, 2, 3}), new byte[] {0, 1, 2, 3}},
        new Object[] {NULLABLE_BYTES_FIELD, ByteString.copyFrom(new byte[] {0, 1, 2, 3}), new byte[] {0, 1, 2, 3}},

        new Object[] {BOOL_FIELD, true, "true"},
        new Object[] {NULLABLE_BOOL_FIELD, true, "true"}
    };
  }

  /**
   * For each case, we have:
   * <ol>
   *   <li>The name of the field to change</li>
   *   <li>A valid protobuf value which <b>is</b> the default field value</li>
   *   <li>The expected pinot value</li>
   * </ol>
   */
  @Test(dataProvider = "normalCases")
  public void whenNormalCases(String fieldName, Object protoVal, Object pinotVal)
      throws Exception {
    Descriptors.FieldDescriptor fd = ComplexTypes.TestMessage.getDescriptor().findFieldByName(fieldName);
    ComplexTypes.TestMessage.Builder messageBuilder = ComplexTypes.TestMessage.newBuilder();
    messageBuilder.setField(fd, protoVal);

    GenericRow row = new GenericRow();
    ProtoBufCodeGenMessageDecoder messageDecoder = setupDecoder("complex_types.jar",
        "org.apache.pinot.plugin.inputformat.protobuf.ComplexTypes$TestMessage", getAllSourceFieldsForComplexType());
    messageDecoder.decode(messageBuilder.build().toByteArray(), row);
    Assert.assertEquals(row.getValue(fd.getName()), pinotVal);
  }

  /**
   * For each case, we have:
   * <ol>
   *   <li>The name of the field read</li>
   *   <li>The expected pinot value when the value is not set</li>
   * </ol>
   */
  @DataProvider(name = "defaultCases")
  public Object[][] defaultCases() {
    return new Object[][]{
        new Object[] {STRING_FIELD, "", ""},
        new Object[] {NULLABLE_STRING_FIELD, "", ""},

        new Object[] {INT_FIELD, 0, 0},
        new Object[] {NULLABLE_INT_FIELD, 0, 0},

        new Object[] {LONG_FIELD, 0L, 0L},
        new Object[] {NULLABLE_LONG_FIELD, 0L, 0L},

        new Object[] {DOUBLE_FIELD, 0d, 0d},
        new Object[] {NULLABLE_DOUBLE_FIELD, 0d, 0d},

        new Object[] {FLOAT_FIELD, 0f, 0f},
        new Object[] {NULLABLE_FLOAT_FIELD, 0f, 0f},

        new Object[] {BYTES_FIELD, ByteString.EMPTY, new byte[] {}},
        new Object[] {NULLABLE_BYTES_FIELD, ByteString.EMPTY, new byte[] {}},

        new Object[] {BOOL_FIELD, false, "false"},
        new Object[] {NULLABLE_BOOL_FIELD, false, "false"}
    };
  }

  @Test(dataProvider = "defaultCases")
  public void whenDefaultCases(String fieldName, Object protoValue, Object pinotVal)
      throws Exception {
    Descriptors.FieldDescriptor fd = ComplexTypes.TestMessage.getDescriptor().findFieldByName(fieldName);
    ComplexTypes.TestMessage.Builder messageBuilder = ComplexTypes.TestMessage.newBuilder();

    Assert.assertEquals(protoValue, fd.getDefaultValue());
    messageBuilder.setField(fd, protoValue);

    GenericRow row = new GenericRow();
    ProtoBufCodeGenMessageDecoder messageDecoder = setupDecoder("complex_types.jar",
        "org.apache.pinot.plugin.inputformat.protobuf.ComplexTypes$TestMessage", getAllSourceFieldsForComplexType());
    messageDecoder.decode(messageBuilder.build().toByteArray(), row);

    Assert.assertEquals(row.getValue(fd.getName()), pinotVal);
  }

  @DataProvider(name = "unsetCases")
  public Object[][] unsetCases() {
    return new Object[][]{
        new Object[] {STRING_FIELD, ""},
        new Object[] {NULLABLE_STRING_FIELD, null},

        new Object[] {INT_FIELD, 0},
        new Object[] {NULLABLE_INT_FIELD, null},

        new Object[] {LONG_FIELD, 0L},
        new Object[] {NULLABLE_LONG_FIELD, null},

        new Object[] {DOUBLE_FIELD, 0d},
        new Object[] {NULLABLE_DOUBLE_FIELD, null},

        new Object[] {FLOAT_FIELD, 0f},
        new Object[] {NULLABLE_FLOAT_FIELD, null},

        new Object[] {BYTES_FIELD, new byte[] {}},
        new Object[] {NULLABLE_BYTES_FIELD, null},

        new Object[] {BOOL_FIELD, "false"},
        new Object[] {NULLABLE_BOOL_FIELD, null}
    };
  }

  @Test(dataProvider = "unsetCases")
  public void whenUnset(String fieldName, Object pinotVal)
      throws Exception {
    Descriptors.FieldDescriptor fd = ComplexTypes.TestMessage.getDescriptor().findFieldByName(fieldName);
    ComplexTypes.TestMessage.Builder messageBuilder = ComplexTypes.TestMessage.newBuilder();

    GenericRow row = new GenericRow();
    ProtoBufCodeGenMessageDecoder messageDecoder = setupDecoder("complex_types.jar",
        "org.apache.pinot.plugin.inputformat.protobuf.ComplexTypes$TestMessage", getAllSourceFieldsForComplexType());
    messageDecoder.decode(messageBuilder.build().toByteArray(), row);

    Assert.assertEquals(row.getValue(fd.getName()), pinotVal);
  }

  @DataProvider(name = "clearCases")
  public Object[][] clearCases() {
    return new Object[][]{
        new Object[] {STRING_FIELD, ""},
        new Object[] {NULLABLE_STRING_FIELD, null},

        new Object[] {INT_FIELD, 0},
        new Object[] {NULLABLE_INT_FIELD, null},

        new Object[] {LONG_FIELD, 0L},
        new Object[] {NULLABLE_LONG_FIELD, null},

        new Object[] {DOUBLE_FIELD, 0d},
        new Object[] {NULLABLE_DOUBLE_FIELD, null},

        new Object[] {FLOAT_FIELD, 0f},
        new Object[] {NULLABLE_FLOAT_FIELD, null},

        new Object[] {BYTES_FIELD, new byte[] {}},
        new Object[] {NULLABLE_BYTES_FIELD, null},

        new Object[] {BOOL_FIELD, "false"},
        new Object[] {NULLABLE_BOOL_FIELD, null}
    };
  }

  @Test(dataProvider = "clearCases")
  public void whenClear(String fieldName, Object pinotVal)
      throws Exception {
    Descriptors.FieldDescriptor fd = ComplexTypes.TestMessage.getDescriptor().findFieldByName(fieldName);
    ComplexTypes.TestMessage.Builder messageBuilder = ComplexTypes.TestMessage.newBuilder();
    messageBuilder.clearField(fd);

    GenericRow row = new GenericRow();
    ProtoBufCodeGenMessageDecoder messageDecoder = setupDecoder("complex_types.jar",
        "org.apache.pinot.plugin.inputformat.protobuf.ComplexTypes$TestMessage", getAllSourceFieldsForComplexType());
    messageDecoder.decode(messageBuilder.build().toByteArray(), row);

    Assert.assertEquals(row.getValue(fd.getName()), pinotVal);
  }

  private static Set<String> getAllSourceFieldsForComplexType() {
    return Sets.newHashSet(STRING_FIELD, INT_FIELD, LONG_FIELD, DOUBLE_FIELD, FLOAT_FIELD, BOOL_FIELD, BYTES_FIELD,
        REPEATED_STRINGS, NESTED_MESSAGE, REPEATED_NESTED_MESSAGES, COMPLEX_MAP, SIMPLE_MAP, ENUM_FIELD,
        NULLABLE_STRING_FIELD, NULLABLE_INT_FIELD, NULLABLE_LONG_FIELD, NULLABLE_DOUBLE_FIELD, NULLABLE_FLOAT_FIELD,
        NULLABLE_BOOL_FIELD, NULLABLE_BYTES_FIELD);
  }

  private ProtoBufCodeGenMessageDecoder setupDecoder(String name, String value,
      Set<String> sourceFieldsForComplexType)
      throws Exception {
    Map<String, String> decoderProps = new HashMap<>();
    URL jarFIle = getClass().getClassLoader().getResource(name);
    decoderProps.put(PROTOBUF_JAR_FILE_PATH, jarFIle.toURI().toString());
    decoderProps.put(PROTO_CLASS_NAME, value);
    ProtoBufCodeGenMessageDecoder messageDecoder = new ProtoBufCodeGenMessageDecoder();
    messageDecoder.init(decoderProps, sourceFieldsForComplexType, "");
    return messageDecoder;
  }
}
