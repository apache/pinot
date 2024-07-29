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
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.apache.pinot.plugin.inputformat.protobuf.ProtoBufTestDataGenerator.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class ProtoBufMessageDecoderTest {

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
}
