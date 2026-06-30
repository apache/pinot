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

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.testng.annotations.Test;

import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Tests [ProtoBufRecordExtractor] — see its class Javadoc for the proto source type → Java output type
/// matrix. Each test builds a single-field [DynamicMessage] whose only field is named [#COLUMN] of the type
/// under test, so [#extract] only needs to take the message.
public class ProtoBufRecordExtractorTest {

  private static final String COLUMN = "col";

  // === Single-value primitives — order follows the proto type list in the class Javadoc ===

  @Test
  public void testBooleanPreserved() {
    Object result = extract(singleField(Type.TYPE_BOOL, true));
    assertEquals(result, true);
  }

  @Test
  public void testIntegerPreserved() {
    Object result = extract(singleField(Type.TYPE_INT32, 123));
    assertEquals(result, 123);
  }

  @Test
  public void testLongPreserved() {
    Object result = extract(singleField(Type.TYPE_INT64, 1_588_469_340_000L));
    assertEquals(result, 1_588_469_340_000L);
  }

  @Test
  public void testFloatPreserved() {
    Object result = extract(singleField(Type.TYPE_FLOAT, 0.5f));
    assertEquals(result, 0.5f);
  }

  @Test
  public void testDoublePreserved() {
    Object result = extract(singleField(Type.TYPE_DOUBLE, 0.5d));
    assertEquals(result, 0.5d);
  }

  @Test
  public void testStringPreserved() {
    Object result = extract(singleField(Type.TYPE_STRING, "hello"));
    assertEquals(result, "hello");
  }

  @Test
  public void testBytesConvertedToByteArray() {
    Object result = extract(singleField(Type.TYPE_BYTES, ByteString.copyFrom(new byte[]{0, 1, 2, 3})));
    assertEquals((byte[]) result, new byte[]{0, 1, 2, 3});
  }

  // === Enum → String ===

  @Test
  public void testEnumExtractedAsString() {
    EnumDescriptorProto enumProto = EnumDescriptorProto.newBuilder()
        .setName("Color")
        .addValue(EnumValueDescriptorProto.newBuilder().setName("RED").setNumber(0))
        .addValue(EnumValueDescriptorProto.newBuilder().setName("GREEN").setNumber(1))
        .addValue(EnumValueDescriptorProto.newBuilder().setName("BLUE").setNumber(2))
        .build();
    Descriptor desc = buildDescriptor(builder -> {
      builder.addEnumType(enumProto);
      builder.addField(fieldProto(Type.TYPE_ENUM, Label.LABEL_OPTIONAL).setTypeName(enumProto.getName()));
    });
    EnumValueDescriptor green = desc.findEnumTypeByName(enumProto.getName()).findValueByNumber(1);
    Message message = DynamicMessage.newBuilder(desc).setField(desc.findFieldByName(COLUMN), green).build();
    Object result = extract(message);
    assertEquals(result, "GREEN");
  }

  // === Repeated → Object[] ===

  @Test
  public void testRepeatedStringsExtractedAsArray() {
    Object[] result = (Object[]) extract(repeatedField(Type.TYPE_STRING, List.of("a", "b", "c")));
    assertEquals(result, new Object[]{"a", "b", "c"});
  }

  @Test
  public void testEmptyRepeatedExtractedAsEmptyArray() {
    Object[] result = (Object[]) extract(repeatedField(Type.TYPE_STRING, List.of()));
    assertEquals(result.length, 0);
  }

  // === Nested message → Map ===

  @Test
  public void testNestedMessageExtractedAsMap() {
    DescriptorProto inner = pairMessage("Inner", Type.TYPE_STRING, Type.TYPE_INT32);
    Descriptor outer = buildDescriptor(builder -> {
      builder.addNestedType(inner);
      builder.addField(fieldProto(Type.TYPE_MESSAGE, Label.LABEL_OPTIONAL).setTypeName(inner.getName()));
    });
    DynamicMessage innerMsg = DynamicMessage.newBuilder(outer.findNestedTypeByName(inner.getName()))
        .setField(outer.findNestedTypeByName(inner.getName()).findFieldByName("a"), "hello")
        .setField(outer.findNestedTypeByName(inner.getName()).findFieldByName("b"), 42)
        .build();
    Message message = DynamicMessage.newBuilder(outer).setField(outer.findFieldByName(COLUMN), innerMsg).build();
    Map<?, ?> result = (Map<?, ?>) extract(message);
    assertEquals(result.get("a"), "hello");
    assertEquals(result.get("b"), 42);
  }

  @Test
  public void testUnsetNestedMessageReturnsNull() {
    Descriptor outer = buildDescriptor(builder -> {
      DescriptorProto inner = pairMessage("Inner", Type.TYPE_STRING, Type.TYPE_INT32);
      builder.addNestedType(inner);
      builder.addField(fieldProto(Type.TYPE_MESSAGE, Label.LABEL_OPTIONAL).setTypeName(inner.getName()));
    });
    assertNull(extract(DynamicMessage.newBuilder(outer).build()));
  }

  // === Map (proto `map<K, V>`) ===

  @Test
  public void testMapStringToInt() {
    DescriptorProto entry = mapEntry(Type.TYPE_STRING, Type.TYPE_INT32);
    Descriptor outer = buildDescriptor(builder -> {
      builder.addNestedType(entry);
      builder.addField(fieldProto(Type.TYPE_MESSAGE, Label.LABEL_REPEATED).setTypeName(entry.getName()));
    });
    Descriptor entryDesc = outer.findNestedTypeByName(entry.getName());
    Message message = DynamicMessage.newBuilder(outer)
        .addRepeatedField(outer.findFieldByName(COLUMN), buildMapEntry(entryDesc, "a", 1))
        .addRepeatedField(outer.findFieldByName(COLUMN), buildMapEntry(entryDesc, "b", 2))
        .build();
    Map<?, ?> result = (Map<?, ?>) extract(message);
    assertEquals(result.size(), 2);
    assertEquals(result.get("a"), 1);
    assertEquals(result.get("b"), 2);
  }

  // === Field-presence semantics (proto3 `optional` keyword) ===

  @Test
  public void testNonOptionalFieldUnsetSurfacesTypeDefault() {
    // Non-optional int32 field unset returns the proto3 type default (0), not null.
    Descriptor desc = buildDescriptor(builder -> builder.addField(fieldProto(Type.TYPE_INT32, Label.LABEL_OPTIONAL)));
    assertEquals(extract(DynamicMessage.newBuilder(desc).build()), 0);
  }

  @Test
  public void testOptionalFieldUnsetReturnsNull() {
    // proto3 `optional` keyword (proto3_optional + synthetic oneof) tracks presence: unset → null.
    Descriptor desc = buildDescriptor(builder -> builder
        .addOneofDecl(com.google.protobuf.DescriptorProtos.OneofDescriptorProto.newBuilder().setName("_" + COLUMN))
        .addField(fieldProto(Type.TYPE_INT32, Label.LABEL_OPTIONAL).setProto3Optional(true).setOneofIndex(0)));
    assertNull(extract(DynamicMessage.newBuilder(desc).build()));
  }

  @Test
  public void testOptionalFieldExplicitDefaultIsPreserved() {
    // Setting an optional field to its type default (0) preserves it as 0, distinguishing from "unset".
    Descriptor desc = buildDescriptor(builder -> builder
        .addOneofDecl(com.google.protobuf.DescriptorProtos.OneofDescriptorProto.newBuilder().setName("_" + COLUMN))
        .addField(fieldProto(Type.TYPE_INT32, Label.LABEL_OPTIONAL).setProto3Optional(true).setOneofIndex(0)));
    Message message = DynamicMessage.newBuilder(desc).setField(desc.findFieldByName(COLUMN), 0).build();
    assertEquals(extract(message), 0);
  }

  // === Helpers — runtime proto descriptor / DynamicMessage construction ===

  private static Object extract(Message message) {
    ProtoBufRecordExtractor extractor = new ProtoBufRecordExtractor();
    extractor.init(null, new RecordExtractorConfig() {
    });
    GenericRow row = new GenericRow();
    extractor.extract(message, row);
    return row.getValue(COLUMN);
  }

  /// Build a single-field message of the given primitive type set to `value`.
  private static Message singleField(Type type, Object value) {
    Descriptor desc = buildDescriptor(builder -> builder.addField(fieldProto(type, Label.LABEL_OPTIONAL)));
    return DynamicMessage.newBuilder(desc).setField(desc.findFieldByName(COLUMN), value).build();
  }

  /// Build a single-repeated-field message of the given element type with all entries set.
  private static Message repeatedField(Type elementType, List<?> values) {
    Descriptor desc = buildDescriptor(builder -> builder.addField(fieldProto(elementType, Label.LABEL_REPEATED)));
    DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(desc);
    FieldDescriptor fd = desc.findFieldByName(COLUMN);
    for (Object value : values) {
      messageBuilder.addRepeatedField(fd, value);
    }
    return messageBuilder.build();
  }

  /// Build a nested message descriptor with two fields named `a` and `b` of the given types.
  private static DescriptorProto pairMessage(String name, Type aType, Type bType) {
    return DescriptorProto.newBuilder()
        .setName(name)
        .addField(named("a", 1, aType, Label.LABEL_OPTIONAL))
        .addField(named("b", 2, bType, Label.LABEL_OPTIONAL))
        .build();
  }

  /// Build a `map<K, V>` entry descriptor (a synthetic message with `key` and `value` fields and the
  /// `map_entry` option set, per the proto wire format for maps).
  private static DescriptorProto mapEntry(Type keyType, Type valueType) {
    return DescriptorProto.newBuilder()
        .setName("Entry")
        .setOptions(MessageOptions.newBuilder().setMapEntry(true))
        .addField(named("key", 1, keyType, Label.LABEL_OPTIONAL))
        .addField(named("value", 2, valueType, Label.LABEL_OPTIONAL))
        .build();
  }

  private static FieldDescriptorProto.Builder named(String name, int number, Type type, Label label) {
    return FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(type).setLabel(label);
  }

  private static DynamicMessage buildMapEntry(Descriptor entryDesc, Object key, Object value) {
    return DynamicMessage.newBuilder(entryDesc)
        .setField(entryDesc.findFieldByName("key"), key)
        .setField(entryDesc.findFieldByName("value"), value)
        .build();
  }

  /// Build the outer message [Descriptor] given a customizer for its [DescriptorProto.Builder]. The customizer
  /// adds whatever fields / nested types the test needs.
  private static Descriptor buildDescriptor(Consumer<DescriptorProto.Builder> customizer) {
    DescriptorProto.Builder messageBuilder = DescriptorProto.newBuilder().setName("Test");
    customizer.accept(messageBuilder);
    FileDescriptorProto fileProto = FileDescriptorProto.newBuilder()
        .setName("test.proto")
        .setSyntax("proto3")
        .addMessageType(messageBuilder.build())
        .build();
    try {
      FileDescriptor file = FileDescriptor.buildFrom(fileProto, new FileDescriptor[]{});
      return file.findMessageTypeByName("Test");
    } catch (DescriptorValidationException e) {
      throw new RuntimeException(e);
    }
  }

  /// Build a [FieldDescriptorProto.Builder] for the test's [#COLUMN] field with the given type / label.
  private static FieldDescriptorProto.Builder fieldProto(Type type, Label label) {
    return FieldDescriptorProto.newBuilder().setName(COLUMN).setNumber(1).setType(type).setLabel(label);
  }
}
