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
package org.apache.pinot.plugin.inputformat.protobuf.codegen;

import com.google.protobuf.Descriptors;
import com.google.protobuf.ProtobufInternalUtils;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.pinot.plugin.inputformat.protobuf.ComplexTypes;
import org.apache.pinot.plugin.inputformat.protobuf.CompositeTypes;
import org.apache.pinot.plugin.inputformat.protobuf.ProtoBufUtils;
import org.apache.pinot.plugin.inputformat.protobuf.Sample;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.plugin.inputformat.protobuf.ProtoBufTestDataGenerator.*;
import static org.testng.Assert.assertEquals;

public class MessageCodeGenTest {
    @Test
    public void testCompleteLine() {
        MessageCodeGen messageCodeGen = new MessageCodeGen();
        assertEquals(messageCodeGen.completeLine("add indentation", 3), "      add indentation;\n");
        assertEquals(messageCodeGen.completeLine("add indentation", 0), "add indentation;\n");
    }

    @Test
    public void testAddIndent() {
        MessageCodeGen messageCodeGen = new MessageCodeGen();
        assertEquals(messageCodeGen.addIndent("add indentation", 3), "      add indentation\n");
        assertEquals(messageCodeGen.addIndent("add indentation", 0), "add indentation\n");
    }

    @Test
    public void testAddImports() {
        MessageCodeGen messageCodeGen = new MessageCodeGen();
        ArrayList<String> inputs = new ArrayList<>();
        inputs.add("com.pinot.MessageCodeGen");
        inputs.add("com.pinot.Test");
        assertEquals(
            messageCodeGen.addImports(inputs), "import com.pinot.MessageCodeGen;\n" + "import com.pinot.Test;\n");
    }

    @Test
    public void testPutFieldInMsgMapCode() {
        MessageCodeGen messageCodeGen = new MessageCodeGen();
        assertEquals(
            messageCodeGen.putFieldInMsgMapCode("field1", "getField1", null, null),
            "msgMap.put(\"field1\", msg.getField1())");
        assertEquals(
            messageCodeGen.putFieldInMsgMapCode("field1", "getField1", "", ""),
            "msgMap.put(\"field1\", msg.getField1())");
        assertEquals(
            messageCodeGen.putFieldInMsgMapCode("field1", "getField1", "decodeField1", null),
            "msgMap.put(\"field1\", decodeField1(msg.getField1()))");
        assertEquals(
            messageCodeGen.putFieldInMsgMapCode("field1", "getField1", "decodeField1",
                ""), "msgMap.put(\"field1\", decodeField1(msg.getField1()))");
        assertEquals(
            messageCodeGen.putFieldInMsgMapCode("field1", "getField1", null, ".toByteArray()"),
            "msgMap.put(\"field1\", msg.getField1().toByteArray())");
        assertEquals(
            messageCodeGen.putFieldInMsgMapCode("field1", "getField1", "", ".toByteArray()"),
            "msgMap.put(\"field1\", msg.getField1().toByteArray())");
        assertEquals(
            messageCodeGen.putFieldInMsgMapCode("field1", "getField1", "decodeField1", ".toString()"),
            "msgMap.put(\"field1\", decodeField1(msg.getField1().toString()))");
    }

    @DataProvider(name = "scalarFields")
    public Object[][] scalarFields() {
        return new Object[][]{
            new Object[] {STRING_FIELD, "  msgMap.put(\"string_field\", msg.getStringField());\n"},
            new Object[] {NULLABLE_STRING_FIELD, "  if (msg.hasNullableStringField()) {\n"
                + "    msgMap.put(\"nullable_string_field\", msg.getNullableStringField());\n" + "  }\n"},
            new Object[] {REPEATED_STRINGS, "  if (msg.getRepeatedStringsCount() > 0) {\n"
                + "    msgMap.put(\"repeated_strings\", msg.getRepeatedStringsList().toArray());\n" + "  }\n"},


            new Object[] {INT_FIELD, "  msgMap.put(\"int_field\", msg.getIntField());\n"},
            new Object[] {NULLABLE_INT_FIELD, "  if (msg.hasNullableIntField()) {\n"
                + "    msgMap.put(\"nullable_int_field\", msg.getNullableIntField());\n" + "  }\n"},
            new Object[] {REPEATED_INTS, "  if (msg.getRepeatedIntsCount() > 0) {\n"
                + "    msgMap.put(\"repeated_ints\", msg.getRepeatedIntsList().toArray());\n" + "  }\n"},

            new Object[] {LONG_FIELD, "  msgMap.put(\"long_field\", msg.getLongField());\n"},
            new Object[] {NULLABLE_LONG_FIELD, "  if (msg.hasNullableLongField()) {\n"
                + "    msgMap.put(\"nullable_long_field\", msg.getNullableLongField());\n" + "  }\n"},
            new Object[] {REPEATED_LONGS, "  if (msg.getRepeatedLongsCount() > 0) {\n"
                + "    msgMap.put(\"repeated_longs\", msg.getRepeatedLongsList().toArray());\n" + "  }\n"},

            new Object[] {DOUBLE_FIELD, "  msgMap.put(\"double_field\", msg.getDoubleField());\n"},
            new Object[] {NULLABLE_DOUBLE_FIELD, "  if (msg.hasNullableDoubleField()) {\n"
                + "    msgMap.put(\"nullable_double_field\", msg.getNullableDoubleField());\n" + "  }\n"},
            new Object[] {REPEATED_DOUBLES, "  if (msg.getRepeatedDoublesCount() > 0) {\n"
                + "    msgMap.put(\"repeated_doubles\", msg.getRepeatedDoublesList().toArray());\n" + "  }\n"},

            new Object[] {FLOAT_FIELD, "  msgMap.put(\"float_field\", msg.getFloatField());\n"},
            new Object[] {NULLABLE_FLOAT_FIELD, "  if (msg.hasNullableFloatField()) {\n"
                + "    msgMap.put(\"nullable_float_field\", msg.getNullableFloatField());\n" + "  }\n"},
            new Object[] {REPEATED_FLOATS, "  if (msg.getRepeatedFloatsCount() > 0) {\n"
                + "    msgMap.put(\"repeated_floats\", msg.getRepeatedFloatsList().toArray());\n" + "  }\n"},
            new Object[] {"does_not_exist_in_desc", ""},
        };
    }

    @Test
    public void testCodeForScalarFieldExtraction() {
        MessageCodeGen messageCodeGen = new MessageCodeGen();
        // Simple field
            Descriptors.FieldDescriptor fd = ComplexTypes.TestMessage.getDescriptor().findFieldByName(STRING_FIELD);
        String fieldNameInCode = ProtobufInternalUtils.underScoreToCamelCase(fd.getName(), true);
        assertEquals(messageCodeGen.codeForScalarFieldExtraction(fd, fieldNameInCode, 1).toString(),
            "  msgMap.put(\"string_field\", msg.getStringField());\n");

        // Nullable field Or Has Presence
        fd = ComplexTypes.TestMessage.getDescriptor().findFieldByName(NULLABLE_STRING_FIELD);
        fieldNameInCode = ProtobufInternalUtils.underScoreToCamelCase(fd.getName(), true);
        assertEquals(messageCodeGen.codeForScalarFieldExtraction(fd, fieldNameInCode, 1).toString(),
            "  if (msg.hasNullableStringField()) {\n"
                + "    msgMap.put(\"nullable_string_field\", msg.getNullableStringField());\n" + "  }\n");

        // Repeated field
        fd = ComplexTypes.TestMessage.getDescriptor().findFieldByName(REPEATED_STRINGS);
        fieldNameInCode = ProtobufInternalUtils.underScoreToCamelCase(fd.getName(), true);
        assertEquals(messageCodeGen.codeForScalarFieldExtraction(fd, fieldNameInCode, 1).toString(),
            "  if (msg.getRepeatedStringsCount() > 0) {\n"
                + "    msgMap.put(\"repeated_strings\", msg.getRepeatedStringsList().toArray());\n" + "  }\n");
    }

    @Test
    public void testCodeForComplexFieldExtractionNonRepeated() {
        MessageCodeGen messageCodeGen = new MessageCodeGen();
        // Message type
        Descriptors.FieldDescriptor fd = ComplexTypes.TestMessage.getDescriptor().findFieldByName(NESTED_MESSAGE);
        String fieldNameInCode = ProtobufInternalUtils.underScoreToCamelCase(fd.getName(), true);
        String javaType = ProtoBufUtils.getFullJavaName(fd.getMessageType());
        assertEquals(messageCodeGen.codeForComplexFieldExtraction(
            fd, fieldNameInCode, javaType, 1, 1, "decodeNestedMessage", "").toString(),
            "  if (msg.hasNestedMessage()) {\n"
                + "    msgMap.put(\"nested_message\", decodeNestedMessage(msg.getNestedMessage()));\n" + "  }\n");
        assertEquals(messageCodeGen.codeForComplexFieldExtraction(
            fd, fieldNameInCode, javaType, 1, 1, "decodeNestedMessage", null).toString(),
            "  if (msg.hasNestedMessage()) {\n"
                + "    msgMap.put(\"nested_message\", decodeNestedMessage(msg.getNestedMessage()));\n" + "  }\n");

        assertEquals(messageCodeGen.codeForComplexFieldExtraction(
            fd, fieldNameInCode, javaType, 1, 1, "decodeNestedMessage", ".toString()").toString(),
            "  if (msg.hasNestedMessage()) {\n"
                + "    msgMap.put(\"nested_message\", decodeNestedMessage(msg.getNestedMessage().toString()));\n"
                + "  }\n");

        // Complex type with no presence i.e Scalar type with additional Extractions eg bytes, enum, bool
        fd = ComplexTypes.TestMessage.getDescriptor().findFieldByName(BOOL_FIELD);
        fieldNameInCode = ProtobufInternalUtils.underScoreToCamelCase(fd.getName(), true);
        javaType = "String";
        assertEquals(messageCodeGen.codeForComplexFieldExtraction(
            fd, fieldNameInCode, javaType, 1, 1, "String.valueOf", "").toString(),
            "  msgMap.put(\"bool_field\", String.valueOf(msg.getBoolField()));\n");
        assertEquals(messageCodeGen.codeForComplexFieldExtraction(
            fd, fieldNameInCode, javaType, 1, 1, "String.valueOf", null).toString(),
            "  msgMap.put(\"bool_field\", String.valueOf(msg.getBoolField()));\n");

        fd = ComplexTypes.TestMessage.getDescriptor().findFieldByName(ENUM_FIELD);
        fieldNameInCode = ProtobufInternalUtils.underScoreToCamelCase(fd.getName(), true);
        javaType = "String";
        assertEquals(messageCodeGen.codeForComplexFieldExtraction(
            fd, fieldNameInCode, javaType, 1, 1, "", ".name()").toString(),
            "  msgMap.put(\"enum_field\", msg.getEnumField().name());\n");
        assertEquals(messageCodeGen.codeForComplexFieldExtraction(
            fd, fieldNameInCode, javaType, 1, 1, null, ".name()").toString(),
            "  msgMap.put(\"enum_field\", msg.getEnumField().name());\n");

        assertEquals(messageCodeGen.codeForComplexFieldExtraction(
            fd, fieldNameInCode, javaType, 1, 1, "String.valueOf", ".name()").toString(),
            "  msgMap.put(\"enum_field\", String.valueOf(msg.getEnumField().name()));\n");
    }

    @Test
    public void testCodeForComplexFieldExtractionRepeated() {
        MessageCodeGen messageCodeGen = new MessageCodeGen();
        // Repeated decoder method is non-empty
        Descriptors.FieldDescriptor fd = ComplexTypes.TestMessage.getDescriptor()
            .findFieldByName(REPEATED_NESTED_MESSAGES);
        String fieldNameInCode = ProtobufInternalUtils.underScoreToCamelCase(fd.getName(), true);
        String javaType = ProtoBufUtils.getFullJavaName(fd.getMessageType());
        String expectedCode = "  List<Object> list2 = new ArrayList<>();\n"
            + "  for (org.apache.pinot.plugin.inputformat.protobuf.ComplexTypes.TestMessage.NestedMessage "
            + "row: msg.getRepeatedNestedMessagesList()) {\n"
            + "    list2.add(decodeNestedMessage(row));\n"
            + "  }\n"
            + "  if (!list2.isEmpty()) {\n"
            + "    msgMap.put(\"repeated_nested_messages\", list2.toArray());\n"
            + "  }\n";
        assertEquals(messageCodeGen.codeForComplexFieldExtraction(fd, fieldNameInCode, javaType, 1, 1,
                "decodeNestedMessage", "").toString(), expectedCode);
        assertEquals(messageCodeGen.codeForComplexFieldExtraction(fd, fieldNameInCode, javaType, 1, 1,
                "decodeNestedMessage", null).toString(), expectedCode);

        // Repeated both decoder methods and additional extractions present
        expectedCode = "  List<Object> list2 = new ArrayList<>();\n"
            + "  for (org.apache.pinot.plugin.inputformat.protobuf.ComplexTypes.TestMessage.NestedMessage "
            + "row: msg.getRepeatedNestedMessagesList()) {\n"
            + "    list2.add(decodeNestedMessage(row.toString()));\n"
            + "  }\n"
            + "  if (!list2.isEmpty()) {\n"
            + "    msgMap.put(\"repeated_nested_messages\", list2.toArray());\n"
            + "  }\n";
        assertEquals(messageCodeGen.codeForComplexFieldExtraction(fd, fieldNameInCode, javaType, 1, 1,
            "decodeNestedMessage", ".toString()").toString(), expectedCode);


        // Repeated decoder method is non empty
        fd = ComplexTypes.TestMessage.getDescriptor().findFieldByName(REPEATED_BYTES);
        fieldNameInCode = ProtobufInternalUtils.underScoreToCamelCase(fd.getName(), true);
        javaType = "String";
        expectedCode = "  List<Object> list2 = new ArrayList<>();\n"
            + "  for (String row: msg.getRepeatedBytesList()) {\n"
            + "    list2.add(row.name());\n"
            + "  }\n"
            + "  if (!list2.isEmpty()) {\n"
            + "    msgMap.put(\"repeated_bytes\", list2.toArray());\n"
            + "  }\n";
        assertEquals(messageCodeGen.codeForComplexFieldExtraction(fd, fieldNameInCode, javaType, 1, 1,
                "", ".name()").toString(), expectedCode);
        assertEquals(messageCodeGen.codeForComplexFieldExtraction(fd, fieldNameInCode, javaType, 1, 1,
                null, ".name()").toString(), expectedCode);
    }

    @Test
    public void testCodeForMapWithValueMessageType() {
        MessageCodeGen messageCodeGen = new MessageCodeGen();
        // Repeated decoder method is non empty
        Descriptors.FieldDescriptor fd = ComplexTypes.TestMessage.getDescriptor().findFieldByName(COMPLEX_MAP);
        String fieldNameInCode = ProtobufInternalUtils.underScoreToCamelCase(fd.getName(), true);
        String javaType = ProtoBufUtils.getFullJavaName(fd.getMessageType());
        String expectedCode = "  Map<Object, Map<String, Object>> map2 = new HashMap<>();\n"
            + "  for (Map.Entry<String, Map<String,"
            + "org.apache.pinot.plugin.inputformat.protobuf.ComplexTypes.TestMessage.NestedMessage>> entry:"
            + " msg.getComplexMapMap().entrySet()) {\n"
            + "    map2.put(entry.getKey(), decodeNestedMessageMapMessage( (NestedMessageMap) entry.getValue()));\n"
            + "  }\n"
            + "  msgMap.put(\"complex_map\", map2);\n";
        assertEquals(messageCodeGen.codeForMapWithValueMessageType(
            fd, fieldNameInCode, "NestedMessageMap", 1, 1).toString(), expectedCode);
    }

    @Test
    public void testGenerateDecodeCodeForAMessageForAllFieldsToRead()
        throws URISyntaxException, IOException {
        MessageCodeGen messageCodeGen = new MessageCodeGen();
        Queue<Descriptors.Descriptor> queue = new ArrayDeque<>();
        Map<String, MessageCodeGen.MessageDecoderMethod> msgDecodeCode = new HashMap<>();
        Set<String> fieldsToRead = new HashSet<>();

        queue.add(ComplexTypes.TestMessage.getDescriptor());
        messageCodeGen.generateDecodeCodeForAMessage(msgDecodeCode, queue, fieldsToRead);
        Set<String> nameList = queue.stream()
            .map(Descriptors.Descriptor::getName)
            .collect(Collectors.toSet());
        assertEquals(nameList, Set.of("NestedMessage"));
        assertEquals(msgDecodeCode.size(), 1);
        URL resource = getClass().getClassLoader().getResource("codegen_output/complex_type_all_method.txt");
        String expectedCodeOutput = new String(Files.readAllBytes(Paths.get(resource.toURI())));
        MessageCodeGen.MessageDecoderMethod messageDecoderMethod =
            msgDecodeCode.get("org.apache.pinot.plugin.inputformat.protobuf.ComplexTypes.TestMessage");
        assertEquals(messageDecoderMethod.getCode(), expectedCodeOutput);
        assertEquals(messageDecoderMethod.getMethodName(),
            "decodeorg_apache_pinot_plugin_inputformat_protobuf_ComplexTypes_TestMessageMessage");
    }

    @Test
    public void testGenerateDecodeCodeForAMessageForOnlySomeFieldsToRead()
        throws URISyntaxException, IOException {
        MessageCodeGen messageCodeGen = new MessageCodeGen();
        Queue<Descriptors.Descriptor> queue = new ArrayDeque<>();
        Map<String, MessageCodeGen.MessageDecoderMethod> msgDecodeCode = new HashMap<>();
        Set<String> fieldsToRead = Set.of(STRING_FIELD, COMPLEX_MAP, REPEATED_NESTED_MESSAGES, REPEATED_BYTES,
            NULLABLE_DOUBLE_FIELD);

        queue.add(ComplexTypes.TestMessage.getDescriptor());
        messageCodeGen.generateDecodeCodeForAMessage(msgDecodeCode, queue, fieldsToRead);
        Set<String> nameList = queue.stream()
            .map(Descriptors.Descriptor::getName)
            .collect(Collectors.toSet());
        assertEquals(nameList, Set.of("NestedMessage"));
        assertEquals(msgDecodeCode.size(), 1);
        URL resource = getClass().getClassLoader().getResource("codegen_output/complex_type_some_method.txt");
        String expectedCodeOutput = new String(Files.readAllBytes(Paths.get(resource.toURI())));
        MessageCodeGen.MessageDecoderMethod messageDecoderMethod =
            msgDecodeCode.get("org.apache.pinot.plugin.inputformat.protobuf.ComplexTypes.TestMessage");
        assertEquals(messageDecoderMethod.getCode(), expectedCodeOutput);
        assertEquals(messageDecoderMethod.getMethodName(),
            "decodeorg_apache_pinot_plugin_inputformat_protobuf_ComplexTypes_TestMessageMessage");
    }

    @Test
    public void testCodeGen()
        throws URISyntaxException, IOException {
        MessageCodeGen messageCodeGen = new MessageCodeGen();

        URL resource = getClass().getClassLoader().getResource("codegen_output/sample_record_extractor.txt");
        String expectedCodeOutput = new String(Files.readAllBytes(Paths.get(resource.toURI())));
        assertEquals(messageCodeGen.codegen(Sample.SampleRecord.getDescriptor(), new HashSet<>()),
            expectedCodeOutput);

        resource = getClass().getClassLoader().getResource("codegen_output/complex_types_record_extractor.txt");
        expectedCodeOutput = new String(Files.readAllBytes(Paths.get(resource.toURI())));
        assertEquals(messageCodeGen.codegen(ComplexTypes.TestMessage.getDescriptor(), new HashSet<>()),
            expectedCodeOutput);

        resource = getClass().getClassLoader().getResource("codegen_output/composite_types_record_extractor.txt");
        expectedCodeOutput = new String(Files.readAllBytes(Paths.get(resource.toURI())));
        assertEquals(messageCodeGen.codegen(CompositeTypes.CompositeMessage.getDescriptor(), new HashSet<>()),
            expectedCodeOutput);

        resource = getClass().getClassLoader()
            .getResource("codegen_output/complex_types_some_fields_record_extractor.txt");
        expectedCodeOutput = new String(Files.readAllBytes(Paths.get(resource.toURI())));
        assertEquals(messageCodeGen.codegen(ComplexTypes.TestMessage.getDescriptor(),
            Set.of(STRING_FIELD, COMPLEX_MAP, REPEATED_NESTED_MESSAGES, REPEATED_BYTES, NULLABLE_DOUBLE_FIELD)),
            expectedCodeOutput);

        resource = getClass().getClassLoader()
            .getResource("codegen_output/complex_types_some_fields_record_extractor.txt");
        expectedCodeOutput = new String(Files.readAllBytes(Paths.get(resource.toURI())));
        assertEquals(messageCodeGen.codegen(ComplexTypes.TestMessage.getDescriptor(),
                Set.of(STRING_FIELD, COMPLEX_MAP, REPEATED_NESTED_MESSAGES, REPEATED_BYTES, NULLABLE_DOUBLE_FIELD,
                    "does_not_exist_in_desc")),
            expectedCodeOutput);
    }
}
