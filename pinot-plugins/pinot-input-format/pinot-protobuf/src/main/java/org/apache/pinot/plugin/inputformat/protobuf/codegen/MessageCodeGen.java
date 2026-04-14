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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.plugin.inputformat.protobuf.ProtoBufUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageCodeGen {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageCodeGen.class);
  public static final String EXTRACTOR_PACKAGE_NAME = "org.apache.pinot.plugin.inputformat.protobuf.decoder";
  public static final String EXTRACTOR_CLASS_NAME = "ProtobufRecorderMessageExtractor";
  public static final String EXTRACTOR_METHOD_NAME = "execute";

  public String codegen(Descriptors.Descriptor descriptor, Set<String> fieldsToRead) {
    // Generate the code for each message type in the fieldsToRead and the descriptor
    HashMap<String, MessageDecoderMethod> msgDecodeCode = generateMessageDeserializeCode(descriptor, fieldsToRead);
    return generateRecordExtractorCode(descriptor, fieldsToRead, msgDecodeCode);
  }

  /**
   * Batch decoder codegen. The batchMessageField is a dot-separated path through repeated MESSAGE
   * fields (e.g., "resource_logs.scope_logs.log_records"). The execute() method produced by this
   * codegen iterates over nested repeated fields and stores the innermost records as a
   * List&lt;GenericRow&gt; under GenericRow.MULTIPLE_RECORDS_KEY.
   */
  public String codegen(Descriptors.Descriptor descriptor, Set<String> fieldsToRead, String batchMessageField) {
    String[] pathSegments = batchMessageField.split("\\.");
    List<Descriptors.FieldDescriptor> batchFieldChain = new ArrayList<>();
    Descriptors.Descriptor currentDescriptor = descriptor;

    for (String segment : pathSegments) {
      Descriptors.FieldDescriptor field = currentDescriptor.findFieldByName(segment);
      if (field == null) {
        throw new IllegalArgumentException(String.format(
            "Field '%s' not found in descriptor '%s'. Available fields: %s",
            segment, currentDescriptor.getName(),
            currentDescriptor.getFields().stream()
                .map(Descriptors.FieldDescriptor::getName).collect(Collectors.toList())));
      }
      if (!field.isRepeated() || field.isMapField()) {
        throw new IllegalArgumentException(String.format(
            "Field '%s' must be a repeated (non-map) field", segment));
      }
      if (field.getType() != Descriptors.FieldDescriptor.Type.MESSAGE) {
        throw new IllegalArgumentException(String.format(
            "Field '%s' must be of message type", segment));
      }
      batchFieldChain.add(field);
      currentDescriptor = field.getMessageType();
    }

    Descriptors.Descriptor innerDescriptor = currentDescriptor;
    HashMap<String, MessageDecoderMethod> msgDecodeCode =
        generateMessageDeserializeCode(innerDescriptor, fieldsToRead);
    return generateBatchRecordExtractorCode(descriptor, batchFieldChain, innerDescriptor, fieldsToRead, msgDecodeCode);
  }

  /*
   * Generate the code for the Record Extractor that's specific to the given descriptor
   * Generates a class org.apache.pinot.plugin.inputformat.protobuf.decoder.ProtobufRecorderMessageExtractor with a
   * static method execute that takes a byte array and a GenericRow object and populates the GenericRow object with the
   * values using the message decoder generated before for the main class in the descriptor.
   */
  public String generateRecordExtractorCode(
      Descriptors.Descriptor descriptor,
      Set<String> fieldsToRead,
      HashMap<String, MessageDecoderMethod> msgDecodeCode) {
    String fullyQualifiedMsgName = ProtoBufUtils.getFullJavaName(descriptor);

    StringBuilder code = new StringBuilder();
    code.append(completeLine("package " + EXTRACTOR_PACKAGE_NAME, 0));
    code.append(addImports(List.of(
        "org.apache.pinot.spi.data.readers.GenericRow",
        "java.util.ArrayList",
        "java.util.HashMap",
        "java.util.List",
        "java.util.Map")));
    code.append("\n");
    code.append(String.format("public class %s {\n", EXTRACTOR_CLASS_NAME));
    int indent = 1;
    code.append(
        addIndent(String.format("public static GenericRow %s(byte[] from, GenericRow to) throws Exception {",
            EXTRACTOR_METHOD_NAME), indent));
    // Call the decode method for the main class in the descriptor and populate the GenericRow object
    // Example: Map<String, Object> msgMap = decodeSample_SampleRecordMessage(Sample.SampleRecord.parseFrom(from));
    code.append(
        completeLine(String.format("Map<String, Object> msgMap = %s(%s.parseFrom(from))",
                msgDecodeCode.get(ProtoBufUtils.getFullJavaName(descriptor)).getMethodName(),
                fullyQualifiedMsgName),
            ++indent));

    // Find all the fields in the descriptor to read based on fieldsToRead
    List<Descriptors.FieldDescriptor> allDesc = resolveFieldsToRead(descriptor, fieldsToRead);
    // Add the values to the GenericRow object
    // Example: to.putValue("email", msgMap.getOrDefault("email", null));
    for (Descriptors.FieldDescriptor desc: allDesc) {
      code.append(
          completeLine(String.format("to.putValue(\"%s\", msgMap.getOrDefault(\"%s\", null))",
              desc.getName(),
              desc.getName()),
              indent));
    }
    code.append(completeLine("return to", indent));
    code.append(addIndent("}", --indent));
    for (MessageDecoderMethod msgCode: msgDecodeCode.values()) {
      code.append("\n");
      code.append(msgCode.getCode());
    }
    code.append(addIndent("}", --indent));
    return code.toString();
  }


  // Batch-mode Record Extractor code. The execute() method parses the wrapper message,
  // iterates over nested repeated fields (one for-loop per level in batchFieldChain), and stores the
  // innermost records as a List<GenericRow> under GenericRow.MULTIPLE_RECORDS_KEY.
  public String generateBatchRecordExtractorCode(
      Descriptors.Descriptor wrapperDescriptor,
      List<Descriptors.FieldDescriptor> batchFieldChain,
      Descriptors.Descriptor innerDescriptor,
      Set<String> fieldsToRead,
      HashMap<String, MessageDecoderMethod> msgDecodeCode) {
    String wrapperFullName = ProtoBufUtils.getFullJavaName(wrapperDescriptor);
    String innerFullName = ProtoBufUtils.getFullJavaName(innerDescriptor);
    String innerDecoderMethodName = msgDecodeCode.get(innerFullName).getMethodName();

    StringBuilder code = new StringBuilder();
    code.append(completeLine("package " + EXTRACTOR_PACKAGE_NAME, 0));
    code.append(addImports(List.of(
        "org.apache.pinot.spi.data.readers.GenericRow",
        "java.util.ArrayList",
        "java.util.HashMap",
        "java.util.List",
        "java.util.Map")));
    code.append("\n");
    code.append(String.format("public class %s {\n", EXTRACTOR_CLASS_NAME));
    int indent = 1;
    code.append(
        addIndent(String.format("public static GenericRow %s(byte[] from, GenericRow to) throws Exception {",
            EXTRACTOR_METHOD_NAME), indent));
    indent++;

    // Parse the wrapper message
    code.append(completeLine(String.format("%s wrapper = %s.parseFrom(from)", wrapperFullName, wrapperFullName),
        indent));

    // Create the list for multiple records
    code.append(completeLine("List<GenericRow> multipleRecords = new ArrayList<>()", indent));

    // Generate nested for-loops, one per level in the batch field chain
    int chainSize = batchFieldChain.size();
    String parentVar = "wrapper";
    for (int i = 0; i < chainSize; i++) {
      Descriptors.FieldDescriptor field = batchFieldChain.get(i);
      String fieldType = ProtoBufUtils.getFullJavaName(field.getMessageType());
      String fieldCamelCase = ProtobufInternalUtils.underScoreToCamelCase(field.getName(), true);
      String loopVar = (i == chainSize - 1) ? "innerMsg" : "lvl" + i;

      code.append(addIndent(String.format("for (%s %s : %s.get%sList()) {",
          fieldType, loopVar, parentVar, fieldCamelCase), indent));
      indent++;
      parentVar = loopVar;
    }

    // Decode each inner message
    code.append(completeLine(String.format("Map<String, Object> msgMap = %s(innerMsg)", innerDecoderMethodName),
        indent));
    code.append(completeLine("GenericRow row = new GenericRow()", indent));

    // Populate fields from the inner descriptor
    List<Descriptors.FieldDescriptor> allDesc = resolveFieldsToRead(innerDescriptor, fieldsToRead);
    for (Descriptors.FieldDescriptor desc : allDesc) {
      code.append(completeLine(String.format("row.putValue(\"%s\", msgMap.getOrDefault(\"%s\", null))",
          desc.getName(), desc.getName()), indent));
    }

    code.append(completeLine("multipleRecords.add(row)", indent));

    // Close all for-loops
    for (int i = 0; i < chainSize; i++) {
      code.append(addIndent("}", --indent));
    }

    // Store under MULTIPLE_RECORDS_KEY
    code.append(completeLine("to.putValue(GenericRow.MULTIPLE_RECORDS_KEY, multipleRecords)", indent));
    code.append(completeLine("return to", indent));
    code.append(addIndent("}", --indent));

    // Append all decoder methods
    for (MessageDecoderMethod msgCode : msgDecodeCode.values()) {
      code.append("\n");
      code.append(msgCode.getCode());
    }
    code.append(addIndent("}", --indent));
    return code.toString();
  }

  // Generates methods to decode each message type in the descriptor as needed.
  public HashMap<String, MessageDecoderMethod> generateMessageDeserializeCode(
      Descriptors.Descriptor mainDescriptor, Set<String> fieldsToRead) {
    HashMap<String, MessageDecoderMethod> msgDecodeCode = new HashMap<>();
    Queue<Descriptors.Descriptor> queue = new ArrayDeque<>();
    queue.add(mainDescriptor);
    generateDecodeCodeForAMessage(msgDecodeCode, queue, fieldsToRead);

    while (!queue.isEmpty()) {
      generateDecodeCodeForAMessage(msgDecodeCode, queue, new HashSet<>());
    }
    return msgDecodeCode;
  }


  // Generates the code to decode a message type and adds it to the msgDecodeCode map
  void generateDecodeCodeForAMessage(Map<String, MessageDecoderMethod> msgDecodeCode,
      Queue<Descriptors.Descriptor> queue, Set<String> fieldsToRead) {
    Descriptors.Descriptor descriptor = queue.remove();
    String fullyQualifiedMsgName = ProtoBufUtils.getFullJavaName(descriptor);
    int varNum = 1;
    if (msgDecodeCode.containsKey(fullyQualifiedMsgName)) {
      return;
    }
    StringBuilder code = new StringBuilder();
    String methodNameOfDecoder = getDecoderMethodName(fullyQualifiedMsgName);
    int indent = 1;
    // Creates decoder method for a message type. Example method signature:
    // public static Map<String, Object> decodeSample_SampleRecordMessage(Sample.SampleRecord msg)
    code.append(addIndent(
        String.format("public static Map<String, Object> %s(%s msg) {", methodNameOfDecoder,
            fullyQualifiedMsgName), indent));
    code.append(completeLine("Map<String, Object> msgMap = new HashMap<>()", ++indent));
    List<Descriptors.FieldDescriptor> descriptorsToDerive = resolveFieldsToRead(descriptor, fieldsToRead);

    for (Descriptors.FieldDescriptor desc : descriptorsToDerive) {
      Descriptors.FieldDescriptor.Type type = desc.getType();
      String fieldNameInCode = ProtobufInternalUtils.underScoreToCamelCase(desc.getName(), true);
      switch (type) {
        case STRING:
        case INT32:
        case INT64:
        case UINT64:
        case FIXED64:
        case FIXED32:
        case UINT32:
        case SFIXED32:
        case SFIXED64:
        case SINT32:
        case SINT64:
        case DOUBLE:
        case FLOAT:
          /* Generate code for scalar field extraction
           Example: If field has presence
            if (msg.hasEmail()) {
              msgMap.put("email", msg.getEmail());
            }
           OR if no presence:
            msgMap.put("email", msg.getEmail());
           OR if repeated:
            if (msg.getEmailCount() > 0) {
             msgMap.put("email", msg.getEmailList().toArray());
            }
          */
          code.append(codeForScalarFieldExtraction(desc, fieldNameInCode, indent));
          break;
        case BOOL:
          /* Generate code for boolean field extraction
           Example: If field has presence
             if (msg.hasIsRegistered()) {
               msgMap.put("is_registered", String.valueOf(msg.getIsRegistered()));
             }
           OR if no presence:
             msgMap.put("is_registered", String.valueOf(msg.getIsRegistered()));
           OR if repeated:
             List<Object> list1 = new ArrayList<>();
             for (String row: msg.getIsRegisteredList()) {
               list3.add(String.valueOf(row));
             }
             if (!list1.isEmpty()) {
                msgMap.put("is_registered", list1.toArray());
             }
          */
          code.append(codeForComplexFieldExtraction(
              desc,
              fieldNameInCode,
              "String",
              indent,
              ++varNum,
              "String.valueOf",
              ""));
          break;
        case BYTES:
          /* Generate code for bytes field extraction
            Example: If field has presence
              if (msg.hasEmail()) {
                msgMap.put("email", msg.getEmail().toByteArray());
              }
            OR if no presence:
              msgMap.put("email", msg.getEmail().toByteArray());
            OR if repeated:
              List<Object> list1 = new ArrayList<>();
              for (com.google.protobuf.ByteString row: msg.getEmailList()) {
                list1.add(row.toByteArray());
              }
              if (!list1.isEmpty()) {
                msgMap.put("email", list1.toArray());
              }
           */
          code.append(codeForComplexFieldExtraction(
              desc,
              fieldNameInCode,
              "com.google.protobuf.ByteString",
              indent,
              ++varNum,
              "",
              ".toByteArray()"));
          break;
        case ENUM:
          /* Generate code for enum field extraction
            Example: If field has presence
              if (msg.hasStatus()) {
                msgMap.put("status", msg.getStatus().name());
              }
            OR if no presence:
              msgMap.put("status", msg.getStatus().name());
            OR if repeated:
              List<Object> list1 = new ArrayList<>();
              for (Status row: msg.getStatusList()) {
                list1.add(row.name());
              }
              if (!list1.isEmpty()) {
                msgMap.put("status", list1.toArray());
              }
           */
          code.append(codeForComplexFieldExtraction(
              desc,
              fieldNameInCode,
              ProtoBufUtils.getFullJavaNameForEnum(desc.getEnumType()),
              indent,
              ++varNum,
              "",
              ".name()"));
          break;
        case MESSAGE:
          String messageType = ProtoBufUtils.getFullJavaName(desc.getMessageType());
          if (desc.isMapField()) {
            // Generated code for Map extraction. The key for the map is always a scalar object in Protobuf.
            Descriptors.FieldDescriptor valueDescriptor = desc.getMessageType().findFieldByName("value");
            if (valueDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
              /* Generate code for map field extraction if the value type is a message
              Example: If field has presence
                if (msg.hasComplexMap()) {
                  Map<Object, Map<String, Object>> map1 = new HashMap<>();
                  for (Map.Entry<String, ComplexTypes.TestMessage.NestedMessage> entry: msg.getComplexMapMap()
                    .entrySet()) {
                    map1.put(entry.getKey(), decodeComplexTypes_TestMessage_NestedMessageMessage(entry.getValue()));
                  }
                  msgMap.put("complex_map", map1);
                }
              OR if no presence:
                Map<Object, Map<String, Object>> map1 = new HashMap<>();
                for (Map.Entry<String, ComplexTypes.TestMessage.NestedMessage> entry: msg.getComplexMapMap().entrySet())
                {
                  map1.put(entry.getKey(), decodeComplexTypes_TestMessage_NestedMessageMessage(entry.getValue()));
                }
                msgMap.put("complex_map", map1);
             */
              String valueDescClassName = ProtoBufUtils.getFullJavaName(valueDescriptor.getMessageType());
              if (!msgDecodeCode.containsKey(valueDescClassName)) {
                queue.add(valueDescriptor.getMessageType());
              }
              code.append(codeForMapWithValueMessageType(desc, fieldNameInCode, valueDescClassName, indent, varNum));
              break;
            } else {
              /* Generate code for map field extraction if the value type is a scalar
                msgMap.put("simple_map", msg.getSimpleMapMap());
               */
              code.append(completeLine(putFieldInMsgMapCode(desc.getName(),
                  getProtoFieldMethodName(fieldNameInCode + "Map"), null, null),
                  indent));
            }
          } else {
            if (!msgDecodeCode.containsKey(messageType)) {
              queue.add(desc.getMessageType());
            }
            code.append(codeForComplexFieldExtraction(desc, fieldNameInCode, messageType, indent, ++varNum,
                getDecoderMethodName(messageType), ""));
          }
          break;
        default:
          LOGGER.error(String.format("Protobuf type %s is not supported by pinot yet. Skipping this field %s",
              type, desc.getName()));
          break;
      }
    }
    code.append(completeLine("return msgMap", indent));
    code.append(addIndent("}", --indent));
    msgDecodeCode.put(fullyQualifiedMsgName, new MessageDecoderMethod(methodNameOfDecoder, code.toString()));
  }

  /* Generate code for map field extraction if the value type is a Message
   * Example: If field has presence
   *   if (msg.hasComplexMap()) {
   *    Map<Object, Map<String, Object>> map1 = new HashMap<>();
   *    for (Map.Entry<String, ComplexTypes.TestMessage.NestedMessage> entry: msg.getComplexMapMap().entrySet()) {
   *      map1.put(entry.getKey(), decodeComplexTypes_TestMessage_NestedMessageMessage(entry.getValue()));
   *    }
   *    msgMap.put("complex_map", map1);
   *  }
   * OR if no presence:
   *   Map<Object, Map<String, Object>> map1 = new HashMap<>();
   *     for (Map.Entry<String, ComplexTypes.TestMessage.NestedMessage> entry: msg.getComplexMapMap().entrySet()) {
   *     map1.put(entry.getKey(), decodeComplexTypes_TestMessage_NestedMessageMessage(entry.getValue()));
   *   }
   *   msgMap.put("complex_map", map1);
   *
   *  @param desc Field descriptor for the map field
   *  @param fieldNameInCode Field name in the generated code
   *  @param valueDescClassName Full class name of the value type in the map field
   *  @param indent Indentation level
   *  @param varNum Variable number to use in the generated code
   */
  StringBuilder codeForMapWithValueMessageType(Descriptors.FieldDescriptor desc,
      String fieldNameInCode,
      String valueDescClassName,
      int indent,
      int varNum) {
    StringBuilder code = new StringBuilder();
    varNum++;
    String mapVarName = "map" + varNum;
    StringBuilder code1 = new StringBuilder();
    code.append(
        completeLine(String.format("Map<Object, Map<String, Object>> %s = new HashMap<>()", mapVarName), indent));
    code.append(addIndent(String.format("for (Map.Entry<%s, %s> entry: msg.%s().entrySet()) {",
        ProtoBufUtils.getTypeStrFromProto(desc.getMessageType().findFieldByName("key")),
        ProtoBufUtils.getTypeStrFromProto(desc),
        getProtoFieldMethodName(fieldNameInCode + "Map")), indent));
    code.append(completeLine(String.format("%s.put(entry.getKey(), %s( (%s) entry.getValue()))", mapVarName,
        getDecoderMethodName(valueDescClassName), valueDescClassName), ++indent));
    code.append(addIndent("}", --indent));
    code.append(completeLine(String.format("msgMap.put(\"%s\", %s)", desc.getName(), mapVarName), indent));
    return code;
  }

    /*
    * Generate code for scalar field extraction
    * Example: If field has presence
    *   if (msg.hasEmail()) {
    *     msgMap.put("email", msg.getEmail());
    *   }
    * OR if no presence:
    *   msgMap.put("email", msg.getEmail());
    * OR if repeated:
    *  if (msg.getEmailCount() > 0) {
    *    msgMap.put("email", msg.getEmailList().toArray());
    *  }
   */
  StringBuilder codeForScalarFieldExtraction(Descriptors.FieldDescriptor desc, String fieldNameInCode, int indent) {
    StringBuilder code = new StringBuilder();
    if (desc.isRepeated()) {
      code.append(addIndent(String.format("if (msg.%s() > 0) {", getCountMethodName(fieldNameInCode)), indent));
      code.append(completeLine(
          putFieldInMsgMapCode(desc.getName(),
              getProtoFieldListMethodName(fieldNameInCode) + "().toArray", null, null),
          ++indent));
      code.append(addIndent("}", --indent));
    } else if (desc.hasPresence()) {
      code.append(addIndent(String.format("if (msg.%s()) {", hasPresenceMethodName(fieldNameInCode)), indent));
      code.append(completeLine(
          putFieldInMsgMapCode(desc.getName(), getProtoFieldMethodName(fieldNameInCode), null, null),
          ++indent));
      code.append(addIndent("}", --indent));
    } else {
      code.append(completeLine(
          putFieldInMsgMapCode(desc.getName(), getProtoFieldMethodName(fieldNameInCode), null, null), indent));
    }
    return code;
  }

  /*
   * Generate code for complex field extraction
   * Example: If field has presence
   *   if (msg.hasNestedMessage()) {
   *     msgMap.put("nested_message", decodeComplexTypes_TestMessage_NestedMessageMessage(msg.getNestedMessage()));
   *   }
   * OR if no presence:
   *   msgMap.put("nested_message", decodeComplexTypes_TestMessage_NestedMessageMessage(msg.getNestedMessage()));
   * OR if repeated:
   *   List<Object> list1 = new ArrayList<>();
   *   for (ComplexTypes.TestMessage.NestedMessage row: msg.getRepeatedNestedMessagesList()) {
   *     list1.add(decodeComplexTypes_TestMessage_NestedMessageMessage(row));
   *   }
   *   if (!list1.isEmpty()) {
   *     msgMap.put("repeated_nested_messages", list1.toArray());
   *   }
   */
  StringBuilder codeForComplexFieldExtraction(Descriptors.FieldDescriptor desc, String fieldNameInCode,
      String javaFieldType, int indent, int varNum, String decoderMethod, String additionalExtractions) {
    StringBuilder code = new StringBuilder();
    if (StringUtils.isBlank(additionalExtractions)) {
      additionalExtractions = "";
    }
    if (desc.isRepeated()) {
      varNum++;
      String listVarName = "list" + varNum;
      code.append(completeLine(String.format("List<Object> %s = new ArrayList<>()", listVarName), indent));
      code.append(addIndent(
          String.format("for (%s row: msg.%s()) {", javaFieldType, getProtoFieldListMethodName(fieldNameInCode)),
          indent));
      if (!StringUtils.isBlank(decoderMethod)) {
        code.append(completeLine(
            String.format("%s.add(%s(row%s))", listVarName, decoderMethod, additionalExtractions),
            ++indent));
      } else {
        code.append(completeLine(String.format("%s.add(row%s)", listVarName, additionalExtractions), ++indent));
      }
      code.append(addIndent("}", --indent));
      code.append(addIndent(String.format("if (!%s.isEmpty()) {", listVarName), indent));
      code.append(completeLine(
          String.format("msgMap.put(\"%s\", %s.toArray())", desc.getName(), listVarName),
          ++indent));
      code.append(addIndent("}", --indent));
    } else if (desc.hasPresence()) {
      code.append(addIndent(String.format("if (msg.%s()) {", hasPresenceMethodName(fieldNameInCode)), indent));
      code.append(completeLine(putFieldInMsgMapCode(
          desc.getName(), getProtoFieldMethodName(fieldNameInCode), decoderMethod, additionalExtractions),
          ++indent));
      code.append(addIndent("}", --indent));
    } else {
      code.append(completeLine(putFieldInMsgMapCode(
          desc.getName(), getProtoFieldMethodName(fieldNameInCode), decoderMethod, additionalExtractions),
          indent));
    }
    return code;
  }

  private List<Descriptors.FieldDescriptor> resolveFieldsToRead(
      Descriptors.Descriptor descriptor, Set<String> fieldsToRead) {
    if (fieldsToRead != null && !fieldsToRead.isEmpty()) {
      List<Descriptors.FieldDescriptor> result = new ArrayList<>();
      for (String fieldName : fieldsToRead.stream().sorted().collect(Collectors.toList())) {
        Descriptors.FieldDescriptor fd = descriptor.findFieldByName(fieldName);
        if (fd == null) {
          LOGGER.debug("Field " + fieldName + " not found in the descriptor");
        } else {
          result.add(fd);
        }
      }
      return result;
    }
    return descriptor.getFields();
  }

  private String getDecoderMethodName(String fullJavaType) {
    return String.format("decode%sMessage", fullJavaType.replace('.', '_'));
  }

  private String getProtoFieldMethodName(String msgNameInCode) {
    return String.format("get%s", msgNameInCode);
  }

  private String getProtoFieldListMethodName(String msgNameInCode) {
    return String.format("get%sList", msgNameInCode);
  }

  private String hasPresenceMethodName(String msgNameInCode) {
    return String.format("has%s", msgNameInCode);
  }

  private String getCountMethodName(String msgNameInCode) {
    return String.format("get%sCount", msgNameInCode);
  }

  protected String putFieldInMsgMapCode(String fieldNameInProto, String getFieldMethodName, String optionalDecodeMethod,
      String optionalAdditionalCalls) {
    if (StringUtils.isBlank(optionalAdditionalCalls)) {
      optionalAdditionalCalls = "";
    }
    if (!StringUtils.isBlank(optionalDecodeMethod)) {
      return String.format("msgMap.put(\"%s\", %s(msg.%s()%s))",
          fieldNameInProto, optionalDecodeMethod, getFieldMethodName, optionalAdditionalCalls);
    }
    return String.format("msgMap.put(\"%s\", msg.%s()%s)",
        fieldNameInProto, getFieldMethodName, optionalAdditionalCalls);
  }

  protected String addImports(List<String> classNames) {
    StringBuilder code = new StringBuilder();
    for (String className: classNames) {
      code.append("import ").append(className).append(";\n");
    }
    return code.toString();
  }

  // Adds a ';' and newline at the end of the line and adds the indent spaces at the beginning
  protected String completeLine(String line, int indent) {
    return "  ".repeat(Math.max(0, indent)) + line + ";\n";
  }

  // Adds a newline at the end of the line and adds the indent spaces at the beginning
  protected String addIndent(String line, int indent) {
    return "  ".repeat(Math.max(0, indent)) + line + "\n";
  }

  // Stores the method name and the code for the message decoder
  static class MessageDecoderMethod {
    private final String _methodName;
    private final String _code;

    MessageDecoderMethod(String methodName, String code) {
      _methodName = methodName;
      _code = code;
    }

    public String getMethodName() {
      return _methodName;
    }

    public String getCode() {
      return _code;
    }
  }
}
