package org.apache.pinot.plugin.inputformat.protobuf.codegen;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.ProtobufInternalUtils;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.plugin.inputformat.protobuf.ProtoBufCodeGenMessgeDecoder;

public class MessageCodeGen {

  public String codegen(ClassLoader protoMessageClsLoader, String protoClassName, Set<String> fieldsToRead)
      throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException,
             InstantiationException {
    Descriptors.Descriptor descriptor = getDescriptorForProtoClass(protoMessageClsLoader, protoClassName);
    HashMap<String, MessageDecoderMethod> msgDecodeCode = generateMessageDeserializeCode(descriptor, fieldsToRead);
    return generateCode(descriptor, fieldsToRead, msgDecodeCode);
  }

  public String generateCode(
      Descriptors.Descriptor descriptor,
      Set<String> fieldsToRead,
      HashMap<String, MessageDecoderMethod> msgDecodeCode)
      throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    String fullyQualifiedMsgName = ProtobufInternalUtils.getFullJavaName(descriptor);

    StringBuilder code = new StringBuilder();
    code.append("package org.apache.pinot.plugin.inputformat.protobuf.decoder;\n");
    code.append(addImports(Set.of(
        "org.apache.pinot.spi.data.readers.GenericRow",
        "java.util.ArrayList",
        "java.util.HashMap",
        "java.util.List",
        "java.util.Map")));
    code.append("\n");
    code.append(String.format("public class %s {\n", ProtoBufCodeGenMessgeDecoder.EXTRACTOR_CLASS_NAME));
    int indent = 1;
    code.append(
        addIndent("public static GenericRow execute(byte[] from, GenericRow to) throws Exception {", indent));
    code.append(
        completeLine(String.format("Map<String, Object> msgMap = %s(%s.parseFrom(from))",
                msgDecodeCode.get(ProtobufInternalUtils.getFullJavaName(descriptor)).getMethodName(),
                fullyQualifiedMsgName),
            ++indent));

    List<Descriptors.FieldDescriptor> allDesc = new ArrayList<>();
    if (fieldsToRead != null && !fieldsToRead.isEmpty()) {
      for (String fieldName: fieldsToRead) {
        allDesc.add(descriptor.findFieldByName(fieldName));
      }
    } else {
      allDesc = descriptor.getFields();
    }
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
    code.append(addIndent("}\n", --indent));
    return code.toString();
  }

  public String addImports(Set<String> classNames) {
    String code = "";
    for (String className: classNames) {
      code = code + "import " + className + ";\n";
    }
    return code;
  }

  private Descriptors.Descriptor getDescriptorForProtoClass(
      ClassLoader protoMessageClsLoader,
      String protoClassName)
      throws NoSuchMethodException, ClassNotFoundException, InvocationTargetException, IllegalAccessException,
             InstantiationException {
    Class<? extends Message> updateMessage = (Class<Message>) protoMessageClsLoader.loadClass(protoClassName);
    return (Descriptors.Descriptor) updateMessage.getMethod("getDescriptor").invoke(null);
  }

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

  private void generateDecodeCodeForAMessage(
      HashMap<String, MessageDecoderMethod> msgDecodeCode,
      Queue<Descriptors.Descriptor> queue,
      Set<String> fieldsToRead) {
    Descriptors.Descriptor descriptor = queue.remove();
    String fullyQualifiedMsgName = ProtobufInternalUtils.getFullJavaName(descriptor);
    int varNum = 1;
    if (msgDecodeCode.containsKey(fullyQualifiedMsgName)) {
      return;
    }
    String msgInGenFuncName = ProtobufInternalUtils.underScoreToCamelCase(descriptor.getName(), true);
    StringBuilder code = new StringBuilder();
    String methodNameOfDecoder = getDecoderMethodName(fullyQualifiedMsgName);
    int indent = 1;
    code.append(addIndent(
        String.format("private static Map<String, Object> %s(%s msg) {", methodNameOfDecoder,
            fullyQualifiedMsgName), indent));
    code.append(completeLine("Map<String, Object> msgMap = new HashMap<>()", ++indent));
    List<Descriptors.FieldDescriptor> descriptorsToDerive = new ArrayList<>();
    if (fieldsToRead != null && !fieldsToRead.isEmpty()) {
      for (String fieldName: fieldsToRead) {
        descriptorsToDerive.add(descriptor.findFieldByName(fieldName));
      }
    } else {
      descriptorsToDerive = descriptor.getFields();
    }

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
        case SFIXED64:
        case SINT32:
        case SINT64:
        case DOUBLE:
        case FLOAT:
        case BOOL:
          code.append(codeForScalarFieldExtraction(desc, fieldNameInCode, indent));
          break;
        case BYTES:
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
          code.append(codeForComplexFieldExtraction(
              desc,
              fieldNameInCode,
              ProtobufInternalUtils.getFullJavaName(desc.getEnumType()),
              indent,
              ++varNum,
              "",
              ".name()"));
          break;
        case MESSAGE:
          String messageType = ProtobufInternalUtils.getFullJavaName(desc.getMessageType());
          if (!msgDecodeCode.containsKey(messageType)) {
            queue.add(desc.getMessageType());
          }
          code.append(codeForComplexFieldExtraction(
              desc,
              fieldNameInCode,
              messageType,
              indent,
              ++varNum,
              getDecoderMethodName(messageType),
              ""));
          break;
        default:
          break;
      }
    }
    code.append(completeLine("return msgMap", indent));
    code.append(addIndent("}", --indent));
    msgDecodeCode.put(fullyQualifiedMsgName, new MessageDecoderMethod(methodNameOfDecoder, code.toString()));
  }

  private StringBuilder codeForScalarFieldExtraction(
      Descriptors.FieldDescriptor desc, String fieldNameInCode, int indent) {
    StringBuilder code = new StringBuilder();
    if (desc.isRepeated()) {
      code.append(addIndent(String.format("if (msg.%s() > 0) {", getCountMethodName(fieldNameInCode)), indent));
      code.append(completeLine(
          putFieldInMsgMapCode(desc.getName(), getProtoFieldListMethodName(fieldNameInCode) + "().toArray"),
          ++indent));
      code.append(addIndent("}", --indent));
    } else if (desc.hasPresence()) {
      code.append(addIndent(String.format("if (msg.%s()) {", hasPresenceMethodName(fieldNameInCode)), indent));
      code.append(completeLine(
          putFieldInMsgMapCode(desc.getName(), getProtoFieldMethodName(fieldNameInCode)),
          ++indent));
      code.append(addIndent("}", --indent));
    } else {
      code.append(completeLine(putFieldInMsgMapCode(desc.getName(), getProtoFieldMethodName(fieldNameInCode)), indent));
    }
    return code;
  }

  private StringBuilder codeForComplexFieldExtraction(
      Descriptors.FieldDescriptor desc,
      String fieldNameInCode,
      String javaFieldType,
      int indent,
      int varNum,
      String decoderMethod,
      String additionalExtractions) {
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
      code.append(addIndent("}", indent--));
    } else {
      code.append(completeLine(putFieldInMsgMapCode(
          desc.getName(), getProtoFieldMethodName(fieldNameInCode), decoderMethod, additionalExtractions),
          indent));
    }
    return code;
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

  private String putFieldInMsgMapCode(String fieldNameInProto, String methodToCall) {
    return String.format("msgMap.put(\"%s\", msg.%s())", fieldNameInProto, methodToCall);
  }

  private String putFieldInMsgMapCode(String fieldNameInProto,
      String getFieldMethodName, String optionalDecodeMethod, String optionalAdditionalCalls) {
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



  protected String completeLine(String line, int indent) {
    StringBuilder sb = new StringBuilder();
    sb.append(" ".repeat(Math.max(0, indent)));
    sb.append(line);
    sb.append(";\n");
    return sb.toString();
  }

  protected String addIndent(String line, int indent) {
    StringBuilder sb = new StringBuilder();
    sb.append(" ".repeat(Math.max(0, indent)));
    sb.append(line);
    sb.append("\n");
    return sb.toString();
  }

  private class MessageDecoderMethod {
    private String _methodName;
    private String _code;

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
