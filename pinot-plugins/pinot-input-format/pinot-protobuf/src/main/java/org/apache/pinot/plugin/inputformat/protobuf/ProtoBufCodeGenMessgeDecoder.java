package org.apache.pinot.plugin.inputformat.protobuf;

import com.google.common.base.Preconditions;
import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.plugin.inputformat.protobuf.codegen.MessageCodeGen;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.codehaus.janino.SimpleCompiler;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtoBufCodeGenMessgeDecoder implements StreamMessageDecoder<byte[]> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(
          org.apache.pinot.plugin.inputformat.protobuf.ProtoBufCodeGenMessgeDecoder.class);

  public static final String PROTOBUF_JAR_FILE_PATH = "jarFile";
  public static final String PROTO_CLASS_NAME = "protoClassName";
  public static final String EXTRACTOR_CLASS_NAME = "ProtobufRecorderMessageExtractor";
  private Class _recordExtractor = ProtoBufMessageDecoder.class;
  private Method _decodeMethod;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    Preconditions.checkState(
        props.containsKey(PROTOBUF_JAR_FILE_PATH),
        "Protocol Buffer schema jar file must be provided");
    Preconditions.checkState(
        props.containsKey(PROTO_CLASS_NAME),
        "Protocol Buffer Message class name must be provided");
    String protoClassName = props.getOrDefault(PROTO_CLASS_NAME, "");
    String jarPath = props.getOrDefault(PROTOBUF_JAR_FILE_PATH, "");
    ClassLoader protoMessageClsLoader = loadClass(jarPath);
    String codeGenCode = new MessageCodeGen().codegen(protoMessageClsLoader, protoClassName, fieldsToRead);
    _recordExtractor = compileClass(protoMessageClsLoader, EXTRACTOR_CLASS_NAME, codeGenCode);
    _decodeMethod = _recordExtractor.getMethod("execute", byte[].class, GenericRow.class);
  }


  @Nullable
  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    try {
      destination = (GenericRow) _decodeMethod.invoke(null, payload, destination);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return destination;
  }

  @Nullable
  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
  }

  private static ClassLoader loadClass(String jarFilePath) {
    try {
      File file = ProtoBufUtils.getFileCopiedToLocal(jarFilePath);
      URL url = file.toURI().toURL();
      URL[] urls = new URL[] {url};
      return new URLClassLoader(urls);
    } catch (Exception e) {
      throw new RuntimeException("Error loading protobuf class", e);
    }
  }

  public static Class compileClass(ClassLoader classloader, String className, String code)
      throws ClassNotFoundException {
    SimpleCompiler simpleCompiler = new SimpleCompiler();
    simpleCompiler.setParentClassLoader(classloader);
    try {
      simpleCompiler.cook(code);
    } catch (Throwable t) {
      System.out.println("Protobuf codegen compile error: \n" + code);
      throw new RuntimeException(
          "Program cannot be compiled. This is a bug. Please file an issue.", t);
    }
    return simpleCompiler.getClassLoader()
        .loadClass("org.apache.pinot.plugin.inputformat.protobuf.decoder." + className);
  }
}
