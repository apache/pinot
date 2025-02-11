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

import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.plugin.inputformat.protobuf.codegen.MessageCodeGen;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtoBufCodeGenMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProtoBufCodeGenMessageDecoder.class);

  public static final String PROTOBUF_JAR_FILE_PATH = "jarFile";
  public static final String PROTO_CLASS_NAME = "protoClassName";
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
    Descriptors.Descriptor descriptor = getDescriptorForProtoClass(protoMessageClsLoader, protoClassName);
    String codeGenCode = new MessageCodeGen().codegen(descriptor, fieldsToRead);
    _recordExtractor = compileClass(
        protoMessageClsLoader,
        MessageCodeGen.EXTRACTOR_PACKAGE_NAME + "." + MessageCodeGen.EXTRACTOR_CLASS_NAME, codeGenCode);
    _decodeMethod = _recordExtractor.getMethod(MessageCodeGen.EXTRACTOR_METHOD_NAME, byte[].class, GenericRow.class);
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

  public static ClassLoader loadClass(String jarFilePath) {
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
      throw new RuntimeException(
          "Program cannot be compiled. This is a bug. Please file an issue.", t);
    }
    return simpleCompiler.getClassLoader()
        .loadClass(className);
  }

  public static Descriptors.Descriptor getDescriptorForProtoClass(ClassLoader protoMessageClsLoader,
      String protoClassName)
      throws NoSuchMethodException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
    Class<? extends Message> updateMessage = (Class<Message>) protoMessageClsLoader.loadClass(protoClassName);
    return (Descriptors.Descriptor) updateMessage.getMethod("getDescriptor").invoke(null);
  }
}
