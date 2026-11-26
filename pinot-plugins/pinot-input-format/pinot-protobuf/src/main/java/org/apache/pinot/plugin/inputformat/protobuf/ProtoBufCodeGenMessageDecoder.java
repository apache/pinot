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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.plugin.inputformat.protobuf.codegen.MessageCodeGen;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.codehaus.janino.SimpleCompiler;


/// Decodes protobuf messages with an extractor generated and compiled from the message class in a jar file.
///
/// The jar is loaded into the JVM, so it must come from a location the operator of the process has allowed through
/// the JVM system property [#ALLOWED_JAR_LOCATIONS_PROPERTY]. The decoder cannot be used when the property is not
/// set. The property is deliberately a JVM system property rather than a Pinot config, so that it can only be set
/// by whoever starts the process.
///
/// Instances are not thread-safe.
public class ProtoBufCodeGenMessageDecoder implements StreamMessageDecoder<byte[]> {
  public static final String PROTOBUF_JAR_FILE_PATH = "jarFile";
  public static final String PROTO_CLASS_NAME = "protoClassName";
  /// JVM system property holding the comma-separated locations (directories or URI prefixes, e.g.
  /// `/opt/pinot/protobuf,s3://my-bucket/protobuf`) the jar file may be loaded from.
  public static final String ALLOWED_JAR_LOCATIONS_PROPERTY = "pinot.protobuf.codegen.decoder.allowed.jar.locations";
  private Method _decodeMethod;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    Preconditions.checkState(props.containsKey(PROTOBUF_JAR_FILE_PATH),
        "Protocol Buffer schema jar file must be provided");
    Preconditions.checkState(props.containsKey(PROTO_CLASS_NAME),
        "Protocol Buffer Message class name must be provided");
    String protoClassName = props.getOrDefault(PROTO_CLASS_NAME, "");
    String jarPath = props.getOrDefault(PROTOBUF_JAR_FILE_PATH, "");
    checkJarLocationAllowed(jarPath);
    ClassLoader protoMessageClsLoader = loadClass(jarPath);
    Descriptors.Descriptor descriptor = getDescriptorForProtoClass(protoMessageClsLoader, protoClassName);
    String codeGenCode = new MessageCodeGen().codegen(descriptor, fieldsToRead);
    Class<?> recordExtractor = compileClass(protoMessageClsLoader,
        MessageCodeGen.EXTRACTOR_PACKAGE_NAME + "." + MessageCodeGen.EXTRACTOR_CLASS_NAME, codeGenCode);
    _decodeMethod = recordExtractor.getMethod(MessageCodeGen.EXTRACTOR_METHOD_NAME, byte[].class, GenericRow.class);
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    try {
      destination = (GenericRow) _decodeMethod.invoke(null, payload, destination);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while decoding protobuf message", e);
    }
    return destination;
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    if (offset != 0 || payload.length > length) {
      payload = Arrays.copyOfRange(payload, offset, offset + length);
    }
    return decode(payload, destination);
  }

  /// Fails unless the jar file is under one of the locations in [#ALLOWED_JAR_LOCATIONS_PROPERTY].
  @VisibleForTesting
  static void checkJarLocationAllowed(String jarPath)
      throws IOException {
    String allowedLocations = System.getProperty(ALLOWED_JAR_LOCATIONS_PROPERTY);
    Preconditions.checkState(StringUtils.isNotBlank(allowedLocations),
        "ProtoBufCodeGenMessageDecoder is disabled. To enable it, set the JVM system property: %s to the "
            + "comma-separated locations the jar file may be loaded from", ALLOWED_JAR_LOCATIONS_PROPERTY);
    String jarLocation = normalizeLocation(jarPath);
    for (String allowedLocation : StringUtils.split(allowedLocations, ',')) {
      if (StringUtils.isBlank(allowedLocation)) {
        continue;
      }
      String prefix = normalizeLocation(allowedLocation.trim());
      if (!prefix.endsWith("/")) {
        prefix += "/";
      }
      if (jarLocation.startsWith(prefix)) {
        return;
      }
    }
    throw new IllegalStateException(
        String.format("Protocol Buffer schema jar file: %s is not under any of the locations allowed by: %s", jarPath,
            ALLOWED_JAR_LOCATIONS_PROPERTY));
  }

  /// Returns the location as a normalized string of scheme, authority and decoded path. Local paths are
  /// canonicalized so that relative segments and symbolic links cannot be used to leave an allowed directory; remote
  /// paths with relative segments are rejected.
  private static String normalizeLocation(String location)
      throws IOException {
    URI uri = URI.create(location);
    String scheme = uri.getScheme();
    String path = uri.getPath();
    Preconditions.checkState(StringUtils.isNotEmpty(path), "Invalid location: %s", location);
    if (scheme == null || scheme.equals(PinotFSFactory.LOCAL_PINOT_FS_SCHEME)) {
      return new File(path).getCanonicalFile().toURI().toString();
    }
    for (String segment : StringUtils.split(path, '/')) {
      Preconditions.checkState(!segment.equals("..") && !segment.equals("."),
          "Relative path segments are not allowed in location: %s", location);
    }
    return scheme + "://" + StringUtils.defaultString(uri.getRawAuthority()) + path;
  }

  public static ClassLoader loadClass(String jarFilePath) {
    try {
      File file = ProtoBufUtils.getFileCopiedToLocal(jarFilePath);
      URL url = file.toURI().toURL();
      URL[] urls = new URL[]{url};
      return new URLClassLoader(urls);
    } catch (Exception e) {
      throw new RuntimeException("Error loading protobuf class", e);
    }
  }

  public static Class<?> compileClass(ClassLoader classloader, String className, String code)
      throws ClassNotFoundException {
    SimpleCompiler simpleCompiler = new SimpleCompiler();
    simpleCompiler.setParentClassLoader(classloader);
    try {
      simpleCompiler.cook(code);
    } catch (Throwable t) {
      throw new RuntimeException("Program cannot be compiled. This is a bug. Please file an issue.", t);
    }
    return simpleCompiler.getClassLoader().loadClass(className);
  }

  public static Descriptors.Descriptor getDescriptorForProtoClass(ClassLoader protoMessageClsLoader,
      String protoClassName)
      throws NoSuchMethodException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
    Class<? extends Message> updateMessage = (Class<Message>) protoMessageClsLoader.loadClass(protoClassName);
    return (Descriptors.Descriptor) updateMessage.getMethod("getDescriptor").invoke(null);
  }
}
