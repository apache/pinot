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

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProtobufUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProtobufUtils.class);
  public static final String TMP_DIR_PREFIX = "pinot-protobuf";

  private ProtobufUtils() {
  }

  public static InputStream getDescriptorFileInputStream(String descriptorFilePath)
      throws Exception {
    URI descriptorFileURI = URI.create(descriptorFilePath);
    String scheme = descriptorFileURI.getScheme();
    if (scheme == null) {
      scheme = PinotFSFactory.LOCAL_PINOT_FS_SCHEME;
    }
    if (PinotFSFactory.isSchemeSupported(scheme)) {
      PinotFS pinotFS = PinotFSFactory.create(scheme);
      Path localTmpDir = Files.createTempDirectory(TMP_DIR_PREFIX + System.currentTimeMillis());
      File protoDescriptorLocalFile = createLocalFile(descriptorFileURI, localTmpDir.toFile());
      LOGGER.info("Copying protocol buffer descriptor file from source: {} to dst: {}", descriptorFilePath,
          protoDescriptorLocalFile.getAbsolutePath());
      pinotFS.copyToLocalFile(descriptorFileURI, protoDescriptorLocalFile);
      return new FileInputStream(protoDescriptorLocalFile);
    } else {
      throw new RuntimeException(String.format("Scheme: %s not supported in PinotFSFactory"
          + " for protocol buffer descriptor file: %s.", scheme, descriptorFilePath));
    }
  }

  public static File createLocalFile(URI srcURI, File dstDir) {
    String sourceURIPath = srcURI.getPath();
    File dstFile = new File(dstDir, new File(sourceURIPath).getName());
    LOGGER.debug("Created empty local temporary file {} to copy protocol "
        + "buffer descriptor {}", dstFile.getAbsolutePath(), srcURI);
    return dstFile;
  }

  public static Descriptors.Descriptor buildDescriptor(String messageName, String descFilePathOpt) {
    if (descFilePathOpt != null && !descFilePathOpt.isEmpty()) {
      return getDescriptor(descFilePathOpt, messageName);
    } else {
      return buildDescriptorFromJavaClass(messageName);
    }
  }

  /**
   *  Loads the given protobuf class and returns Protobuf descriptor for it.
   */
  private static Descriptors.Descriptor buildDescriptorFromJavaClass(String protobufClassName) {
    Class<?> protobufClass;
    try {
      protobufClass = Class.forName(protobufClassName);
    } catch (ClassNotFoundException e) {
      String explanation = protobufClassName.contains(".") ? "Ensure the class is included in the jar"
              : "Ensure the class name includes the package prefix";
      throw new RuntimeException(String.format("Could not load Protobuf class with name %s. %s.",
              protobufClassName, explanation), e);
    } catch (NoClassDefFoundError e) {
      if (e.getMessage().matches("com/google/proto.*Generated.*")) {
        throw new RuntimeException(String.format("Could not load Protobuf class with name %s.", protobufClassName), e);
      } else {
        throw e;
      }
    }

    if (Message.class.isAssignableFrom(protobufClass)) {
      try {
        // Extract the descriptor from Protobuf message.
        Method getDescriptorMethod = protobufClass.getDeclaredMethod("getDescriptor");
        return (Descriptors.Descriptor) getDescriptorMethod.invoke(null);
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(String.format("Could not load Protobuf class with name %s."
                + " %s is not a Protobuf Message type", protobufClassName, protobufClassName), e);
      }
    }
    throw new RuntimeException(String.format("Could not load Protobuf class with name %s."
            + " %s is not a Protobuf Message type", protobufClassName, protobufClassName));
  }

  private static Descriptors.Descriptor getDescriptor(String descFilePath, String messageName) {
    // Find the first message descriptor that matches the name.
    List<Descriptors.Descriptor> descriptorList = new ArrayList<>();
    for (Descriptors.FileDescriptor fileDescriptor : parseFileDescriptorSet(descFilePath)) {
      for (Descriptors.Descriptor descriptor : fileDescriptor.getMessageTypes()) {
        if (descriptor.getName().equalsIgnoreCase(messageName)
                || descriptor.getFullName().equalsIgnoreCase(messageName)) {
          descriptorList.add(descriptor);
        }
      }
    }

    if (descriptorList.size() > 0) {
      return descriptorList.get(0);
    } else {
      throw new RuntimeException(String.format("Unable to locate Message %s in Descriptor", messageName));
    }
  }

  private static List<Descriptors.FileDescriptor> parseFileDescriptorSet(String descFilePath) {
    DescriptorProtos.FileDescriptorSet fileDescriptorSet = null;
    try {
      InputStream dscFile = getDescriptorFileInputStream(descFilePath);
      fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(dscFile);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(String.format("Error parsing file %s descriptor byte[] into Descriptor"
              + " object", descFilePath), e);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Error reading Protobuf descriptor file at path: %s", descFilePath), e);
    }
    try {
      Map<String, DescriptorProtos.FileDescriptorProto> fileDescriptorProtoIndex =
              fileDescriptorSet.getFileList().stream().collect(Collectors.toMap(
                      fileDescriptorProto -> fileDescriptorProto.getName(),
                      fileDescriptorProto -> fileDescriptorProto));
      List<Descriptors.FileDescriptor> fileDescriptorList =
              fileDescriptorSet.getFileList().stream().map(fileDescriptorProto ->
                      buildFileDescriptor(fileDescriptorProto, fileDescriptorProtoIndex)).collect(Collectors.toList());
      return fileDescriptorList;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Error constructing FileDescriptor for %s", descFilePath), e);
    }
  }

  /**
   * Recursively constructs file descriptors for all dependencies for given
   * FileDescriptorProto and return.
   */
  private static Descriptors.FileDescriptor buildFileDescriptor(
          DescriptorProtos.FileDescriptorProto fileDescriptorProto,
          Map<String, DescriptorProtos.FileDescriptorProto> fileDescriptorProtoMap) {

    List<Descriptors.FileDescriptor> fileDescriptorList = fileDescriptorProto.getDependencyList().stream().map(
            dependency -> {
              if (fileDescriptorProtoMap.containsKey(dependency)) {
                return buildFileDescriptor(fileDescriptorProtoMap.get(dependency), fileDescriptorProtoMap);
              } else {
                throw new RuntimeException(String.format("Could not find dependency: %s", dependency));
              }
            }).collect(Collectors.toList());
    try {
      return Descriptors.FileDescriptor.buildFrom(fileDescriptorProto,
              fileDescriptorList.toArray(new Descriptors.FileDescriptor[0]));
    } catch (Descriptors.DescriptorValidationException e) {
      throw new RuntimeException(e);
    }
  }
}
