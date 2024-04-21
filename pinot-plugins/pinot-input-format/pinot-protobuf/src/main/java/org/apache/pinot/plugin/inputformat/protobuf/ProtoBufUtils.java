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

import com.google.protobuf.Descriptors;
import com.google.protobuf.ProtobufInternalUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProtoBufUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProtoBufUtils.class);
  public static final String TMP_DIR_PREFIX = "pinot-protobuf";
  public static final String PB_OUTER_CLASS_SUFFIX = "OuterClass";

  private ProtoBufUtils() {
  }

  public static File getFileCopiedToLocal(String filePath)
      throws Exception {
    URI descriptorFileURI = URI.create(filePath);
    String scheme = descriptorFileURI.getScheme();
    if (scheme == null) {
      scheme = PinotFSFactory.LOCAL_PINOT_FS_SCHEME;
    }
    if (PinotFSFactory.isSchemeSupported(scheme)) {
      PinotFS pinotFS = PinotFSFactory.create(scheme);
      Path localTmpDir = Files.createTempDirectory(TMP_DIR_PREFIX + System.currentTimeMillis());
      File protoDescriptorLocalFile = createLocalFile(descriptorFileURI, localTmpDir.toFile());
      LOGGER.info("Copying protocol buffer jar/descriptor file from source: {} to dst: {}", filePath,
          protoDescriptorLocalFile.getAbsolutePath());
      pinotFS.copyToLocalFile(descriptorFileURI, protoDescriptorLocalFile);
      return protoDescriptorLocalFile;
    } else {
      throw new RuntimeException(String.format("Scheme: %s not supported in PinotFSFactory"
          + " for protocol buffer jar/descriptor file: %s.", scheme, filePath));
    }
  }

  public static InputStream getDescriptorFileInputStream(String descriptorFilePath)
      throws Exception {
    return new FileInputStream(getFileCopiedToLocal(descriptorFilePath));
  }

  public static File createLocalFile(URI srcURI, File dstDir) {
    String sourceURIPath = srcURI.getPath();
    File dstFile = new File(dstDir, new File(sourceURIPath).getName());
    LOGGER.debug("Created empty local temporary file {} to copy protocol "
        + "buffer descriptor {}", dstFile.getAbsolutePath(), srcURI);
    return dstFile;
  }

  // This is needed since the generated class name is not always the same as the proto file name.
  // The descriptor that we get from the jar drops the first prefix of the proto class name.
  // For example, insead of com.data.example.ExampleProto we get data.example.ExampleProto.
  // Copied from Flink codebase.
  // https://github.com/apache/flink/blob/master/flink-formats/flink-protobuf/
  // src/main/java/org/apache/flink/formats/protobuf/util/PbCodegenUtils.java
  public static String getFullJavaName(Descriptors.Descriptor descriptor) {
    if (null != descriptor.getContainingType()) {
      // nested type
      String parentJavaFullName = getFullJavaName(descriptor.getContainingType());
      return parentJavaFullName + "." + descriptor.getName();
    } else {
      // top level message
      String outerProtoName = getOuterProtoPrefix(descriptor.getFile());
      return outerProtoName + descriptor.getName();
    }
  }

  public static String getFullJavaName(Descriptors.EnumDescriptor enumDescriptor) {
    if (null != enumDescriptor.getContainingType()) {
      return getFullJavaName(enumDescriptor.getContainingType())
          + "."
          + enumDescriptor.getName();
    } else {
      String outerProtoName = getOuterProtoPrefix(enumDescriptor.getFile());
      return outerProtoName + enumDescriptor.getName();
    }
  }

  public static String getOuterProtoPrefix(Descriptors.FileDescriptor fileDescriptor) {
    String javaPackageName =
        fileDescriptor.getOptions().hasJavaPackage()
            ? fileDescriptor.getOptions().getJavaPackage()
            : fileDescriptor.getPackage();
    if (fileDescriptor.getOptions().getJavaMultipleFiles()) {
      return javaPackageName + ".";
    } else {
      String outerClassName = getOuterClassName(fileDescriptor);
      return javaPackageName + "." + outerClassName + ".";
    }
  }

  public static String getOuterClassName(Descriptors.FileDescriptor fileDescriptor) {
    if (fileDescriptor.getOptions().hasJavaOuterClassname()) {
      return fileDescriptor.getOptions().getJavaOuterClassname();
    } else {
      String[] fileNames = fileDescriptor.getName().split("/");
      String fileName = fileNames[fileNames.length - 1];
      String outerName = ProtobufInternalUtils.underScoreToCamelCase(fileName.split("\\.")[0], true);
      // https://developers.google.com/protocol-buffers/docs/reference/java-generated#invocation
      // The name of the wrapper class is determined by converting the base name of the .proto
      // file to camel case if the java_outer_classname option is not specified.
      // For example, foo_bar.proto produces the class name FooBar. If there is a service,
      // enum, or message (including nested types) in the file with the same name,
      // "OuterClass" will be appended to the wrapper class's name.
      boolean hasSameNameMessage =
          fileDescriptor.getMessageTypes().stream()
              .anyMatch(f -> f.getName().equals(outerName));
      boolean hasSameNameEnum =
          fileDescriptor.getEnumTypes().stream()
              .anyMatch(f -> f.getName().equals(outerName));
      boolean hasSameNameService =
          fileDescriptor.getServices().stream()
              .anyMatch(f -> f.getName().equals(outerName));
      if (hasSameNameMessage || hasSameNameEnum || hasSameNameService) {
        return outerName + PB_OUTER_CLASS_SUFFIX;
      } else {
        return outerName;
      }
    }
  }

  /**
   * Get java type str from {@link Descriptors.FieldDescriptor} which directly fetched from protobuf object.
   *
   * @return The returned code phrase will be used as java type str in codegen sections.
   */
  public static String getTypeStrFromProto(Descriptors.FieldDescriptor fd, boolean isList) {
    String typeStr;
    switch (fd.getJavaType()) {
      case MESSAGE:
        if (fd.isMapField()) {
          // map
          Descriptors.FieldDescriptor keyFd =
              fd.getMessageType().findFieldByName("key");
          Descriptors.FieldDescriptor valueFd =
              fd.getMessageType().findFieldByName("value");
          // key and value cannot be repeated
          String keyTypeStr = getTypeStrFromProto(keyFd, false);
          String valueTypeStr = getTypeStrFromProto(valueFd, false);
          typeStr = "Map<" + keyTypeStr + "," + valueTypeStr + ">";
        } else {
          // simple message
          typeStr = getFullJavaName(fd.getMessageType());
        }
        break;
      case INT:
        typeStr = "Integer";
        break;
      case LONG:
        typeStr = "Long";
        break;
      case STRING:
        typeStr = "String";
        break;
      case ENUM:
        typeStr = getFullJavaName(fd.getEnumType());
        break;
      case FLOAT:
        typeStr = "Float";
        break;
      case DOUBLE:
        typeStr = "Double";
        break;
      case BYTE_STRING:
        typeStr = "ByteString";
        break;
      case BOOLEAN:
        typeStr = "Boolean";
        break;
      default:
        throw new RuntimeException("do not support field type: " + fd.getJavaType());
    }
    if (isList) {
      return "List<" + typeStr + ">";
    } else {
      return typeStr;
    }
  }
}
