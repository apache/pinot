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

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ProtobufInternalUtils;
import java.io.ByteArrayInputStream;
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

  // Last successfully fetched (and parseable) content of each remote (S3, GCS, ...) descriptor file, keyed by URI.
  // The descriptor is still fetched fresh on every decoder creation, so in-place updates of the file keep
  // propagating exactly as before; this copy is served only when the fetch fails (e.g. a transient DNS or
  // object-store outage), so a CONSUMING transition cannot go to ERROR on a network blip once the descriptor has
  // been fetched once by this JVM. Bounded by total content size as a safety net.
  private static final long FALLBACK_CACHE_MAX_WEIGHT_BYTES = 64L << 20;
  private static final Cache<String, byte[]> LAST_KNOWN_GOOD_DESCRIPTORS = CacheBuilder.newBuilder()
      .maximumWeight(FALLBACK_CACHE_MAX_WEIGHT_BYTES)
      .weigher((String key, byte[] value) -> value.length)
      .build();

  private ProtoBufUtils() {
  }

  /// Downloads the file at the given path into a fresh local temp directory and returns it. This is a plain
  /// download with no outage fallback: [ProtoBufCodeGenMessageDecoder] calls it directly for its jar file (which
  /// must live on disk for class loading and cannot be validated as a descriptor set), so a remote jar remains a
  /// hard dependency on the remote filesystem being reachable. Descriptor files should be read through
  /// [#getDescriptorFileInputStream(String)] instead, which adds the last-known-good fallback.
  public static File getFileCopiedToLocal(String filePath)
      throws Exception {
    URI fileURI = URI.create(filePath);
    String scheme = fileURI.getScheme();
    if (scheme == null) {
      scheme = PinotFSFactory.LOCAL_PINOT_FS_SCHEME;
    }
    if (PinotFSFactory.isSchemeSupported(scheme)) {
      PinotFS pinotFS = PinotFSFactory.create(scheme);
      Path localTmpDir = Files.createTempDirectory(TMP_DIR_PREFIX + System.currentTimeMillis());
      File localFile = createLocalFile(fileURI, localTmpDir.toFile());
      LOGGER.info("Copying protocol buffer jar/descriptor file from source: {} to dst: {}", filePath,
          localFile.getAbsolutePath());
      pinotFS.copyToLocalFile(fileURI, localFile);
      return localFile;
    } else {
      throw new RuntimeException(String.format("Scheme: %s not supported in PinotFSFactory"
          + " for protocol buffer jar/descriptor file: %s.", scheme, filePath));
    }
  }

  /// Returns the content of the descriptor file at the given path. A remote file is fetched fresh on every call so
  /// in-place updates are picked up, and the last successfully fetched (and parseable) content is remembered per
  /// URI: when the fetch fails, or returns bytes that do not parse as a descriptor set, the remembered copy is
  /// served instead so that decoder creation survives transient DNS / object-store outages. Local files are always
  /// read fresh and never remembered.
  ///
  /// NOTE: Only descriptor files get this fallback. The jar used by [ProtoBufCodeGenMessageDecoder] is downloaded
  /// via [#getFileCopiedToLocal(String)] without one (see the note there).
  public static InputStream getDescriptorFileInputStream(String descriptorFilePath)
      throws Exception {
    URI fileURI = URI.create(descriptorFilePath);
    String scheme = fileURI.getScheme();
    if (scheme == null || scheme.equals(PinotFSFactory.LOCAL_PINOT_FS_SCHEME)) {
      return new FileInputStream(getFileCopiedToLocal(descriptorFilePath));
    }
    byte[] content;
    try {
      content = downloadFileToBytes(descriptorFilePath);
      // Validate before remembering so that a corrupt/truncated download can neither be served nor overwrite the
      // last known good copy
      DynamicSchema.parseFrom(new ByteArrayInputStream(content));
      LAST_KNOWN_GOOD_DESCRIPTORS.put(descriptorFilePath, content);
    } catch (Exception e) {
      content = LAST_KNOWN_GOOD_DESCRIPTORS.getIfPresent(descriptorFilePath);
      if (content == null) {
        throw e;
      }
      LOGGER.warn("Failed to fetch protocol buffer descriptor file: {}, falling back to the last successfully"
          + " fetched copy", descriptorFilePath, e);
    }
    return new ByteArrayInputStream(content);
  }

  private static byte[] downloadFileToBytes(String filePath)
      throws Exception {
    File localFile = getFileCopiedToLocal(filePath);
    try {
      return Files.readAllBytes(localFile.toPath());
    } finally {
      Files.deleteIfExists(localFile.toPath());
      Files.deleteIfExists(localFile.getParentFile().toPath());
    }
  }

  @VisibleForTesting
  static void clearDescriptorCache() {
    LAST_KNOWN_GOOD_DESCRIPTORS.invalidateAll();
  }

  public static File createLocalFile(URI srcURI, File dstDir) {
    String sourceURIPath = srcURI.getPath();
    File dstFile = new File(dstDir, new File(sourceURIPath).getName());
    LOGGER.debug("Created empty local temporary file {} to copy protocol "
        + "buffer descriptor {}", dstFile.getAbsolutePath(), srcURI);
    return dstFile;
  }

  public static String getFullJavaName(Descriptors.Descriptor descriptor) {
    String prefix;
    if (null != descriptor.getContainingType()) {
      // nested type
      prefix = getFullJavaName(descriptor.getContainingType());
    } else {
      // top level message
      prefix = getOuterProtoPrefix(descriptor.getFile());
    }
    return prefix + "." + descriptor.getName();
  }

  public static String getFullJavaNameForEnum(Descriptors.EnumDescriptor enumDescriptor) {
    if (null != enumDescriptor.getContainingType()) {
      return getFullJavaName(enumDescriptor.getContainingType())
          + "."
          + enumDescriptor.getName();
    } else {
      String outerProtoName = getOuterProtoPrefix(enumDescriptor.getFile());
      return outerProtoName + "." + enumDescriptor.getName();
    }
  }

  public static String getOuterProtoPrefix(Descriptors.FileDescriptor fileDescriptor) {
    String javaPackageName =
        fileDescriptor.getOptions().hasJavaPackage()
            ? fileDescriptor.getOptions().getJavaPackage()
            : fileDescriptor.getPackage();
    if (fileDescriptor.getOptions().getJavaMultipleFiles()) {
      return javaPackageName;
    } else if (fileDescriptor.getOptions().hasJavaOuterClassname()) {
      return javaPackageName + "." + fileDescriptor.getOptions().hasJavaOuterClassname();
    } else {
      String[] fileNames = fileDescriptor.getName().split("/");
      String fileName = fileNames[fileNames.length - 1];
      String outerName = ProtobufInternalUtils.underScoreToCamelCase(fileName.split("\\.")[0], true);
      if (hasTypeWithName(fileDescriptor.getMessageTypes(), outerName)
          || hasTypeWithName(fileDescriptor.getEnumTypes(), outerName)
          || hasTypeWithName(fileDescriptor.getServices(), outerName)) {
        // https://developers.google.com/protocol-buffers/docs/reference/java-generated#invocation
        // The name of the wrapper class is determined by converting the base name of the .proto
        // file to camel case if the java_outer_classname option is not specified.
        // For example, foo_bar.proto produces the class name FooBar. If there is a service,
        // enum, or message (including nested types) in the file with the same name,
        // "OuterClass" will be appended to the wrapper class's name.
        return javaPackageName + "." + outerName + PB_OUTER_CLASS_SUFFIX;
      } else {
        return javaPackageName + "." + outerName;
      }
    }
  }

  private static boolean hasTypeWithName(Iterable<? extends Descriptors.GenericDescriptor> descriptors, String name) {
    for (Descriptors.GenericDescriptor descriptor : descriptors) {
      if (descriptor.getName().equals(name)) {
        return true;
      }
    }
    return false;
  }

  /// Get java type str from [Descriptors.FieldDescriptor] which directly fetched from protobuf object.
  ///
  /// @return The returned code phrase will be used as java type str in codegen sections.
  public static String getTypeStrFromProto(Descriptors.FieldDescriptor desc) {
    switch (desc.getJavaType()) {
      case INT:
        return "Integer";
      case LONG:
        return "Long";
      case STRING:
        return "String";
      case FLOAT:
        return "Float";
      case DOUBLE:
        return "Double";
      case BYTE_STRING:
        return "ByteString";
      case BOOLEAN:
        return "Boolean";
      case ENUM:
        return getFullJavaNameForEnum(desc.getEnumType());
      case MESSAGE:
        if (desc.isMapField()) {
          // map
          final Descriptors.FieldDescriptor key = desc.getMessageType().findFieldByName("key");
          final Descriptors.FieldDescriptor value = desc.getMessageType().findFieldByName("value");
          // key and value cannot be repeated
          String keyTypeStr = getTypeStrFromProto(key);
          String valueTypeStr = getTypeStrFromProto(value);
          return "Map<" + keyTypeStr + "," + valueTypeStr + ">";
        } else {
          // simple message
          return getFullJavaName(desc.getMessageType());
        }
      default:
        throw new RuntimeException("do not support field type: " + desc.getJavaType());
    }
  }
}
