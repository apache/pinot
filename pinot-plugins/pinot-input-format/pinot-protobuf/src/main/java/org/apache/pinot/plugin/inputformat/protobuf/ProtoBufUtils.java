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
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ProtobufInternalUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtoBufUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProtoBufUtils.class);
  public static final String TMP_DIR_PREFIX = "pinot-protobuf";
  public static final String PB_OUTER_CLASS_SUFFIX = "OuterClass";

  // Last content of each remote (S3, GCS, ...) descriptor file that both fetched and resolved successfully, keyed
  // by URI. The descriptor is still fetched fresh on every decoder creation, so in-place updates of the file keep
  // propagating exactly as before; this copy is served only when the fetch itself fails (e.g. a transient DNS or
  // object-store outage), so a CONSUMING transition cannot go to ERROR on a network blip once the descriptor has
  // been fetched once by this JVM. The weight charges content, key and a fixed per-entry overhead so both total
  // memory and entry count stay bounded.
  private static final long FALLBACK_CACHE_MAX_WEIGHT_BYTES = 64L << 20;
  private static final int FALLBACK_CACHE_ENTRY_OVERHEAD_BYTES = 1024;
  private static final Cache<String, CachedDescriptor> LAST_KNOWN_GOOD_DESCRIPTORS = CacheBuilder.newBuilder()
      .maximumWeight(FALLBACK_CACHE_MAX_WEIGHT_BYTES)
      .weigher((String key, CachedDescriptor value) ->
          value._content.length + 2 * key.length() + FALLBACK_CACHE_ENTRY_OVERHEAD_BYTES)
      .build();

  /// Descriptor content stamped with the time its fetch started. Publication keeps the entry whose fetch started
  /// last, so a slow stale fetch that completes after a newer one can never roll the cache backward.
  private static class CachedDescriptor {
    final byte[] _content;
    final long _fetchStartNanos;

    CachedDescriptor(byte[] content, long fetchStartNanos) {
      _content = content;
      _fetchStartNanos = fetchStartNanos;
    }
  }

  private ProtoBufUtils() {
  }

  /// Downloads the file at the given path into a fresh local temp directory and returns it. This is a plain
  /// download with no outage fallback: [ProtoBufCodeGenMessageDecoder] calls it directly for its jar file (which
  /// must live on disk for class loading and cannot be validated as a descriptor set), so a remote jar remains a
  /// hard dependency on the remote filesystem being reachable. Descriptors should be resolved through
  /// [#getDescriptor(String, String, boolean)] instead, which reads without temp files and adds the
  /// last-known-good fallback. On success the caller owns the returned file; on copy failure the temp directory
  /// is removed (the copy may have left partial content or filesystem sidecars such as Hadoop `.crc` files).
  public static File getFileCopiedToLocal(String filePath)
      throws Exception {
    URI fileURI = URI.create(filePath);
    PinotFS pinotFS = getPinotFS(fileURI, filePath);
    Path localTmpDir = Files.createTempDirectory(TMP_DIR_PREFIX + System.currentTimeMillis());
    File localFile = createLocalFile(fileURI, localTmpDir.toFile());
    LOGGER.info("Copying protocol buffer jar/descriptor file from source: {} to dst: {}", filePath,
        localFile.getAbsolutePath());
    try {
      pinotFS.copyToLocalFile(fileURI, localFile);
      return localFile;
    } catch (Exception e) {
      deleteRecursivelyQuietly(localTmpDir);
      throw e;
    }
  }

  /// Opens the descriptor file at the given path. This is a plain read with no outage fallback, kept for
  /// compatibility; realtime decoding should resolve descriptors through
  /// [#getDescriptor(String, String, boolean)] instead.
  public static InputStream getDescriptorFileInputStream(String descriptorFilePath)
      throws Exception {
    return new FileInputStream(getFileCopiedToLocal(descriptorFilePath));
  }

  /// Resolves a message [Descriptors.Descriptor] from the descriptor set at the given path: the message type with
  /// the given name, or the first message type in the set when the name is null or empty.
  ///
  /// The descriptor set is read fresh on every call via [PinotFS#open(URI)] — no temp files — so in-place updates
  /// of the file keep propagating. When `fallbackToLastKnownGood` is set, the last remote content that both
  /// fetched and resolved successfully is remembered per URI and served ONLY when the fetch itself fails (e.g. a
  /// transient DNS or object-store outage), so decoder creation survives outages instead of permanently marking
  /// the CONSUMING segment ERROR. Content that fetches successfully but does not resolve (corrupt, empty, or
  /// missing the requested message type) always fails the call and leaves the remembered copy untouched: a bad
  /// descriptor deployment must surface as an error, never silently serve an obsolete schema. Local files are
  /// read fresh and never remembered.
  ///
  /// NOTE: Only this method has the fallback. The jar used by [ProtoBufCodeGenMessageDecoder] is downloaded via
  /// [#getFileCopiedToLocal(String)] without one (see the note there).
  public static Descriptors.Descriptor getDescriptor(String descriptorFilePath, @Nullable String messageTypeName,
      boolean fallbackToLastKnownGood)
      throws Exception {
    URI fileURI = URI.create(descriptorFilePath);
    String scheme = fileURI.getScheme();
    boolean remote = scheme != null && !scheme.equals(PinotFSFactory.LOCAL_PINOT_FS_SCHEME);
    boolean fallbackEnabled = remote && fallbackToLastKnownGood;
    // The stamp is taken before the fetch so that publication can reject a slow stale fetch that completes after
    // a newer one (see CachedDescriptor)
    long fetchStartNanos = System.nanoTime();
    byte[] content;
    try {
      content = readFileToBytes(fileURI, descriptorFilePath);
    } catch (Exception fetchException) {
      CachedDescriptor lastKnownGood =
          fallbackEnabled ? LAST_KNOWN_GOOD_DESCRIPTORS.getIfPresent(descriptorFilePath) : null;
      if (lastKnownGood == null) {
        throw fetchException;
      }
      LOGGER.warn("Failed to fetch protocol buffer descriptor file: {}, falling back to the last known good copy",
          descriptorFilePath, fetchException);
      return resolveMessageDescriptor(lastKnownGood._content, messageTypeName, descriptorFilePath);
    }
    // A fetched-but-unresolvable descriptor fails here, before publication, so it can neither be served nor
    // overwrite the last known good copy
    Descriptors.Descriptor descriptor = resolveMessageDescriptor(content, messageTypeName, descriptorFilePath);
    if (fallbackEnabled) {
      LAST_KNOWN_GOOD_DESCRIPTORS.asMap().merge(descriptorFilePath,
          new CachedDescriptor(content, fetchStartNanos),
          (existing, candidate) -> candidate._fetchStartNanos - existing._fetchStartNanos > 0 ? candidate
              : existing);
    }
    return descriptor;
  }

  /// Parses the descriptor set and resolves the requested message type (or the first one when no name is given).
  /// Failures here mean the content is unusable — deliberately distinct from a fetch failure.
  private static Descriptors.Descriptor resolveMessageDescriptor(byte[] descriptorSetBytes,
      @Nullable String messageTypeName, String descriptorFilePath)
      throws Exception {
    DynamicSchema schema;
    try {
      schema = DynamicSchema.parseFrom(descriptorSetBytes);
    } catch (Exception e) {
      throw new IllegalStateException("Invalid protocol buffer descriptor set at: " + descriptorFilePath, e);
    }
    String typeName = messageTypeName;
    if (StringUtils.isEmpty(typeName)) {
      Set<String> messageTypes = schema.getMessageTypes();
      Preconditions.checkState(!messageTypes.isEmpty(), "Descriptor set at: %s contains no message types",
          descriptorFilePath);
      typeName = messageTypes.iterator().next();
    }
    Descriptors.Descriptor descriptor = schema.getMessageDescriptor(typeName);
    Preconditions.checkState(descriptor != null, "Message type: %s not found in descriptor set at: %s", typeName,
        descriptorFilePath);
    return descriptor;
  }

  private static byte[] readFileToBytes(URI fileURI, String filePath)
      throws Exception {
    try (InputStream inputStream = getPinotFS(fileURI, filePath).open(fileURI)) {
      return inputStream.readAllBytes();
    }
  }

  private static PinotFS getPinotFS(URI fileURI, String filePath) {
    String scheme = fileURI.getScheme();
    if (scheme == null) {
      scheme = PinotFSFactory.LOCAL_PINOT_FS_SCHEME;
    }
    if (!PinotFSFactory.isSchemeSupported(scheme)) {
      throw new RuntimeException(String.format("Scheme: %s not supported in PinotFSFactory"
          + " for protocol buffer jar/descriptor file: %s.", scheme, filePath));
    }
    return PinotFSFactory.create(scheme);
  }

  private static void deleteRecursivelyQuietly(Path dir) {
    try (Stream<Path> paths = Files.walk(dir)) {
      paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
    } catch (Exception e) {
      LOGGER.warn("Failed to clean up temporary directory: {}", dir, e);
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
