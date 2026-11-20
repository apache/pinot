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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.plugin.inputformat.protobuf.codegen.MessageCodeGen;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link StreamMessageDecoder} for Protocol Buffer messages that uses runtime code generation and Janino compilation
 * to produce an efficient, reflection-free decode path.
 *
 * <p>The only expensive operation in {@link #init} is fetching the schema JAR from remote storage (S3, HDFS, etc.).
 * Code generation and Janino compilation are fast (sub-100ms, in-memory) and run on every {@link #init} call, which
 * is correct because {@code fieldsToRead} can differ between decoder instances for the same topic.
 *
 * <p>A JVM-level cache keyed by {@code topicName} stores the local copy of the JAR file. On every {@link #init}:
 * <ul>
 *   <li>If the cached {@code jarPath} matches the configured one, the local file is reused — no network I/O.</li>
 *   <li>If {@code jarPath} changed (config update), the new JAR is fetched and the cache entry is replaced.</li>
 *   <li>If the fetch fails and a stale entry exists, the stale local file is reused and a warning is logged,
 *       so segment creation succeeds rather than failing on a transient network error.</li>
 * </ul>
 *
 * <p>Thread-safety: {@link #JAR_CACHE} is a {@link ConcurrentHashMap}. Concurrent {@link #init} calls for the
 * same topic may both fetch the JAR — this is safe (both writes produce equivalent local copies) and rare
 * (only on the very first init per topic per JVM lifetime, or after a config change).
 */
public class ProtoBufCodeGenMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProtoBufCodeGenMessageDecoder.class);

  public static final String PROTOBUF_JAR_FILE_PATH = "jarFile";
  public static final String PROTO_CLASS_NAME = "protoClassName";

  /**
   * Holds a locally cached copy of the remote JAR alongside the remote path it was fetched from.
   * The remote path is used to detect config changes (new JAR deployed) and trigger a re-fetch.
   */
  private static final class CachedJar {
    final String _jarPath;
    final File _localFile;

    CachedJar(String jarPath, File localFile) {
      _jarPath = jarPath;
      _localFile = localFile;
    }
  }

  // JVM-level cache: topicName → locally cached JAR file.
  // Eliminates repeated remote fetches across segment rollovers for the same topic.
  private static final ConcurrentHashMap<String, CachedJar> JAR_CACHE = new ConcurrentHashMap<>();

  private Method _decodeMethod;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    Preconditions.checkState(props.containsKey(PROTOBUF_JAR_FILE_PATH),
        "Protocol Buffer schema jar file must be provided");
    Preconditions.checkState(props.containsKey(PROTO_CLASS_NAME),
        "Protocol Buffer Message class name must be provided");
    String jarPath = props.get(PROTOBUF_JAR_FILE_PATH);
    String protoClassName = props.get(PROTO_CLASS_NAME);

    File localFile = resolveJar(topicName, jarPath);
    URLClassLoader loader = new URLClassLoader(new URL[]{localFile.toURI().toURL()});
    Descriptors.Descriptor descriptor = getDescriptorForProtoClass(loader, protoClassName);
    String codeGenCode = new MessageCodeGen().codegen(descriptor, fieldsToRead);
    Class<?> recordExtractor = compileClass(loader,
        MessageCodeGen.EXTRACTOR_PACKAGE_NAME + "." + MessageCodeGen.EXTRACTOR_CLASS_NAME, codeGenCode);
    _decodeMethod = recordExtractor.getMethod(MessageCodeGen.EXTRACTOR_METHOD_NAME, byte[].class, GenericRow.class);
    loader.close();
  }

  /**
   * Returns the local {@link File} for the given {@code jarPath}, fetching it from remote storage if needed.
   *
   * <p>Fast path: if the cache already holds a local copy for this topic with the same remote path, return it
   * immediately with no network I/O.
   *
   * <p>Slow path: fetch the JAR, update the cache, register the temp dir for deletion on JVM exit.
   *
   * <p>Stale fallback: if the fetch fails and a prior local copy exists (e.g. from an older {@code jarPath}),
   * log a warning and return the stale file so segment creation can proceed.
   */
  private static File resolveJar(String topicName, String jarPath)
      throws Exception {
    CachedJar cached = JAR_CACHE.get(topicName);
    if (cached != null && cached._jarPath.equals(jarPath) && cached._localFile.exists()) {
      return cached._localFile;
    }
    try {
      File localFile = ProtoBufUtils.getFileCopiedToLocal(jarPath);
      localFile.getParentFile().deleteOnExit();
      JAR_CACHE.put(topicName, new CachedJar(jarPath, localFile));
      return localFile;
    } catch (Exception e) {
      if (cached != null && cached._localFile.exists()) {
        LOGGER.error("Failed to fetch JAR for topic '{}' from '{}', reusing stale local copy from '{}'. "
                + "Rows decoded with the stale schema may be incorrect if the schema has changed.",
            topicName, jarPath, cached._jarPath, e);
        return cached._localFile;
      }
      throw e;
    }
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

  /** Clears the JAR cache. For use in tests only. */
  @VisibleForTesting
  static void clearCacheForTest() {
    JAR_CACHE.clear();
  }
}
