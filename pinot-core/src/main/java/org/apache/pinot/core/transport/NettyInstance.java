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

package org.apache.pinot.core.transport;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.AbstractMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Represents an instance of Netty, allowing access to certain static properties via reflection, with support for
/// shaded Netty versions.
///
/// We know 3 common Netty instances:
/// - Unshaded Netty, which uses the standard `io.netty` package
/// - Pinot-shaded Netty, shaded by Pinot (only when the shade profile is enabled).
///     It uses `org.apache.pinot.shaded.io.netty` package.
/// - gRPC-shaded Netty, shaded by gRPC and included as a dependency. It uses `io.grpc.netty.shaded.io.netty` package.
///
/// This is important because Netty defines is not designed to be shade, and it uses some static attributes to determine
/// its behavior, specially whether it can use `Unsafe` or not or how much memory to allocate for direct buffers.
/// These attributes are set using JAVA_OPTs. Each shaded version uses different JAVA_OPT properties. If we forget to
/// set one of these properties for a shaded version, that Netty _instance_ will fall back to some default behavior that
/// may not be optimal, and we won't have any indication of that happening.
///
/// By using reflection to access these properties, we can log their values at startup and check if they are set to the
/// expected values, logging warnings if they are not.
/// This allows us to catch misconfigurations early.
///
/// **It is critical to not shade this class**, otherwise the literals used for reflection
/// (ie `io.netty.util.internal.PlatformDependent`) will be shaded too, so instead of looking for
/// `io.grpc.netty.shaded.io.netty.util.internal.PlatformDependent`, as you may think reading this class, the shaded
/// version of this class will look for
/// `io.grpc.netty.shaded.org.apache.pinot.shaded.io.netty.util.internal.PlatformDependent`.
/// At the moment this is written, Pinot _does not_ shade Netty, so it is safe. Just to be sure, this class should be
/// excluded in the maven shade plugin configuration (see pom.xml on the root of the project).
public class NettyInstance {
  private static final Logger LOGGER = LoggerFactory.getLogger(NettyInstance.class);
  /// We use a CopyOnWriteArrayList to allow dynamic addition of Netty instances at runtime,
  /// if needed (e.g. if we want to support other shaded versions of Netty).
  public static final CopyOnWriteArrayList<NettyInstance> KNOWN_INSTANCES;

  static {
    List<NettyInstance> instances = new ArrayList<>(3);
    NettyInstance unshaded = NettyInstance.create("Unshaded", "");
    if (unshaded != null) {
      instances.add(unshaded);
    } else {
      LOGGER.warn("Unshaded Netty instance not found, if Netty is shade we need to add a new NettyInstance.");
    }
    NettyInstance gRPC = NettyInstance.create("gRPC", "io.grpc.netty.shaded.");
    if (gRPC != null) {
      instances.add(gRPC);
    } else {
      LOGGER.warn("gRPC-shaded Netty instance not found, unable to inspect Netty constants for gRPC-shaded Netty. "
          + "Make sure the gRPC dependency is included and the Netty classes are included in the gRPC shade.");
    }
    KNOWN_INSTANCES = new CopyOnWriteArrayList<>(instances);
  }

  private final String _name;
  private final String _shadePrefix;

  private static final String PLATFORM_DEPENDENT_CLASS_NAME = "io.netty.util.internal.PlatformDependent";
  private static final String PLATFORM_DEPENDENT0_CLASS_NAME = "io.netty.util.internal.PlatformDependent0";
  private static final String POOLED_BYTE_BUF_ALLOCATOR_CLASS_NAME = "io.netty.buffer.PooledByteBufAllocator";

  /// Creates a new Netty instance with the given name and shade prefix.
  /// This instance won't be added to the list of known instances, so it won't be logged or monitored.
  /// In order to do so the caller needs to add it to the KNOWN_INSTANCES list after creating it.
  ///
  /// @param name The name of the Netty instance. It will be used on logs but also on metric names, so it should be
  ///             something short and without spaces like "Unshaded", "Pinot", "gRPC".
  ///             Add underscores if you need to separate words, but avoid other special characters.
  /// @param shadePrefix The prefix of the package where the Netty classes are located.
  ///             For example, for unshaded Netty it would be `""`, for grpc-shaded Netty it would be
  ///             `"io.grpc.netty.shaded."`.
  public NettyInstance(String name, String shadePrefix)
      throws ClassNotFoundException {
    _name = name;
    _shadePrefix = shadePrefix;
  }

  public static void registerMetrics(AbstractMetrics<?, ?, ?, ?> metrics) {
    for (NettyInstance instance : KNOWN_INSTANCES) {
      metrics.setOrUpdateGauge(instance._name + "_netty_direct_memory_used",
          () -> instance.getUsedDirectMemory().orElse(-1L));
      metrics.setOrUpdateGauge(instance._name + "_netty_direct_memory_max",
          () -> instance.getMaxDirectMemory().orElse(-1L));
    }
  }

  /// Logs the memory used by each known Netty instance as long as the max memory it can use.
  /// It also logs the total memory used by all Netty instances and its max memory.
  public static void logMemory() {
    long totalUsedMemory = 0;
    long totalMaxMemory = 0;
    for (NettyInstance instance : NettyInstance.KNOWN_INSTANCES) {
      Optional<Long> usedMemoryOpt = instance.getUsedDirectMemory();
      Optional<Long> maxMemoryOpt = instance.getMaxDirectMemory();
      if (usedMemoryOpt.isPresent() && maxMemoryOpt.isPresent()) {
        long usedMemory = usedMemoryOpt.get();
        long maxMemory = maxMemoryOpt.get();
        totalUsedMemory += usedMemory;
        totalMaxMemory += maxMemory;
        LOGGER.info("Netty instance '{}' is using {} of direct memory (max {}).", instance.getName(),
            humanReadableByteCount(usedMemory), humanReadableByteCount(maxMemory));
      } else {
        LOGGER.warn("Could not determine direct memory usage for Netty instance '{}'.", instance.getName());
      }
    }
    LOGGER.info("Total direct memory used by all Netty instances: {} (max {}).",
        humanReadableByteCount(totalUsedMemory), humanReadableByteCount(totalMaxMemory));
  }

  private static String humanReadableByteCount(long bytes) {
    if (bytes < 1024) {
      return bytes + " B";
    }
    int exp = (int) (Math.log(bytes) / Math.log(1024));
    String pre = "KMGTPE".charAt(exp - 1) + "B";
    return String.format("%.1f %s", bytes / Math.pow(1024, exp), pre);
  }

  public String getName() {
    return _name;
  }

  public String getShadePrefix() {
    return _shadePrefix;
  }

  @Nullable
  public static NettyInstance create(String name, String shadePrefix) {
    try {
      Class.forName(shadePrefix + PLATFORM_DEPENDENT0_CLASS_NAME);
      return new NettyInstance(name, shadePrefix);
    } catch (ClassNotFoundException e) {
      LOGGER.debug("Netty class not found for instance '{}', cannot create.", name);
      return null;
    }
  }

  public Optional<Boolean> isExplicitTryReflectionSetAccessible() {
    return getStaticProperty(
        PLATFORM_DEPENDENT0_CLASS_NAME,
        "isExplicitTryReflectionSetAccessible",
        "isExplicitTryReflectionSetAccessible",
        "IS_EXPLICIT_TRY_REFLECTION_SET_ACCESSIBLE",
        Boolean.class
    );
  }

  public Optional<Integer> getDirectArenas() {
    return getStaticProperty(
        POOLED_BYTE_BUF_ALLOCATOR_CLASS_NAME,
        "DEFAULT_NUM_DIRECT_ARENA",
        "DEFAULT_NUM_DIRECT_ARENA",
        "DEFAULT_NUM_DIRECT_ARENA",
        Integer.class
    );
  }

  public Optional<Integer> getHeapArenas() {
    return getStaticProperty(
        POOLED_BYTE_BUF_ALLOCATOR_CLASS_NAME,
        "DEFAULT_NUM_HEAP_ARENA",
        "DEFAULT_NUM_HEAP_ARENA",
        "DEFAULT_NUM_HEAP_ARENA",
        Integer.class
    );
  }

  public Optional<Integer> getPageSize() {
    return getStaticProperty(
        POOLED_BYTE_BUF_ALLOCATOR_CLASS_NAME,
        "DEFAULT_PAGE_SIZE",
        "DEFAULT_PAGE_SIZE",
        "DEFAULT_PAGE_SIZE",
        Integer.class
    );
  }

  public Optional<Integer> getDirectArenaChunkSize() {
    return getStaticProperty(
        POOLED_BYTE_BUF_ALLOCATOR_CLASS_NAME,
        "DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK",
        "DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK",
        "DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK",
        Integer.class
    );
  }

  public Optional<Integer> getDirectArenaMaxOrder() {
    return getStaticProperty(
        POOLED_BYTE_BUF_ALLOCATOR_CLASS_NAME,
        "DEFAULT_MAX_ORDER",
        "DEFAULT_MAX_ORDER",
        "DEFAULT_MAX_ORDER",
        Integer.class
    );
  }

  public Optional<Long> getUsedDirectMemory() {
    return getStaticProperty(
        PLATFORM_DEPENDENT_CLASS_NAME,
        "usedDirectMemory",
        "usedDirectMemory",
        null,
        Long.class
    );
  }

  public Optional<Long> getMaxDirectMemory() {
    return getStaticProperty(
        PLATFORM_DEPENDENT_CLASS_NAME,
        "maxDirectMemory",
        "maxDirectMemory",
        "DIRECT_MEMORY_LIMIT",
        Long.class
    );
  }

  public Optional<Long> getEstimateMaxDirectMemory() {
    return getStaticProperty(
        PLATFORM_DEPENDENT_CLASS_NAME,
        "estimateMaxDirectMemory",
        "estimateMaxDirectMemory",
        null,
        Long.class
    );
  }

  private <E> Optional<E> getStaticProperty(
      String className, String propName, String methodName, String fieldName, Class<E> type) {
    Class<?> clazz;
    try {
      clazz = Class.forName(_shadePrefix + className);
    } catch (ClassNotFoundException e) {
      LOGGER.debug("Class '{}' not found for property '{}', cannot access.", className, propName);
      return Optional.empty();
    }
    try {
      // Try method first
      try {
        Method m = clazz.getDeclaredMethod(methodName);
        m.setAccessible(true);
        return Optional.ofNullable(m.invoke(null)).map(type::cast);
      } catch (NoSuchMethodException ignored) {
        // Not a method, try field
      }
      Field f = clazz.getDeclaredField(fieldName);
      f.setAccessible(true);
      return Optional.ofNullable(f.get(null)).map(type::cast);
    } catch (Exception e) {
      LOGGER.warn("Unable to access property '{}' on class '{}': {}.", fieldName, clazz.getName(), e.getMessage());
      return Optional.empty();
    }
  }
}
