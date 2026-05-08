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
package org.apache.pinot.segment.spi.partition;

import com.google.common.base.Preconditions;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Dynamic registry for [PartitionFunction] implementations.
///
/// Discovery walks every public, concrete [PartitionFunction] subtype on the classpath under the
/// `org.apache.pinot.*` package tree and registers each under the names returned by
/// [PartitionFunction#getNames()] (defaults to `[getName()]`; override to declare aliases — e.g.
/// `MurmurPartitionFunction` registers under both `Murmur` and `Murmur2`).
///
/// Each registrable class must be public, concrete, and expose a public constructor with signature
/// `(int numPartitions, Map<String, String> functionConfig)` — the constructor used both for the
/// startup `getNames()` probe (called with `(1, null)`) and for [#getPartitionFunction(String, int,
/// Map)] at lookup time.
///
/// The static block scans the classpath once and builds an immutable (canonicalized name →
/// constructor) map. To force eager initialization (e.g. so the scan happens before the first
/// segment is read), call [#init()] from broker / server / controller startup.
public class PartitionFunctionFactory {
  private PartitionFunctionFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionFunctionFactory.class);
  private static final String SCAN_PACKAGE = "org.apache.pinot";

  private static final Map<String, Constructor<? extends PartitionFunction>> REGISTRY;

  static {
    long startTimeMs = System.currentTimeMillis();
    Map<String, Constructor<? extends PartitionFunction>> registry = new HashMap<>();
    Set<Class<? extends PartitionFunction>> subtypes = scanSubtypes();
    for (Class<? extends PartitionFunction> clazz : subtypes) {
      int mods = clazz.getModifiers();
      if (!Modifier.isPublic(mods) || Modifier.isAbstract(mods) || clazz.isInterface()) {
        continue;
      }
      Constructor<? extends PartitionFunction> constructor;
      try {
        constructor = clazz.getConstructor(int.class, Map.class);
      } catch (NoSuchMethodException e) {
        LOGGER.warn("Skipping {}: missing public constructor (int, Map<String, String>)", clazz.getName());
        continue;
      }
      List<String> names = probeNames(clazz, constructor);
      if (names == null) {
        continue;
      }
      for (String name : names) {
        if (name == null || name.trim().isEmpty()) {
          LOGGER.warn("Skipping blank name for {}", clazz.getName());
          continue;
        }
        String canonical = canonicalize(name.trim());
        Constructor<? extends PartitionFunction> existing = registry.put(canonical, constructor);
        Preconditions.checkState(existing == null || existing.getDeclaringClass().equals(clazz),
            "Partition function name '%s' is registered to both %s and %s", name,
            existing == null ? null : existing.getDeclaringClass().getName(), clazz.getName());
      }
    }
    REGISTRY = Collections.unmodifiableMap(registry);
    LOGGER.info("Initialized PartitionFunctionFactory with {} functions: {} in {}ms", REGISTRY.size(),
        REGISTRY.keySet(), System.currentTimeMillis() - startTimeMs);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Set<Class<? extends PartitionFunction>> scanSubtypes() {
    final Set<?>[] result = new Set<?>[1];
    PinotReflectionUtils.runWithLock(() ->
        result[0] = new Reflections(new ConfigurationBuilder()
            .setUrls(ClasspathHelper.forPackage(SCAN_PACKAGE))
            .setScanners(new SubTypesScanner()))
            .getSubTypesOf(PartitionFunction.class));
    return (Set) result[0];
  }

  /// Instantiates `clazz` with `(numPartitions = 1, functionConfig = null)` and returns the
  /// names list reported by [PartitionFunction#getNames()]. Returns `null` (with a warning log)
  /// when the probe construction fails — typically a function whose ctor requires non-null config
  /// (e.g. `BoundedColumnValuePartitionFunction`); such functions need to either supply a usable
  /// default config or skip auto-registration.
  @Nullable
  private static List<String> probeNames(Class<? extends PartitionFunction> clazz,
      Constructor<? extends PartitionFunction> constructor) {
    try {
      PartitionFunction probe = constructor.newInstance(1, null);
      List<String> names = probe.getNames();
      if (names == null || names.isEmpty()) {
        LOGGER.warn("Skipping {}: getNames() returned null/empty", clazz.getName());
        return null;
      }
      return names;
    } catch (ReflectiveOperationException e) {
      LOGGER.warn("Skipping {}: probing getNames() with (1, null) failed: {}", clazz.getName(),
          e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
      return null;
    }
  }

  /// No-op call that exists to force the static initializer of this class to run. Mirrors
  /// `FunctionRegistry.init()` so callers can eagerly trigger classpath scanning during
  /// service startup instead of paying the cost on the first partition function lookup.
  public static void init() {
  }

  /// Builds an instance of the partition function registered under `functionName`.
  ///
  /// @param functionName matched case-insensitively
  /// @param numPartitions positive partition count
  /// @param functionConfig optional, function-specific configuration; may be `null`
  public static PartitionFunction getPartitionFunction(String functionName, int numPartitions,
      @Nullable Map<String, String> functionConfig) {
    Constructor<? extends PartitionFunction> constructor = REGISTRY.get(canonicalize(functionName));
    Preconditions.checkArgument(constructor != null, "No partition function registered for name: %s", functionName);
    try {
      return constructor.newInstance(numPartitions, functionConfig);
    } catch (ReflectiveOperationException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new IllegalStateException(
          "Failed to instantiate partition function '" + functionName + "' with " + numPartitions + " partitions",
          cause);
    }
  }

  public static PartitionFunction getPartitionFunction(ColumnPartitionConfig config) {
    return getPartitionFunction(config.getFunctionName(), config.getNumPartitions(), config.getFunctionConfig());
  }

  public static PartitionFunction getPartitionFunction(ColumnPartitionMetadata metadata) {
    return getPartitionFunction(metadata.getFunctionName(), metadata.getNumPartitions(), metadata.getFunctionConfig());
  }

  private static String canonicalize(String name) {
    return name.toLowerCase(Locale.ROOT);
  }
}
