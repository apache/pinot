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
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.annotations.PartitionFunctionType;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Dynamic registry for {@link PartitionFunction} implementations.
 *
 * <p>Discovery is driven by classpath scanning for classes annotated with
 * {@link PartitionFunctionType}. Annotated classes must:
 * <ul>
 *   <li>be public and implement {@link PartitionFunction}</li>
 *   <li>live under a package matching the regex {@code .*\.partition\.function\..*}
 *       (e.g. {@code org.apache.pinot.common.partition.function} or any plugin package
 *       that follows the same convention)</li>
 *   <li>expose a public constructor with signature
 *       {@code (int numPartitions, Map<String, String> functionConfig)}</li>
 * </ul>
 *
 * <p>The static block scans the classpath once and builds an immutable
 * (canonicalized name → constructor) map. Instances are created on demand by
 * {@link #getPartitionFunction(String, int, Map)}.
 *
 * <p>To force eager initialization (e.g. so the scan happens before the first segment
 * is read), call {@link #init()} from broker/server/controller startup.
 */
public class PartitionFunctionFactory {
  private PartitionFunctionFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionFunctionFactory.class);
  private static final String SCAN_REGEX = ".*\\.partition\\.function\\..*";

  private static final Map<String, Constructor<? extends PartitionFunction>> REGISTRY;

  static {
    long startTimeMs = System.currentTimeMillis();
    Map<String, Constructor<? extends PartitionFunction>> registry = new HashMap<>();
    for (Class<?> clazz : PinotReflectionUtils.getClassesThroughReflection(SCAN_REGEX, PartitionFunctionType.class)) {
      if (!Modifier.isPublic(clazz.getModifiers()) || !PartitionFunction.class.isAssignableFrom(clazz)) {
        continue;
      }
      PartitionFunctionType annotation = clazz.getAnnotation(PartitionFunctionType.class);
      if (!annotation.enabled()) {
        continue;
      }
      String[] names = annotation.names();
      Preconditions.checkState(names.length > 0,
          "@PartitionFunctionType on %s must declare at least one name", clazz.getName());
      Constructor<? extends PartitionFunction> constructor;
      try {
        @SuppressWarnings("unchecked")
        Constructor<? extends PartitionFunction> ctor =
            (Constructor<? extends PartitionFunction>) clazz.getConstructor(int.class, Map.class);
        constructor = ctor;
      } catch (NoSuchMethodException e) {
        throw new IllegalStateException("Partition function " + clazz.getName()
            + " must declare a public constructor (int numPartitions, Map<String, String> functionConfig)", e);
      }
      for (String name : names) {
        String canonical = canonicalize(name);
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

  /**
   * No-op call that exists to force the static initializer of this class to run. Mirrors
   * {@code FunctionRegistry.init()} so callers can eagerly trigger classpath scanning during
   * service startup instead of paying the cost on the first partition function lookup.
   */
  public static void init() {
  }

  /**
   * Builds an instance of the partition function registered under {@code functionName}.
   *
   * @param functionName matched case-insensitively (after stripping underscores)
   * @param numPartitions positive partition count
   * @param functionConfig optional, function-specific configuration; may be {@code null}
   */
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
