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
package org.apache.pinot.common.filter;

import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.sql.FilterKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for custom filter predicate plugins.
 *
 * <p>Plugins are discovered automatically via {@link PluginManager} during {@link #init()},
 * and can also be registered programmatically via {@link #register(FilterPredicatePlugin)}.
 *
 * <p>This registry is consulted at multiple points in the query pipeline when the filter name
 * is not a built-in {@code FilterKind}:
 * <ul>
 *   <li>{@code CalciteSqlParser.validateFilter()} — filter validation</li>
 *   <li>{@code PredicateComparisonRewriter} — expression rewriting</li>
 *   <li>{@code RequestContextUtils.getFilterInner()} — predicate creation</li>
 *   <li>{@code PinotOperatorTable} — Calcite SQL operator registration</li>
 * </ul>
 */
public class FilterPredicateRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(FilterPredicateRegistry.class);
  private static final Map<String, FilterPredicatePlugin> REGISTRY = new ConcurrentHashMap<>();
  private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);

  private FilterPredicateRegistry() {
  }

  /**
   * Discovers and registers all {@link FilterPredicatePlugin} implementations visible from the application classpath
   * and loaded Pinot plugins. Safe to call multiple times; subsequent calls after the first are no-ops.
   */
  public static void init() {
    if (!INITIALIZED.compareAndSet(false, true)) {
      return;
    }
    for (FilterPredicatePlugin plugin : PluginManager.get().loadServices(FilterPredicatePlugin.class)) {
      register(plugin);
    }
  }

  /**
   * Programmatically registers a custom filter predicate plugin.
   *
   * @param plugin the plugin to register
   * @throws IllegalStateException if a different plugin is already registered with the same name
   */
  public static void register(FilterPredicatePlugin plugin) {
    String canonicalName = canonicalize(plugin.name());
    Preconditions.checkState(!isBuiltInName(canonicalName),
        "Cannot register FilterPredicatePlugin '%s' (%s): name collides with a built-in Pinot predicate or function",
        plugin.name(), plugin.getClass().getName());
    FilterPredicatePlugin existing = REGISTRY.putIfAbsent(canonicalName, plugin);
    if (existing != null && existing != plugin) {
      throw new IllegalStateException(
          String.format("Cannot register FilterPredicatePlugin '%s' (%s): already registered by %s",
              plugin.name(), plugin.getClass().getName(), existing.getClass().getName()));
    }
    LOGGER.info("Registered custom filter predicate plugin: {} ({})", plugin.name(), plugin.getClass().getName());
  }

  /**
   * Returns the plugin registered for the given filter name, or null if none is registered.
   */
  @Nullable
  public static FilterPredicatePlugin get(String name) {
    return REGISTRY.get(canonicalize(name));
  }

  /**
   * Returns true if a custom filter predicate plugin is registered for the given name.
   */
  public static boolean isRegistered(String name) {
    return REGISTRY.containsKey(canonicalize(name));
  }

  /**
   * Returns all registered plugins.
   */
  public static Collection<FilterPredicatePlugin> getAllPlugins() {
    return REGISTRY.values();
  }

  /**
   * Removes all registered plugins. Intended for testing only.
   */
  public static void clear() {
    REGISTRY.clear();
    INITIALIZED.set(false);
  }

  private static String canonicalize(String name) {
    return FunctionRegistry.canonicalize(name);
  }

  private static boolean isBuiltInName(String canonicalName) {
    if (FunctionRegistry.contains(canonicalName)) {
      return true;
    }
    for (FilterKind filterKind : FilterKind.values()) {
      if (canonicalize(filterKind.name()).equals(canonicalName)) {
        return true;
      }
    }
    for (TransformFunctionType functionType : TransformFunctionType.values()) {
      for (String name : functionType.getNames()) {
        if (canonicalize(name).equals(canonicalName)) {
          return true;
        }
      }
    }
    for (AggregationFunctionType functionType : AggregationFunctionType.values()) {
      if (canonicalize(functionType.getName()).equals(canonicalName)) {
        return true;
      }
    }
    return false;
  }
}
