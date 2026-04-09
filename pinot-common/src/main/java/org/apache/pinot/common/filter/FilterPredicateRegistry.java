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

import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for custom filter predicate plugins.
 *
 * <p>Plugins are discovered automatically via {@link ServiceLoader} during {@link #init()},
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
   * Discovers and registers all {@link FilterPredicatePlugin} implementations found via ServiceLoader.
   * Safe to call multiple times; subsequent calls after the first are no-ops.
   */
  public static void init() {
    if (!INITIALIZED.compareAndSet(false, true)) {
      return;
    }
    ServiceLoader<FilterPredicatePlugin> loader = ServiceLoader.load(FilterPredicatePlugin.class);
    for (FilterPredicatePlugin plugin : loader) {
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
    String name = plugin.name().toUpperCase(Locale.ROOT);
    FilterPredicatePlugin existing = REGISTRY.putIfAbsent(name, plugin);
    if (existing != null && existing != plugin) {
      throw new IllegalStateException(
          String.format("Cannot register FilterPredicatePlugin '%s' (%s): already registered by %s",
              name, plugin.getClass().getName(), existing.getClass().getName()));
    }
    LOGGER.info("Registered custom filter predicate plugin: {} ({})", name, plugin.getClass().getName());
  }

  /**
   * Returns the plugin registered for the given filter name, or null if none is registered.
   */
  @Nullable
  public static FilterPredicatePlugin get(String name) {
    return REGISTRY.get(name.toUpperCase(Locale.ROOT));
  }

  /**
   * Returns true if a custom filter predicate plugin is registered for the given name.
   */
  public static boolean isRegistered(String name) {
    return REGISTRY.containsKey(name.toUpperCase(Locale.ROOT));
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
}
