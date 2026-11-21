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
package org.apache.pinot.query.planner.optimizer;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.query.planner.spi.QueryOptimizerProvider;
import org.apache.pinot.spi.plugin.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Registry of cost-based optimizer plugins, keyed by provider name.
///
/// Built once at broker startup; the name-to-provider map is immutable for the
/// process lifetime. Mirrors the discovery mechanism of
/// [org.apache.pinot.query.planner.rules.PinotRuleSet]: providers are loaded
/// from [java.util.ServiceLoader] over the context classloader and over every
/// [PluginManager#getPluginClassLoaders()], deduplicated by class name.
public final class QueryOptimizerRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryOptimizerRegistry.class);

  private final Map<String, QueryOptimizerProvider> _providers;

  public QueryOptimizerRegistry(Iterable<QueryOptimizerProvider> providers) {
    Map<String, QueryOptimizerProvider> byName = new LinkedHashMap<>();
    for (QueryOptimizerProvider provider : providers) {
      QueryOptimizerProvider previous = byName.put(provider.getName(), provider);
      if (previous != null) {
        LOGGER.warn("Query optimizer '{}' is provided by both {} and {}; the latter wins", provider.getName(),
            previous.getClass().getName(), provider.getClass().getName());
      }
    }
    _providers = Collections.unmodifiableMap(byName);
  }

  /// Discovers every [QueryOptimizerProvider] via [ServiceLoader]. Used by
  /// [#defaultInstance()] and by callers that manage their own provider list.
  ///
  /// Call after all plugins have been loaded via [PluginManager]; plugins
  /// loaded after this method returns will not be included.
  public static QueryOptimizerRegistry loadFromServiceLoader() {
    return new QueryOptimizerRegistry(discoverProviders(PluginManager.get().getPluginClassLoaders()));
  }

  /// Discovers providers over the context classloader and the given plugin classloaders,
  /// deduplicated by class name.
  @VisibleForTesting
  static List<QueryOptimizerProvider> discoverProviders(Iterable<ClassLoader> pluginClassLoaders) {
    List<QueryOptimizerProvider> providers = new ArrayList<>();
    // Dedup by class name: the context classloader and a plugin classloader may both see the same
    // META-INF/services file if their classpaths overlap (e.g. fat-jar + plugin realm).
    Set<String> seen = new LinkedHashSet<>();
    for (QueryOptimizerProvider provider : ServiceLoader.load(QueryOptimizerProvider.class)) {
      if (seen.add(provider.getClass().getName())) {
        providers.add(provider);
      }
    }
    for (ClassLoader pluginClassLoader : pluginClassLoaders) {
      for (QueryOptimizerProvider provider : ServiceLoader.load(QueryOptimizerProvider.class, pluginClassLoader)) {
        if (seen.add(provider.getClass().getName())) {
          providers.add(provider);
        }
      }
    }
    return providers;
  }

  /// Lazily-initialized process-wide singleton built from the
  /// `ServiceLoader`-discovered providers. Used as the `@Value.Default` of
  /// `QueryEnvironment.Config#getOptimizerRegistry()` so per-query `Config`
  /// instances don't repeat the discovery work.
  public static QueryOptimizerRegistry defaultInstance() {
    return DefaultInstanceHolder.INSTANCE;
  }

  /// Returns the provider registered under the given name, or `null` when no
  /// provider is registered under it.
  @Nullable
  public QueryOptimizerProvider get(String name) {
    return _providers.get(name);
  }

  /// Returns the names of all registered providers.
  public Set<String> getNames() {
    return _providers.keySet();
  }

  private static final class DefaultInstanceHolder {
    static final QueryOptimizerRegistry INSTANCE = loadFromServiceLoader();
  }
}
