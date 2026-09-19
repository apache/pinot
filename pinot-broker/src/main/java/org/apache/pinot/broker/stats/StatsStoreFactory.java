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
package org.apache.pinot.broker.stats;

import com.google.common.annotations.VisibleForTesting;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.query.planner.spi.stats.StatsStore;
import org.apache.pinot.query.planner.spi.stats.StatsStoreException;
import org.apache.pinot.query.planner.spi.stats.StatsStoreProvider;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Resolves the configured [Broker#CONFIG_OF_STATS_STORE] name to a [StatsStore], over the
/// [StatsStoreProvider]s found on the classpath.
///
/// Stores are selected by name rather than class name so that an implementation can be renamed or
/// moved without invalidating an operator's configuration, and so an extension can contribute a
/// store without this class knowing about it.
///
/// Thread-safety: stateless; providers are discovered per call, which happens once per broker.
public final class StatsStoreFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(StatsStoreFactory.class);

  private StatsStoreFactory() {
  }

  /// Creates an un-initialized store for the configured name, or the default when blank.
  ///
  /// @param storeName  configured value, may be `null` or blank
  /// @param properties statistics configuration with the `pinot.broker.stats.` prefix stripped
  /// A name no provider declares is operator error, so it throws [IllegalArgumentException] and
  /// fails broker startup rather than quietly running without the statistics that were asked for.
  /// A provider that fails to build its store is a different matter — that surfaces as
  /// [StatsStoreException], which the caller degrades to statistics-disabled.
  ///
  /// @throws IllegalArgumentException if no provider declares that name, listing what is available
  ///         so a typo is immediately diagnosable
  /// @throws StatsStoreException if the provider cannot build a store from this configuration
  public static StatsStore create(@Nullable String storeName, Map<String, String> properties)
      throws StatsStoreException {
    String effectiveName = StringUtils.isBlank(storeName) ? Broker.DEFAULT_STATS_STORE : storeName;
    Map<String, StatsStoreProvider> providers = discoverProviders();
    StatsStoreProvider provider = providers.get(effectiveName.toLowerCase(Locale.ROOT));
    if (provider == null) {
      throw new IllegalArgumentException("No stats store named '" + effectiveName + "' (configured by "
          + Broker.CONFIG_OF_STATS_STORE + "); available: " + providers.keySet());
    }
    LOGGER.info("Using '{}' stats store", effectiveName);
    return createFrom(provider, effectiveName, properties);
  }

  /// Invokes a provider, normalising how its failures surface.
  ///
  /// A provider's own failure must not carry the same weight as an operator typo: the typo fails
  /// broker startup, while this stays on the degrade-to-no-statistics path.
  @VisibleForTesting
  static StatsStore createFrom(StatsStoreProvider provider, String name, Map<String, String> properties)
      throws StatsStoreException {
    try {
      return provider.create(properties);
    } catch (StatsStoreException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new StatsStoreException("Provider '" + name + "' failed to create a stats store", e);
    }
  }

  /// Discovers providers, keyed by lower-cased name so configuration is case-insensitive.
  ///
  /// Enumerates the context classloader and every plugin realm, because a provider contributed by a
  /// plugin is invisible to the context loader alone — which would make the documented extension
  /// seam unusable. Mirrors `PinotRuleSet.loadFromServiceLoader()`. Call after plugins are loaded.
  private static Map<String, StatsStoreProvider> discoverProviders() {
    Map<String, StatsStoreProvider> providers = new TreeMap<>();
    // Dedup by class name first: the context classloader and a plugin realm may both see the same
    // META-INF/services entry (fat jar plus plugin), which is not a genuine name collision.
    Set<String> seen = new LinkedHashSet<>();
    collectProviders(ServiceLoader.load(StatsStoreProvider.class), seen, providers);
    for (ClassLoader pluginClassLoader : PluginManager.get().getPluginClassLoaders()) {
      collectProviders(ServiceLoader.load(StatsStoreProvider.class, pluginClassLoader), seen, providers);
    }
    return providers;
  }

  @VisibleForTesting
  static void collectProviders(Iterable<StatsStoreProvider> discovered, Set<String> seen,
      Map<String, StatsStoreProvider> providers) {
    for (StatsStoreProvider provider : discovered) {
      if (!seen.add(provider.getClass().getName())) {
        continue;
      }
      String name = provider.getName().toLowerCase(Locale.ROOT);
      StatsStoreProvider previous = providers.put(name, provider);
      if (previous != null) {
        // Two different providers answering to one name means configuration silently picks one.
        throw new IllegalStateException("Duplicate StatsStoreProvider name '" + name + "': "
            + previous.getClass().getName() + " and " + provider.getClass().getName());
      }
    }
  }
}
