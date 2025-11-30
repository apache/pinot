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
package org.apache.pinot.common.systemtable;

import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.helix.HelixAdmin;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.spi.systemtable.SystemTable;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry to hold and lifecycle-manage system table data providers.
 */
public final class SystemTableRegistry implements AutoCloseable {
  public static final SystemTableRegistry INSTANCE = new SystemTableRegistry();
  private static final Logger LOGGER = LoggerFactory.getLogger(SystemTableRegistry.class);

  private final Map<String, SystemTableDataProvider> _providers = new ConcurrentHashMap<>();

  public void register(SystemTableDataProvider provider) {
    _providers.put(normalize(provider.getTableName()), provider);
  }

  public @Nullable SystemTableDataProvider get(String tableName) {
    return _providers.get(normalize(tableName));
  }

  public boolean isRegistered(String tableName) {
    return _providers.containsKey(normalize(tableName));
  }

  public Collection<SystemTableDataProvider> getProviders() {
    return Collections.unmodifiableCollection(_providers.values());
  }

  /**
   * Discover and register providers marked with {@link SystemTable} using the available dependencies.
   * Follows the ScalarFunction pattern: any class annotated with @SystemTable under a "*.systemtable.*" package
   * will be discovered via reflection and registered.
   */
  public void registerAnnotatedProviders(TableCache tableCache, HelixAdmin helixAdmin, String clusterName) {
    Set<Class<?>> classes =
        PinotReflectionUtils.getClassesThroughReflection(".*\\.systemtable\\..*", SystemTable.class);
    for (Class<?> clazz : classes) {
      if (!SystemTableDataProvider.class.isAssignableFrom(clazz)) {
        continue;
      }
      Optional<SystemTableDataProvider> instantiated =
          instantiateProvider((Class<? extends SystemTableDataProvider>) clazz, tableCache, helixAdmin, clusterName);
      instantiated.ifPresent(provider -> {
        if (isRegistered(provider.getTableName())) {
          return;
        }
        LOGGER.info("Registering system table provider: {}", provider.getTableName());
        register(provider);
      });
    }
  }

  @Override
  public void close()
      throws Exception {
    for (SystemTableDataProvider provider : _providers.values()) {
      provider.close();
    }
  }

  /**
   * Initialize and register all annotated system table providers.
   */
  public void init(TableCache tableCache, HelixAdmin helixAdmin, String clusterName) {
    registerAnnotatedProviders(tableCache, helixAdmin, clusterName);
  }

  private static String normalize(String tableName) {
    return tableName.toLowerCase(Locale.ROOT);
  }

  private Optional<SystemTableDataProvider> instantiateProvider(Class<? extends SystemTableDataProvider> clazz,
      TableCache tableCache, HelixAdmin helixAdmin, String clusterName) {
    try {
      // Prefer the most specific constructor available.
      try {
        return Optional.of(clazz.getDeclaredConstructor(TableCache.class, HelixAdmin.class, String.class)
            .newInstance(tableCache, helixAdmin, clusterName));
      } catch (NoSuchMethodException ignored) {
        // fall through
      }
      try {
        return Optional.of(
            clazz.getDeclaredConstructor(TableCache.class, HelixAdmin.class).newInstance(tableCache, helixAdmin));
      } catch (NoSuchMethodException ignored) {
        // fall through
      }
      try {
        return Optional.of(clazz.getDeclaredConstructor(TableCache.class).newInstance(tableCache));
      } catch (NoSuchMethodException ignored) {
        // fall through
      }
      return Optional.of(clazz.getDeclaredConstructor().newInstance());
    } catch (Exception e) {
      LOGGER.warn("Failed to instantiate system table provider {}: {}", clazz.getName(), e.toString());
      return Optional.empty();
    }
  }
}
