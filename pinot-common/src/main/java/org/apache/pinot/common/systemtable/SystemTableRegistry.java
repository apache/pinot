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
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
public final class SystemTableRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(SystemTableRegistry.class);

  // Providers are registered once at broker startup.
  private static final Map<String, SystemTableDataProvider> PROVIDERS = new HashMap<>();

  private SystemTableRegistry() {
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
  }

  public static void register(SystemTableDataProvider provider) {
    synchronized (PROVIDERS) {
      PROVIDERS.put(normalize(provider.getTableName()), provider);
    }
  }

  @Nullable
  public static SystemTableDataProvider get(String tableName) {
    synchronized (PROVIDERS) {
      return PROVIDERS.get(normalize(tableName));
    }
  }

  public static boolean isRegistered(String tableName) {
    synchronized (PROVIDERS) {
      return PROVIDERS.containsKey(normalize(tableName));
    }
  }

  public static Collection<SystemTableDataProvider> getProviders() {
    synchronized (PROVIDERS) {
      return Collections.unmodifiableCollection(new java.util.ArrayList<>(PROVIDERS.values()));
    }
  }

  /**
   * Discover and register providers marked with {@link SystemTable} using the available dependencies.
   * Follows the ScalarFunction pattern: any class annotated with @SystemTable under a "*.systemtable.*" package
   * will be discovered via reflection and registered.
   */
  public static void registerAnnotatedProviders(TableCache tableCache, HelixAdmin helixAdmin, String clusterName) {
    Set<Class<?>> classes =
        PinotReflectionUtils.getClassesThroughReflection(".*\\.systemtable\\..*", SystemTable.class);
    for (Class<?> clazz : classes) {
      if (!SystemTableDataProvider.class.isAssignableFrom(clazz)) {
        continue;
      }
      SystemTableDataProvider provider = instantiateProvider(
          clazz.asSubclass(SystemTableDataProvider.class), tableCache, helixAdmin, clusterName);
      if (provider == null) {
        continue;
      }
      if (isRegistered(provider.getTableName())) {
        continue;
      }
      LOGGER.info("Registering system table provider: {}", provider.getTableName());
      register(provider);
    }
  }

  public static void close()
      throws Exception {
    Exception firstException = null;
    // Snapshot providers to avoid concurrent modifications and to close each provider at most once.
    Map<SystemTableDataProvider, Boolean> providersToClose = new IdentityHashMap<>();
    synchronized (PROVIDERS) {
      for (SystemTableDataProvider provider : PROVIDERS.values()) {
        providersToClose.put(provider, Boolean.TRUE);
      }
    }
    try {
      for (SystemTableDataProvider provider : providersToClose.keySet()) {
        try {
          provider.close();
        } catch (Exception e) {
          if (firstException == null) {
            firstException = e;
          } else {
            firstException.addSuppressed(e);
          }
        }
      }
    } finally {
      synchronized (PROVIDERS) {
        PROVIDERS.clear();
      }
    }
    if (firstException != null) {
      throw firstException;
    }
  }

  /**
   * Initialize and register all annotated system table providers.
   */
  public static void init(TableCache tableCache, HelixAdmin helixAdmin, String clusterName) {
    registerAnnotatedProviders(tableCache, helixAdmin, clusterName);
  }

  private static String normalize(String tableName) {
    return tableName.toLowerCase(Locale.ROOT);
  }

  private static @Nullable SystemTableDataProvider instantiateProvider(Class<? extends SystemTableDataProvider> clazz,
      TableCache tableCache, HelixAdmin helixAdmin, String clusterName) {
    try {
      // Prefer the most specific constructor available.
      // Supported provider constructors (in preference order):
      //   (TableCache, HelixAdmin, String clusterName)
      //   (TableCache, HelixAdmin)
      //   (TableCache)
      //   ()
      try {
        return clazz.getDeclaredConstructor(TableCache.class, HelixAdmin.class, String.class)
            .newInstance(tableCache, helixAdmin, clusterName);
      } catch (NoSuchMethodException ignored) {
        // fall through
      }
      try {
        return clazz.getDeclaredConstructor(TableCache.class, HelixAdmin.class).newInstance(tableCache, helixAdmin);
      } catch (NoSuchMethodException ignored) {
        // fall through
      }
      try {
        return clazz.getDeclaredConstructor(TableCache.class).newInstance(tableCache);
      } catch (NoSuchMethodException ignored) {
        // fall through
      }
      return clazz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOGGER.warn("Failed to instantiate system table provider {}: {}", clazz.getName(), e.toString());
      return null;
    }
  }
}
