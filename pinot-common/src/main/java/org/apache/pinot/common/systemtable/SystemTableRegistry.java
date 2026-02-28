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
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry to hold and lifecycle-manage system table data providers.
 */
public final class SystemTableRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(SystemTableRegistry.class);

  // Providers are registered once at broker startup.
  private static final Map<String, SystemTableProvider> PROVIDERS = new HashMap<>();

  private static final class ConstructorSpec {
    final Class<?>[] _paramTypes;
    final Object[] _args;

    ConstructorSpec(Class<?>[] paramTypes, Object[] args) {
      _paramTypes = paramTypes;
      _args = args;
    }
  }

  private SystemTableRegistry() {
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
  }

  public static void register(SystemTableProvider provider) {
    synchronized (PROVIDERS) {
      PROVIDERS.put(normalize(provider.getTableName()), provider);
    }
  }

  @Nullable
  public static SystemTableProvider get(String tableName) {
    synchronized (PROVIDERS) {
      return PROVIDERS.get(normalize(tableName));
    }
  }

  public static boolean isRegistered(String tableName) {
    synchronized (PROVIDERS) {
      return PROVIDERS.containsKey(normalize(tableName));
    }
  }

  public static Collection<SystemTableProvider> getProviders() {
    synchronized (PROVIDERS) {
      return Collections.unmodifiableCollection(new java.util.ArrayList<>(PROVIDERS.values()));
    }
  }

  /**
   * Discover and register providers marked with {@link SystemTable} using the available dependencies.
   * Follows the ScalarFunction pattern: any class annotated with @SystemTable under a "*.systemtable.*" package
   * will be discovered via reflection and registered.
   */
  public static void registerAnnotatedProviders(TableCache tableCache, HelixAdmin helixAdmin, String clusterName,
      @Nullable PinotConfiguration config) {
    Set<Class<?>> classes =
        PinotReflectionUtils.getClassesThroughReflection(".*\\.systemtable\\..*", SystemTable.class);
    for (Class<?> clazz : classes) {
      if (!SystemTableProvider.class.isAssignableFrom(clazz)) {
        continue;
      }
      SystemTableProvider provider =
          instantiateProvider(clazz.asSubclass(SystemTableProvider.class), tableCache, helixAdmin, clusterName, config);
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
    Set<SystemTableProvider> providersToClose = Collections.newSetFromMap(new IdentityHashMap<>());
    synchronized (PROVIDERS) {
      for (SystemTableProvider provider : PROVIDERS.values()) {
        providersToClose.add(provider);
      }
    }
    try {
      for (SystemTableProvider provider : providersToClose) {
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
    init(tableCache, helixAdmin, clusterName, null);
  }

  /**
   * Initialize and register all annotated system table providers.
   */
  public static void init(TableCache tableCache, HelixAdmin helixAdmin, String clusterName,
      @Nullable PinotConfiguration config) {
    registerAnnotatedProviders(tableCache, helixAdmin, clusterName, config);
  }

  private static String normalize(String tableName) {
    return tableName.toLowerCase(Locale.ROOT);
  }

  @Nullable
  private static SystemTableProvider instantiateProvider(Class<? extends SystemTableProvider> clazz,
      TableCache tableCache, HelixAdmin helixAdmin, String clusterName, @Nullable PinotConfiguration config) {
    try {
      // Prefer the most specific constructor available.
      // System table providers may declare any of the following constructors, in decreasing order of specificity:
      //   (TableCache, HelixAdmin, String, PinotConfiguration), (TableCache, HelixAdmin, String),
      //   (TableCache, HelixAdmin), (TableCache), (PinotConfiguration), or a no-arg constructor.
      // The registry will select the most specific available constructor so providers can opt in to the dependencies
      // they need without forcing all implementations to depend on Helix or cluster metadata.
      java.util.List<ConstructorSpec> specs = new java.util.ArrayList<>();
      if (config != null) {
        specs.add(new ConstructorSpec(
            new Class<?>[]{TableCache.class, HelixAdmin.class, String.class, PinotConfiguration.class},
            new Object[]{tableCache, helixAdmin, clusterName, config}));
      }
      specs.add(new ConstructorSpec(new Class<?>[]{TableCache.class, HelixAdmin.class, String.class},
          new Object[]{tableCache, helixAdmin, clusterName}));
      specs.add(new ConstructorSpec(new Class<?>[]{TableCache.class, HelixAdmin.class},
          new Object[]{tableCache, helixAdmin}));
      specs.add(new ConstructorSpec(new Class<?>[]{TableCache.class}, new Object[]{tableCache}));
      if (config != null) {
        specs.add(new ConstructorSpec(new Class<?>[]{PinotConfiguration.class}, new Object[]{config}));
      }
      specs.add(new ConstructorSpec(new Class<?>[0], new Object[0]));

      for (ConstructorSpec spec : specs) {
        try {
          return clazz.getConstructor(spec._paramTypes).newInstance(spec._args);
        } catch (NoSuchMethodException ignored) {
          // try next
        }
      }

      throw new NoSuchMethodException("No supported public constructor found for: " + clazz.getName());
    } catch (Exception e) {
      LOGGER.warn("Failed to instantiate system table provider {}: {}", clazz.getName(), e.toString());
      return null;
    }
  }
}
