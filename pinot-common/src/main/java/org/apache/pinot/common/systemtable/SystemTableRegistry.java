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
 * <p>
 * This class is thread-safe and manages the lifecycle of registered providers.
 */
public final class SystemTableRegistry implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SystemTableRegistry.class);
  private static final String SYSTEM_TABLE_PREFIX = "system.";

  // Providers are registered once at broker startup.
  private final Map<String, SystemTableProvider> _providers = new HashMap<>();
  private final SystemTableProviderContext _context;
  private volatile boolean _closed = false;

  /**
   * Creates a new SystemTableRegistry and initializes it with annotated providers.
   */
  public SystemTableRegistry(TableCache tableCache, HelixAdmin helixAdmin, String clusterName,
      @Nullable PinotConfiguration config) {
    _context = new SystemTableProviderContext(tableCache, helixAdmin, clusterName, config);
    registerAnnotatedProviders();
  }

  /**
   * Registers a system table provider.
   * <p>
   * Note: This method does NOT call {@link SystemTableProvider#init(SystemTableProviderContext)} on the provider.
   * It is intended for registering providers that have already been initialized (e.g. in tests).
   *
   * @throws IllegalArgumentException if the table name does not start with "system."
   */
  public void register(SystemTableProvider provider) {
    String tableName = provider.getTableName();
    if (!tableName.toLowerCase(Locale.ROOT).startsWith(SYSTEM_TABLE_PREFIX)) {
      throw new IllegalArgumentException(
          "System table name must start with '" + SYSTEM_TABLE_PREFIX + "', got: " + tableName);
    }
    SystemTableProvider displaced;
    synchronized (_providers) {
      if (_closed) {
        throw new IllegalStateException("Cannot register provider after registry is closed");
      }
      displaced = _providers.put(normalize(tableName), provider);
    }
    if (displaced != null && displaced != provider) {
      try {
        displaced.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close displaced provider for {}: {}", tableName, e.toString());
      }
    }
  }

  /**
   * Retrieves a registered system table provider by table name.
   *
   * @return the provider, or null if not found or registry is closed
   */
  @Nullable
  public SystemTableProvider get(String tableName) {
    synchronized (_providers) {
      if (_closed) {
        return null;
      }
      return _providers.get(normalize(tableName));
    }
  }

  /**
   * Checks if a system table is registered.
   */
  public boolean isRegistered(String tableName) {
    synchronized (_providers) {
      if (_closed) {
        return false;
      }
      return _providers.containsKey(normalize(tableName));
    }
  }

  /**
   * Returns all registered providers.
   */
  public Collection<SystemTableProvider> getProviders() {
    synchronized (_providers) {
      if (_closed) {
        return Collections.emptyList();
      }
      return Collections.unmodifiableCollection(new java.util.ArrayList<>(_providers.values()));
    }
  }

  /**
   * Discover and register providers marked with {@link SystemTable} using the available dependencies.
   * <p>
   * Follows the ScalarFunction pattern: any class annotated with @SystemTable under a "*.systemtable.*" package
   * will be discovered via reflection, instantiated with a no-arg constructor, initialized via
   * {@link SystemTableProvider#init(SystemTableProviderContext)}, and registered.
   */
  private void registerAnnotatedProviders() {
    Set<Class<?>> classes =
        PinotReflectionUtils.getClassesThroughReflection(".*\\.systemtable\\..*", SystemTable.class);
    for (Class<?> clazz : classes) {
      if (!SystemTableProvider.class.isAssignableFrom(clazz)) {
        continue;
      }
      Class<? extends SystemTableProvider> providerClass = clazz.asSubclass(SystemTableProvider.class);
      SystemTableProvider provider;
      try {
        provider = providerClass.getConstructor().newInstance();
      } catch (Exception e) {
        LOGGER.warn("Failed to instantiate system table provider {}: {}", providerClass.getName(), e.toString());
        continue;
      }
      if (isRegistered(provider.getTableName())) {
        continue;
      }
      try {
        provider.init(_context);
      } catch (Exception e) {
        LOGGER.warn("Failed to initialize system table provider {}: {}", providerClass.getName(), e.toString());
        continue;
      }
      LOGGER.info("Registering system table provider: {}", provider.getTableName());
      register(provider);
    }
  }

  /**
   * Closes the registry and all registered providers.
   * After calling this method, the registry will not accept new registrations
   * and will return null for all lookups.
   */
  @Override
  public void close()
      throws Exception {
    Exception firstException = null;
    // Snapshot providers to avoid concurrent modifications and to close each provider at most once.
    Set<SystemTableProvider> providersToClose = Collections.newSetFromMap(new IdentityHashMap<>());
    synchronized (_providers) {
      _closed = true;
      for (SystemTableProvider provider : _providers.values()) {
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
      synchronized (_providers) {
        _providers.clear();
      }
    }
    if (firstException != null) {
      throw firstException;
    }
  }

  private String normalize(String tableName) {
    return tableName.toLowerCase(Locale.ROOT);
  }
}
