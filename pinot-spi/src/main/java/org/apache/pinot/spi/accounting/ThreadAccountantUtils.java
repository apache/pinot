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
package org.apache.pinot.spi.accounting;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThreadAccountantUtils {
  private ThreadAccountantUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadAccountantUtils.class);

  /// Extracts the accounting config subset for the given instance type, merging the legacy shared prefix
  /// [Accounting#COMMON_PREFIX] with the role-specific prefix
  /// ([Accounting#BROKER_PREFIX] / [Accounting#SERVER_PREFIX]).
  /// Role-specific values win on conflict.
  /// Keys in the returned config are stripped of the accounting prefix (e.g. `oom.enable.killing.query`) and are
  /// expected to match [Accounting.Keys].
  /// Only BROKER and SERVER instance types are supported.
  public static PinotConfiguration extractAccountingConfig(PinotConfiguration fullConfig, InstanceType instanceType) {
    PinotConfiguration legacy = fullConfig.subset(Accounting.COMMON_PREFIX);
    PinotConfiguration override = fullConfig.subset(getRoleAccountingPrefix(instanceType));
    if (override.isEmpty()) {
      return legacy;
    }
    Map<String, Object> merged = new HashMap<>(legacy.toMap());
    merged.putAll(override.toMap());
    return new PinotConfiguration(merged);
  }

  /// Returns the role-specific accounting config prefix (no trailing dot).
  /// Only BROKER and SERVER are supported; any other instance type throws [IllegalStateException].
  public static String getRoleAccountingPrefix(InstanceType instanceType) {
    switch (instanceType) {
      case BROKER:
        return Accounting.BROKER_PREFIX;
      case SERVER:
        return Accounting.SERVER_PREFIX;
      default:
        throw new IllegalStateException("Unsupported instance type for query accounting config: " + instanceType);
    }
  }

  /// Filtered and normalized cluster config change payload used by accounting listeners. Keys are stripped to the
  /// accounting-local form (e.g. `oom.enable.killing.query`, matching [Accounting.Keys]). Values from the role-specific
  /// prefix take precedence over values from the legacy shared prefix.
  public static class AccountingConfigChange {
    private final Set<String> _changedConfigs;
    private final Map<String, String> _clusterConfigs;

    public AccountingConfigChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
      _changedConfigs = changedConfigs;
      _clusterConfigs = clusterConfigs;
    }

    public Set<String> getChangedConfigs() {
      return _changedConfigs;
    }

    public Map<String, String> getClusterConfigs() {
      return _clusterConfigs;
    }

    public boolean isEmpty() {
      return _changedConfigs.isEmpty();
    }
  }

  /// Filters and normalizes a cluster config change event for the accountant listener of the given instance type.
  /// Accepts keys under both the legacy [Accounting#COMMON_PREFIX] and the role-specific prefix
  /// ([Accounting#BROKER_PREFIX] / [Accounting#SERVER_PREFIX]); role-specific values win on conflict.
  public static AccountingConfigChange filterAccountingConfigChange(Set<String> changedConfigs,
      Map<String, String> clusterConfigs, InstanceType instanceType) {
    String legacyPrefixDot = Accounting.COMMON_PREFIX + ".";
    String rolePrefixDot = getRoleAccountingPrefix(instanceType) + ".";

    Set<String> filteredChanged = new HashSet<>();
    for (String key : changedConfigs) {
      if (key.startsWith(legacyPrefixDot)) {
        filteredChanged.add(key.substring(legacyPrefixDot.length()));
      } else if (key.startsWith(rolePrefixDot)) {
        filteredChanged.add(key.substring(rolePrefixDot.length()));
      }
    }

    Map<String, String> filteredClusterConfigs = new HashMap<>();
    // Legacy values first, then role-specific values overwrite on conflict.
    for (Map.Entry<String, String> entry : clusterConfigs.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(legacyPrefixDot)) {
        filteredClusterConfigs.put(key.substring(legacyPrefixDot.length()), entry.getValue());
      }
    }
    for (Map.Entry<String, String> entry : clusterConfigs.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(rolePrefixDot)) {
        filteredClusterConfigs.put(key.substring(rolePrefixDot.length()), entry.getValue());
      }
    }
    return new AccountingConfigChange(filteredChanged, filteredClusterConfigs);
  }

  /// Creates a thread accountant based on the factory class name in the config. If no factory class name is specified,
  /// or if any exception is thrown while loading the factory class, returns a no-op accountant.
  public static ThreadAccountant createAccountant(PinotConfiguration config, String instanceId,
      InstanceType instanceType) {
    String factoryName = config.getProperty(Accounting.Keys.FACTORY_NAME);
    if (factoryName != null) {
      LOGGER.info("Initializing ThreadAccountant with factory: {}", factoryName);
      try {
        ThreadAccountantFactory threadAccountantFactory =
            (ThreadAccountantFactory) Class.forName(factoryName).getDeclaredConstructor().newInstance();
        return threadAccountantFactory.init(config, instanceId, instanceType);
      } catch (Throwable t) {
        LOGGER.error("Caught exception while initializing ThreadAccountant with factory: {}, "
            + "falling back to no-op accountant", factoryName, t);
      }
    }
    LOGGER.info("Using no-op accountant");
    return NoOpAccountant.INSTANCE;
  }

  public static ThreadAccountant getNoOpAccountant() {
    return NoOpAccountant.INSTANCE;
  }

  /// No-op implementation of [ThreadAccountant].
  private static class NoOpAccountant implements ThreadAccountant {
    static final NoOpAccountant INSTANCE = new NoOpAccountant();

    @Override
    public void setupTask(QueryThreadContext threadContext) {
    }

    @Override
    public void sampleUsage() {
    }

    @Override
    public void clear() {
    }

    @Override
    public void updateUntrackedResourceUsage(String queryId, long allocatedBytes, long cpuTimeNs,
        TrackingScope trackingScope) {
    }

    @Override
    public Collection<? extends ThreadResourceTracker> getThreadResources() {
      return List.of();
    }

    @Override
    public Map<String, ? extends QueryResourceTracker> getQueryResources() {
      return Map.of();
    }
  }
}
