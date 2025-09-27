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
package org.apache.pinot.common.audit;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.services.ServiceRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Configuration manager for audit logging settings.
 * Handles dynamic configuration updates from cluster configuration changes.
 * Note. Needs to be registered with the provided cluster config provider to listen to config changes
 */
@Singleton
public final class AuditConfigManager implements PinotClusterConfigChangeListener {
  private static final Logger LOG = LoggerFactory.getLogger(AuditConfigManager.class);
  private static final String AUDIT_CONFIG_PREFIX = "pinot.audit";
  private static final Map<ServiceRole, String> SERVICE_ROLE_CONFIG_PREFIX_MAP =
      Map.of(ServiceRole.CONTROLLER, "controller", ServiceRole.SERVER, "server", ServiceRole.BROKER, "broker");

  private final String _configPrefix;
  private AuditConfig _currentConfig = new AuditConfig();

  @Inject
  public AuditConfigManager(ServiceRole serviceRole) {
    _configPrefix = constructAuditConfigPrefix(serviceRole);
  }

  /**
   * Maps cluster configuration properties with a common prefix to a POJO using Jackson.
   * Uses PinotConfiguration.subset() to extract properties with the given prefix and
   * Jackson's convertValue() for automatic object mapping.
   */
  @VisibleForTesting
  static AuditConfig buildFromClusterConfig(Map<String, String> clusterConfigs, String auditConfigPrefix) {
    final MapConfiguration mapConfig = new MapConfiguration(clusterConfigs);
    final PinotConfiguration subsetConfig = new PinotConfiguration(mapConfig).subset(auditConfigPrefix);
    return AuditLogger.OBJECT_MAPPER.convertValue(subsetConfig.toMap(), AuditConfig.class);
  }

  private static String constructAuditConfigPrefix(ServiceRole serviceRole) {
    String componentPrefix = SERVICE_ROLE_CONFIG_PREFIX_MAP.get(serviceRole);
    Objects.requireNonNull(componentPrefix, "Unsupported service role: " + serviceRole);
    return AUDIT_CONFIG_PREFIX + "." + componentPrefix;
  }

  public AuditConfig getCurrentConfig() {
    return _currentConfig;
  }

  public boolean isEnabled() {
    return _currentConfig.isEnabled();
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    boolean hasAuditConfigChanges = changedConfigs.stream().anyMatch(key -> key.startsWith(_configPrefix));

    if (!hasAuditConfigChanges) {
      LOG.info("No audit-related configs changed, skipping configuration rebuild");
      return;
    }

    try {
      _currentConfig = buildFromClusterConfig(clusterConfigs, _configPrefix);
      LOG.info("Successfully updated audit configuration");
    } catch (Exception e) {
      LOG.error("Failed to update audit configuration from cluster configs", e);
    }
  }
}
