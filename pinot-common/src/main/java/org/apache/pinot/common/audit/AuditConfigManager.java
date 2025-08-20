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

import java.util.HashSet;
import java.util.Set;
import javax.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Thread-safe configuration manager for audit logging settings.
 * Handles dynamic configuration updates from cluster configuration changes.
 * Self-registers with the provided cluster config provider.
 */
@Singleton
public final class AuditConfigManager {

  private static final Logger LOG = LoggerFactory.getLogger(AuditConfigManager.class);

  // TODO spyne Hardcoded stub code. Wire this up with ClusterConfiguration
  private final AuditConfig _currentConfig = new AuditConfig();

  /**
   * Checks if the given endpoint should be excluded from audit logging.
   * Supports simple wildcard matching with '*' character.
   */
  public static boolean isEndpointExcluded(String endpoint, String excludedEndpointsString) {
    if (StringUtils.isBlank(endpoint) || StringUtils.isBlank(excludedEndpointsString)) {
      return false;
    }

    Set<String> excludedEndpoints = parseExcludedEndpoints(excludedEndpointsString);
    if (excludedEndpoints.isEmpty()) {
      return false;
    }

    // Check for exact matches first
    if (excludedEndpoints.contains(endpoint)) {
      return true;
    }

    // Check for wildcard matches
    for (String excluded : excludedEndpoints) {
      if (excluded.contains("*")) {
        if (matchesWildcard(endpoint, excluded)) {
          return true;
        }
      }
    }

    return false;
  }

  private static Set<String> parseExcludedEndpoints(String excludedEndpointsString) {
    Set<String> excludedEndpoints = new HashSet<>();
    if (StringUtils.isNotBlank(excludedEndpointsString)) {
      String[] endpoints = excludedEndpointsString.split(",");
      for (String endpoint : endpoints) {
        String trimmed = endpoint.trim();
        if (StringUtils.isNotBlank(trimmed)) {
          excludedEndpoints.add(trimmed);
        }
      }
    }
    return excludedEndpoints;
  }

  private static boolean matchesWildcard(String endpoint, String pattern) {
    if (pattern.equals("*")) {
      return true;
    }
    if (pattern.endsWith("/*")) {
      String prefix = pattern.substring(0, pattern.length() - 2);
      return endpoint.startsWith(prefix);
    }
    if (pattern.startsWith("*/")) {
      String suffix = pattern.substring(2);
      return endpoint.endsWith(suffix);
    }
    return false;
  }

  /**
   * Gets the current audit configuration.
   * This method is thread-safe and lock-free.
   *
   * @return the current audit configuration
   */
  public AuditConfig getCurrentConfig() {
    return _currentConfig;
  }

  /**
   * Checks if audit logging is currently enabled.
   * Convenience method that delegates to the current configuration.
   *
   * @return true if audit logging is enabled
   */
  public boolean isEnabled() {
    return _currentConfig.isEnabled();
  }

  /**
   * Checks if the given endpoint should be excluded from audit logging.
   *
   * @param endpoint the endpoint path to check
   * @return true if the endpoint should be excluded
   */
  public boolean isEndpointExcluded(String endpoint) {
    return isEndpointExcluded(endpoint, _currentConfig.getExcludedEndpoints());
  }
}
