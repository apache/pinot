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
package org.apache.pinot.broker.routing;

import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Thread-safe policy for broker fail-open routing.
///
/// The broker-configured value is retained separately from the effective startup value so removing a dynamic
/// cluster override restores the broker value (or the default when the broker did not configure one).
///
/// This class currently only lands the config plumbing: the policy is constructed and registered for cluster config
/// changes, but is not yet consulted on the routing path. Instance selection will read [#isEnabled()] in a
/// follow-up change.
@ThreadSafe
public final class FailOpenPolicy implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(FailOpenPolicy.class);

  private final boolean _brokerConfiguredEnabled;
  private volatile boolean _enabled;

  public FailOpenPolicy(boolean brokerConfiguredEnabled, boolean startupEnabled) {
    _brokerConfiguredEnabled = brokerConfiguredEnabled;
    _enabled = startupEnabled;
  }

  /// Returns a policy with fail-open routing disabled and no broker-configured override. Not yet called; intended for
  /// routing components that are constructed without a broker-provided policy once fail-open routing is wired in.
  public static FailOpenPolicy disabled() {
    return new FailOpenPolicy(false, false);
  }

  /// Returns whether fail-open routing is currently enabled. Not yet read on the routing path; instance selection will
  /// consult this in a follow-up change.
  public boolean isEnabled() {
    return _enabled;
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    String configKey = Broker.CONFIG_OF_ENABLE_FAIL_OPEN;
    if (!changedConfigs.contains(configKey)) {
      return;
    }
    boolean enabled = Boolean.parseBoolean(
        clusterConfigs.getOrDefault(configKey, Boolean.toString(_brokerConfiguredEnabled)));
    if (_enabled != enabled) {
      LOGGER.info("Setting fail-open routing enabled to: {}", enabled);
      _enabled = enabled;
    }
  }
}
