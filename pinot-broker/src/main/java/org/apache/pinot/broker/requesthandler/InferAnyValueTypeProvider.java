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
package org.apache.pinot.broker.requesthandler;

import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Resolves the default of the `inferAnyValueType` query option from
/// [Broker#CONFIG_OF_BROKER_QUERY_INFER_ANY_VALUE_TYPE] in the broker conf and in the Helix cluster config. The cluster
/// config wins where it sets the key, and it is applied live: registered as a [PinotClusterConfigChangeListener], a
/// change takes effect on the next query without a broker restart.
///
/// Thread-safe.
public class InferAnyValueTypeProvider implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(InferAnyValueTypeProvider.class);

  private final boolean _brokerConfEnabled;
  private volatile boolean _enabled;

  public InferAnyValueTypeProvider(PinotConfiguration config) {
    _brokerConfEnabled = config.getProperty(Broker.CONFIG_OF_BROKER_QUERY_INFER_ANY_VALUE_TYPE,
        Broker.DEFAULT_BROKER_QUERY_INFER_ANY_VALUE_TYPE);
    _enabled = _brokerConfEnabled;
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (!changedConfigs.contains(Broker.CONFIG_OF_BROKER_QUERY_INFER_ANY_VALUE_TYPE)) {
      return;
    }
    // A removed or invalid value falls back to the broker conf, so that a broker that restarts resolves the same.
    String value = clusterConfigs.get(Broker.CONFIG_OF_BROKER_QUERY_INFER_ANY_VALUE_TYPE);
    boolean enabled = _brokerConfEnabled;
    // Accept only true/false so a typo restores the fallback rather than silently disabling the rewrite.
    if (value != null && ("true".equalsIgnoreCase(value.trim()) || "false".equalsIgnoreCase(value.trim()))) {
      enabled = Boolean.parseBoolean(value.trim());
    } else if (value != null) {
      LOGGER.error("Ignoring invalid value '{}' for {}, falling back to '{}'", value,
          Broker.CONFIG_OF_BROKER_QUERY_INFER_ANY_VALUE_TYPE, _brokerConfEnabled);
    }
    _enabled = enabled;
    LOGGER.info("Updated {} to {}", Broker.CONFIG_OF_BROKER_QUERY_INFER_ANY_VALUE_TYPE, enabled);
  }

  public boolean isEnabled() {
    return _enabled;
  }
}
