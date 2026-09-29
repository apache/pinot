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
package org.apache.pinot.common.utils.config;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.pinot.common.utils.config.QueryOptionsUtils.SqlOptionsMode;
import org.apache.pinot.common.utils.config.QueryOptionsUtils.SqlQueryOptionValidationMode;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Applies the `pinot.broker.query.option.*` cluster configs to the process-wide SQL query option policies held by
/// [QueryOptionsUtils]: [Broker#CONFIG_OF_BROKER_QUERY_OPTION_VALIDATION_MODE] and
/// [Broker#CONFIG_OF_BROKER_QUERY_OPTION_LEGACY_SYNTAX_MODE]. Changes apply live, and a removed key restores the
/// default; the broker instance config is not consulted. An invalid value is logged and the current mode kept, so
/// that a typo can neither break query parsing nor the other listeners.
public class QueryOptionConfigListener implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryOptionConfigListener.class);

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (changedConfigs.contains(Broker.CONFIG_OF_BROKER_QUERY_OPTION_VALIDATION_MODE)) {
      applyValidationMode(clusterConfigs);
    }
    if (changedConfigs.contains(Broker.CONFIG_OF_BROKER_QUERY_OPTION_LEGACY_SYNTAX_MODE)) {
      applyLegacySyntaxMode(clusterConfigs);
    }
  }

  private static void applyValidationMode(Map<String, String> clusterConfigs) {
    apply(clusterConfigs, Broker.CONFIG_OF_BROKER_QUERY_OPTION_VALIDATION_MODE,
        Broker.DEFAULT_BROKER_QUERY_OPTION_VALIDATION_MODE, SqlQueryOptionValidationMode.class,
        QueryOptionsUtils.getSqlQueryOptionValidationMode(), QueryOptionsUtils::setSqlQueryOptionValidationMode);
  }

  private static void applyLegacySyntaxMode(Map<String, String> clusterConfigs) {
    apply(clusterConfigs, Broker.CONFIG_OF_BROKER_QUERY_OPTION_LEGACY_SYNTAX_MODE,
        Broker.DEFAULT_BROKER_QUERY_OPTION_LEGACY_SYNTAX_MODE, SqlOptionsMode.class,
        QueryOptionsUtils.getLegacyOptionSyntaxMode(), QueryOptionsUtils::setLegacyOptionSyntaxMode);
  }

  private static <E extends Enum<E>> void apply(Map<String, String> clusterConfigs, String key, String defaultValue,
      Class<E> enumClass, E currentValue, Consumer<E> setter) {
    String value = clusterConfigs.getOrDefault(key, defaultValue);
    E newValue;
    try {
      newValue = Enum.valueOf(enumClass, value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      LOGGER.error("Ignoring invalid value '{}' for cluster config: {}, keeping: {}", value, key, currentValue);
      return;
    }
    if (newValue != currentValue) {
      setter.accept(newValue);
      LOGGER.info("Updated cluster config: {} from {} to {}", key, currentValue, newValue);
    }
  }
}
