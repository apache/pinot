/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.transport.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;


/**
 * Maintains mapping of tables to their routing config
 *
 * Relevant config for illustration:
 * pinot.broker.routing.tableName=midas
 *
 * pinot.broker.routing.midas.servers.default=localhost:9099
 *
 *
 */
public class RoutingTableConfig {

  private Configuration _brokerRoutingConfig;

  // Mapping between table to its routing config
  private Map<String, PerTableRoutingConfig> _tableRoutingCfg;

  // Keys to load config
  private static final String TABLE_NAME = "tableName";

  public RoutingTableConfig() {
    _tableRoutingCfg = new HashMap<String, PerTableRoutingConfig>();
  }

  public void init(Configuration brokerRoutingConfig) throws ConfigurationException {
    _brokerRoutingConfig = brokerRoutingConfig;
    loadConfigs();
  }

  /**
   * Load Config
   */
  private void loadConfigs() {
    List<String> tables = getTableNames();
    for (String s : tables) {
      PerTableRoutingConfig cfg = new PerTableRoutingConfig(_brokerRoutingConfig.subset(s));
      _tableRoutingCfg.put(s, cfg);
    }
  }

  @SuppressWarnings("unchecked")
  private List<String> getTableNames() {
    return _brokerRoutingConfig.getList(TABLE_NAME);
  }

  public Map<String, PerTableRoutingConfig> getPerTableRoutingCfg() {
    return _tableRoutingCfg;
  }

  @Override
  public String toString() {
    return "BrokerRoutingConfig [_brokerRoutingConfig=" + _brokerRoutingConfig + ", _perTableRoutingCfg="
        + _tableRoutingCfg + "]";
  }

}
