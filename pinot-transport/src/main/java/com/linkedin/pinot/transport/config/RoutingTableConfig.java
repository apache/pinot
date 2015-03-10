/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
 * Maintains mapping of resources to their routing config
 *
 * Relevant config for illustration:
 * pinot.broker.routing.resourceName=midas
 *
 * pinot.broker.routing.midas.servers.default=localhost:9099
 *
 * @author bvaradar
 *
 */
public class RoutingTableConfig {

  private Configuration _brokerRoutingConfig;

  // Mapping between resource to its routing config
  private Map<String, ResourceRoutingConfig> _resourceRoutingCfg;

  // Keys to load config
  private static final String RESOURCE_NAME = "resourceName";

  public RoutingTableConfig() {
    _resourceRoutingCfg = new HashMap<String, ResourceRoutingConfig>();
  }

  public void init(Configuration brokerRoutingConfig) throws ConfigurationException {
    _brokerRoutingConfig = brokerRoutingConfig;
    loadConfigs();
  }

  /**
   * Load Config
   */
  private void loadConfigs() {
    List<String> resources = getResourceNames();
    for (String s : resources) {
      ResourceRoutingConfig cfg = new ResourceRoutingConfig(_brokerRoutingConfig.subset(s));
      _resourceRoutingCfg.put(s, cfg);
    }
  }

  @SuppressWarnings("unchecked")
  private List<String> getResourceNames() {
    return _brokerRoutingConfig.getList(RESOURCE_NAME);
  }

  public Map<String, ResourceRoutingConfig> getResourceRoutingCfg() {
    return _resourceRoutingCfg;
  }

  @Override
  public String toString() {
    return "BrokerRoutingConfig [_brokerRoutingConfig=" + _brokerRoutingConfig + ", _resourceRoutingCfg="
        + _resourceRoutingCfg + "]";
  }

}
