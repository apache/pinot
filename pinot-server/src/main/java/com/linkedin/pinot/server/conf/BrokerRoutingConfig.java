package com.linkedin.pinot.server.conf;

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
public class BrokerRoutingConfig {

  private final Configuration _brokerRoutingConfig;

  // Mapping between resource to its routing config
  private final Map<String, ResourceRoutingConfig> _resourceRoutingCfg;

  // Keys to load config
  private static final String RESOURCE_NAME = "resourceName";

  public BrokerRoutingConfig(Configuration brokerRoutingConfig) throws ConfigurationException {
    _brokerRoutingConfig = brokerRoutingConfig;
    _resourceRoutingCfg = new HashMap<String, ResourceRoutingConfig>();
    loadConfigs();
  }

  /**
   * Load Config
   */
  private void loadConfigs()
  {
    List<String> resources = getResourceNames();
    for (String s : resources)
    {
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
