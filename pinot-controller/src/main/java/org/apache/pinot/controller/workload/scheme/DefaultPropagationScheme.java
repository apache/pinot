package org.apache.pinot.controller.workload.scheme;

import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.workload.NodeConfig;


public class DefaultPropagationScheme implements PropagationScheme {

  private final PinotHelixResourceManager _pinotHelixResourceManager;

  public DefaultPropagationScheme(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  @Override
  public Set<String> resolveInstances(NodeConfig.Type nodeType, NodeConfig nodeConfig) {
    Set<String> instances;
    switch (nodeType) {
      case NON_LEAF_NODE:
        instances = new HashSet<>(_pinotHelixResourceManager.getAllBrokerInstances());
        break;
      case LEAF_NODE:
        instances = new HashSet<>(_pinotHelixResourceManager.getAllServerInstances());
        break;
      default:
        throw new IllegalArgumentException("Invalid node type: " + nodeType);
    }
    return instances;
  }
}
