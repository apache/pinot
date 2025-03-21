package org.apache.pinot.controller.workload.scheme;

import org.apache.pinot.spi.config.workload.NodeConfig;
import java.util.Set;

/**
 * PropagationScheme is used to resolve instances based on the {@link NodeConfig} and {@link NodeConfig.Type}
 * 1. It helps to identify which instances to propagate the workload to based on the node configuration
 * 2. It helps among which instances the {@link org.apache.pinot.spi.config.workload.EnforcementProfile} should be split
 */
public interface PropagationScheme {
  /**
   * Resolve the instances based on the node type and node configuration
   * @param nodeType {@link NodeConfig.Type}
   * @param nodeConfig The {@link NodeConfig} to resolve the instances
   * @return The set of instances to propagate the workload to
   */
  Set<String> resolveInstances(NodeConfig.Type nodeType, NodeConfig nodeConfig);
}
