package org.apache.pinot.controller.workload.scheme;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.workload.NodeConfig;


/**
 * TenantPropagationScheme is used to resolve instances based on the {@link NodeConfig} and {@link NodeConfig.Type}
 * It resolves the instances based on the tenants specified in the node configuration
 */
public class TenantPropagationScheme implements PropagationScheme {

  private final PinotHelixResourceManager _pinotHelixResourceManager;

  public TenantPropagationScheme(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  @Override
  public Set<String> resolveInstances(NodeConfig.Type nodeType, NodeConfig nodeConfig) {
    Map<String, Set<String>> helixTagToInstances = PropagationUtils.getHelixTagToInstances(_pinotHelixResourceManager);
    Set<String> allInstances = new HashSet<>();
    List<String> tenantNames = nodeConfig.getPropagationScheme().getValues();
    // Get the unique set of helix tags for the tenants
    Set<String> helixTags = new HashSet<>();
    for (String tenantName : tenantNames) {
      if (nodeType == NodeConfig.Type.NON_LEAF_NODE) {
        helixTags.add(TagNameUtils.getBrokerTagForTenant(tenantName));
      } else if (nodeType == NodeConfig.Type.LEAF_NODE) {
        if (TagNameUtils.isOfflineServerTag(tenantName) || TagNameUtils.isRealtimeServerTag(tenantName)) {
          helixTags.add(tenantName);
        } else {
          helixTags.add(TagNameUtils.getOfflineTagForTenant(tenantName));
          helixTags.add(TagNameUtils.getRealtimeTagForTenant(tenantName));
        }
      }
    }
    // Get the instances for the helix tags
    for (String helixTag : helixTags) {
      Set<String> instances = helixTagToInstances.get(helixTag);
      if (instances != null) {
        allInstances.addAll(instances);
      }
    }
    return allInstances;
  }
}
