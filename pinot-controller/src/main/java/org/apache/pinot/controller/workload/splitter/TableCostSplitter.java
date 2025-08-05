package org.apache.pinot.controller.workload.splitter;

import org.apache.pinot.spi.config.workload.CostSplit;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.checkerframework.checker.units.qual.C;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableCostSplitter {

  public Map<String, InstanceCost> computeInstanceCostMap(NodeConfig nodeConfig, Set<String> instances,
                                                          String identifier) {
    PropagationScheme propagationScheme = nodeConfig.getPropagationScheme();
    long totalCpuCostNs = nodeConfig.getEnforcementProfile().getCpuCostNs();
    long totalMemoryCostBytes = nodeConfig.getEnforcementProfile().getMemoryCostBytes();

    CostSplit costSplit = propagationScheme.getCostSplits().get(identifier);
    if (costSplit != null) {
      long cpuCostNs = (long) (totalCpuCostNs * costSplit.getCostValue() / 100);
      long memoryCostBytes = (long) (totalMemoryCostBytes * costSplit.getCostValue() / 100);

      InstanceCost instanceCost = new InstanceCost(cpuCostNs, memoryCostBytes);
      Map<String, InstanceCost> costMap = new HashMap<>();
      for (String instance : instances) {
        costMap.put(instance, instanceCost);
      }
      return costMap;
    }


    return new HashMap<>();
  }

  public InstanceCost computeInstanceCost(NodeConfig nodeConfig, Set<String> instances, String instance) {
    // Implement logic for computing instance cost based on table workload
    return new InstanceCost(0L, 0L);
  }
}
