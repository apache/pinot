package com.linkedin.pinot.transport.common;

/**
 * Determines at what level the selection for nodes (replica) has to happen
 */
public enum ReplicaSelectionGranularity
{
  /**
   * For each partition in the request, the replica-selection policy is applied to get the node.
   * If the selection policy is random or round-robin, then this granularity would likely increase
   * the fan-out of the scatter request since there is a greater chance all replicas will be queried
   **/
  PARTITION,
  /**
   * For each partition-group in the request, the replica-selection policy is applied to get the node.
   * This will likely reduce the fan-out as all partitions in the partition group goes to the same service.
   * This is assuming the services hosting each partition-groups are disjoint.
   **/
  PARTITION_GROUP
}