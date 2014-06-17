package com.linkedin.pinot.transport.common;

import java.util.List;

/**
 * 
 * Replica selection Policy implementation. The options mentioned in {@link ReplicaSelectionPolicy}
 * are supported.
 * 
 * @author bvaradar
 *
 */
public interface ReplicaSelection {

  /**
   * 
   * Policy types for selecting a replica hosting a partition-group among
   * the candidate nodes
   */
  public enum ReplicaSelectionPolicy {
    UNKNOWN, /** Replica Selection Strategy not defined **/
    RANDOM,  /** Replica will be independently selected at random  **/
    ROUND_ROBIN, /**
                       Nodes are supposed to be arranged in deterministic
                       (ascending) order and one picked in a round-robin
                        fashion
     **/
    Hash,        /**
                       Nodes are supposed to be arranged in deterministic
                       (ascending) order. A key ( in the request) is hashed
                       to determine the replica
     **/

  };

  /**
   * This is a notification by the routing table provider that the set of servers
   * hosting the partition "p" has changed. The replica selection policy should
   * use this opportunity to cleanup any state for the partition.
   * @param p Partition for which server set has changed.
   */
  public  void reset(Partition p);

  /**
   * This is a notification by the routing table provider that the set of servers
   * hosting the partition-group "p" has changed. The replica selection policy should
   * use this opportunity to cleanup any state for the partition-group.
   * @param p Partition group for which server set has changed.
   */
  public  void reset(PartitionGroup p);


  /**
   * Selects a server instance from the list of servers provided for the given partition. The list of servers
   * are expected to be ordered ( ascending or descending) to provide consistent selection
   * of servers. No additional ordering is done internally for performance reasons.
   * 
   * This method is expected to be thread-safe as several request can call this method
   * concurrently.
   * 
   * if predefinedSelection is provided for the partition being queried, the implementation will honor them
   * and return the preselected server. This is used for debugging when the request needs to be scattered across the
   * same set of servers.
   * 
   * @param partition The partition for which server selection needs to happen
   * @param orderedServers Ordered list of servers from which a server has to be selected
   * @param predefinedSelection Pre-selected servers (overridden). Used by selectServer if this is not null.
   * @param hashKey bucketKey whose {@link Object.hashcode()} provides hash-based selection
   * @return
   */
  public ServerInstance selectServer(Partition p, List<ServerInstance> orderedServers, BucketingSelection predefinedSelection, Object hashKey);
}
