package com.linkedin.pinot.transport.common;

import java.util.List;

/**
 * 
 * Replica selection Policy implementation. The options mentioned in {@link ReplicaSelectionPolicy}
 * are supported.
 * 
 * @author balaji varadarajan
 *
 */
public abstract class ReplicaSelection {

  /**
   * 
   * Policy types for selecting a replica hosting a segmentId-group among
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
   * hosting the segmentId "p" has changed. The replica selection policy should
   * use this opportunity to cleanup any state for the segmentId.
   * @param p segmentId for which server set has changed.
   */
  public  abstract void reset(SegmentId p);

  /**
   * This is a notification by the routing table provider that the set of servers
   * hosting the segmentId-group "p" has changed. The replica selection policy should
   * use this opportunity to cleanup any state for the segmentId-group.
   * @param p segmentId group for which server set has changed.
   */
  public  abstract void reset(SegmentIdSet p);

  /**
   * Selects a server instance from the list of servers provided for the given segmentId. The list of servers
   * are expected to be ordered ( ascending or descending) to provide consistent selection
   * of servers. No additional ordering is done internally for performance reasons.
   * 
   * This method is expected to be thread-safe as several request can call this method
   * concurrently.
   * 
   * @param segmentId The segmentId for which server selection needs to happen
   * @param orderedServers Ordered list of servers from which a server has to be selected
   * @param hashKey bucketKey whose {@link Object.hashcode()} provides hash-based selection
   * @return
   */
  public abstract ServerInstance selectServer(SegmentId p, List<ServerInstance> orderedServers, Object hashKey);

}
