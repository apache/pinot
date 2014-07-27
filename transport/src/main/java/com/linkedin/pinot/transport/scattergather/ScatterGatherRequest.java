package com.linkedin.pinot.transport.scattergather;

import java.util.List;
import java.util.Map;

import com.linkedin.pinot.transport.common.BucketingSelection;
import com.linkedin.pinot.transport.common.PartitionGroup;
import com.linkedin.pinot.transport.common.ReplicaSelection;
import com.linkedin.pinot.transport.common.ReplicaSelectionGranularity;
import com.linkedin.pinot.transport.common.ServerInstance;

/**
 *
 * All clients of Request-Routing layer must implement and pass the RoutingRequest instance
 * for request-routing
 */
public interface ScatterGatherRequest {

  /**
   * Return the candidate set of servers that hosts each partition group.
   * The List of services are expected to be ordered so that replica-selection strategy can be
   * applied to them to select one Service among the list for each partition.
   *
   * @return PartitionGroup to Request map.
   */
  public Map<PartitionGroup, List<ServerInstance>> getPartitionServicesMap();

  /**
   * Return the requests that will be sent to the service which is hosting a group of interested partitions
   * @param service Service to which request will be sent.
   * @param queryPartitions Partitions to be queried in the service identified by Service.
   * @return byte[] request to be sent
   */
  public byte[] getRequestForService(ServerInstance service, PartitionGroup queryPartitions);

  /**
   * Replica Selection Policy to follow when a partition-group has more than one candidate nodes
   * @return Replica selection strategy for this request.
   **/
  public ReplicaSelection getReplicaSelection();

  /**
   * Replica Selection granularity to follow for applying the replica selection policy.
   * @return Replica selection granularity for this request.
   **/
  public ReplicaSelectionGranularity getReplicaSelectionGranularity();

  /**
   * Used by Hash based selection strategy. Hash-based routing allows groups of request
   * to be sent to the same node. This is called bucketing. Useful for A/B testing.
   * This is an optional field and is only used if hash-based selection is requested.
   * If hash-based routing is requested but the bucket-key is not present, then round-robin
   * selection is used.
   */
  public Object getHashKey();

  /**
   * Return the number of speculative (duplicate) requests ( to different server) that needs
   * to be sent foe each scattered request. To turn off speculative requests, this method should
   * return 0.
   */
  public int getNumSpeculativeRequests();


  /**
   * Used for diagnostics, A predefined selection of service can be chosen for each partition
   * and sent to the Scatter-Gather. Scatter-Gather will honor such selection and do not override them.
   * @return
   */
  public BucketingSelection getPredefinedSelection();

  /**
   * Request Id for tracing purpose
   * @return
   */
  public long getRequestId();

  /**
   * Return timeout in MS for the request. If the timeout gets elapsed, the request will be cancelled.  Timeout
   * with negative values are considered infinite timeout.
   * @return
   */
  public long getRequestTimeoutMS();
}
