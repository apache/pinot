package com.linkedin.pinot.routing;

import java.util.Map;

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.SegmentIdSet;


public interface RoutingTable {

  /**
   * Return the candidate set of servers that hosts each segment-set.
   * The List of services are expected to be ordered so that replica-selection strategy can be
   * applied to them to select one Service among the list for each segment.
   *
   * @return SegmentSet to Servers map.
   */
  public Map<ServerInstance, SegmentIdSet> findServers(RoutingTableLookupRequest request);

  /**
   * Initialize and start the Routing table population
   */
  public void start();

  /**
   * Shutdown Routing table cleanly
   */
  public void shutdown();
}
