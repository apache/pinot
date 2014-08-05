package com.linkedin.pinot.transport.common;

import java.util.List;

import com.linkedin.pinot.common.query.response.ServerInstance;

public class HashReplicaSelection extends ReplicaSelection {

  @Override
  public void reset(SegmentId p) {
    // Nothing to be done here as no state is maintained here
  }

  @Override
  public void reset(SegmentIdSet p) {
    // Nothing to be done here as no state is maintained here
  }

  @Override
  public ServerInstance selectServer(SegmentId p, List<ServerInstance> orderedServers,  Object bucketKey) {

    int size = orderedServers.size();

    if ( size <= 0 ) {
      return null;
    }

    return orderedServers.get(bucketKey.hashCode()%size);
  }

}
