package com.linkedin.pinot.transport.common;

import java.util.List;

public class HashReplicaSelection extends ReplicaSelection {

  @Override
  public void reset(Partition p) {
    // Nothing to be done here as no state is maintained here
  }

  @Override
  public void reset(PartitionGroup p) {
    // Nothing to be done here as no state is maintained here
  }

  @Override
  public ServerInstance selectServer(Partition p, List<ServerInstance> orderedServers,  Object bucketKey) {

    int size = orderedServers.size();

    if ( size <= 0 ) {
      return null;
    }

    return orderedServers.get(bucketKey.hashCode()%size);
  }

}
