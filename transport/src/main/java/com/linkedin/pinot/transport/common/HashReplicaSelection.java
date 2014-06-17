package com.linkedin.pinot.transport.common;

import java.util.List;

public class HashReplicaSelection implements ReplicaSelection {

  @Override
  public void reset(Partition p) {
    // Nothing to be done here as no state is maintained here
  }

  @Override
  public void reset(PartitionGroup p) {
    // Nothing to be done here as no state is maintained here
  }

  @Override
  public ServerInstance selectServer(Partition p, List<ServerInstance> orderedServers, BucketingSelection predefinedSelect, Object bucketKey) {

    // Apply predefined selection if provided
    if ( null != predefinedSelect)
    {
      ServerInstance c = predefinedSelect.selectServer(p, orderedServers);
      if ( null != c)
      {
        return c;
      }
    }

    int size = orderedServers.size();

    if ( size <= 0 )
      return null;

    return orderedServers.get(bucketKey.hashCode()%size);
  }

}
