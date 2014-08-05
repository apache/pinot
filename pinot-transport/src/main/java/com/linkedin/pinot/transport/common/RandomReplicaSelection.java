package com.linkedin.pinot.transport.common;

import java.util.List;
import java.util.Random;

import com.linkedin.pinot.common.query.response.ServerInstance;

public class RandomReplicaSelection extends ReplicaSelection {

  private final Random _rand;

  public RandomReplicaSelection(long seed)
  {
    _rand = new Random(seed);
  }

  @Override
  public void reset(SegmentId p) {
    // Nothing to be done here
  }

  @Override
  public void reset(SegmentIdSet p) {
    // Nothing to be done here
  }

  @Override
  public ServerInstance selectServer(SegmentId p, List<ServerInstance> orderedServers,  Object bucketKey) {

    int size = orderedServers.size();

    if ( size <= 0) {
      return null;
    }

    return orderedServers.get(Math.abs(_rand.nextInt())%size);
  }

}
