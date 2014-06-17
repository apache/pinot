package com.linkedin.pinot.transport.common;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinReplicaSelection implements ReplicaSelection {

  private final Map<Partition, AtomicInteger> _nextPositionMap;

  public RoundRobinReplicaSelection(int numPartitions)
  {
    _nextPositionMap = new ConcurrentHashMap<Partition, AtomicInteger>(numPartitions);
  }


  public RoundRobinReplicaSelection()
  {
    _nextPositionMap = new ConcurrentHashMap<Partition, AtomicInteger>();
  }

  @Override
  public void reset(Partition p) {
    _nextPositionMap.remove(p);
  }

  @Override
  public void reset(PartitionGroup pg) {
    for(Partition p : pg.getPartitions())
    {
      reset(p);
    }
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

    if ( size <= 0)
      return null;

    AtomicInteger a = _nextPositionMap.get(p);

    // Create a new position tracker for the partition if it is not available
    if ( null == a)
    {
      synchronized(this)
      {
        a = _nextPositionMap.get(p);
        if (null == a)
        {
          a = new AtomicInteger(-1);
          _nextPositionMap.put(p,a);
        }
      }
    }

    return orderedServers.get(Math.abs(a.incrementAndGet())%size);
  }

}
