package com.linkedin.pinot.transport.common;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.annotation.ThreadSafe;

import com.linkedin.pinot.common.query.response.ServerInstance;

/**
 * Maintains next pointer per segment basis. Expected to be thread-safe
 * @author bvaradar
 *
 */
@ThreadSafe
public class RoundRobinReplicaSelection extends ReplicaSelection {

  private final Map<SegmentId, AtomicInteger> _nextPositionMap;

  public RoundRobinReplicaSelection()
  {
    _nextPositionMap = new ConcurrentHashMap<SegmentId, AtomicInteger>();
  }

  @Override
  public void reset(SegmentId p) {
    _nextPositionMap.remove(p);
  }

  @Override
  public void reset(SegmentIdSet pg) {
    for(SegmentId p : pg.getSegments())
    {
      reset(p);
    }
  }

  @Override
  public ServerInstance selectServer(SegmentId p, List<ServerInstance> orderedServers, Object bucketKey) {

    int size = orderedServers.size();

    if ( size <= 0) {
      return null;
    }

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
