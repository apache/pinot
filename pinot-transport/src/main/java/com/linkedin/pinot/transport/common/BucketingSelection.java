package com.linkedin.pinot.transport.common;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * This class provides an option to use a precalculated Selection to be used
 * for request. This is useful for testing where we wanted the request to go to
 * specific servers.
 * @author bvaradar
 *
 */
public class BucketingSelection {

  private final Map<Partition, ServerInstance> _bucketMap;

  public BucketingSelection(Map<Partition, ServerInstance> bucketMap)
  {
    if ( null != bucketMap)
    {
      _bucketMap = bucketMap;
    }
    else
    {
      _bucketMap = new HashMap<Partition, ServerInstance>();
    }
  }


  /**
   * 
   * Use the preselected server for a partition only if it is present in the passed list of candidates.
   * 
   * @param p Partition for which selection has to happen.
   * @param orderedServers Collection of candidates from which a server has to be picked
   * @return the preselected server only if it is present in the passed list of candidates. Otherwise, it is null.
   */
  public ServerInstance selectServer(Partition p, Collection<ServerInstance> servers)
  {
    ServerInstance c = _bucketMap.get(p);
    if ( (null == c) || !servers.contains(c))
    {
      return null;
    }
    return c;
  }

  /**
   * Returns the pre-selected server for a partition (if pre-selected)
   * @param p Partition for which pre-selected server needs to be returned.
   * @return
   */
  public ServerInstance getPreSelectedServer(Partition p)
  {
    return _bucketMap.get(p);
  }
}
