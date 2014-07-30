package com.linkedin.pinot.server.conf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.Configuration;

import com.linkedin.pinot.transport.common.Partition;
import com.linkedin.pinot.transport.common.PartitionGroup;
import com.linkedin.pinot.transport.common.ServerInstance;

/**
 * Maintains static routing config of servers to partitions
 * 
 * Relevant config for illustration:
 * 
 * pinot.broker.routing.midas.numPartitions=2
 * pinot.broker.routing.midas.serversForPartitions.default=localhost:9099
 * pinot.broker.routing.midas.serversForPartitions.0=localhost:9099
 * pinot.broker.routing.midas.serversForPartitions.1=localhost:9099
 * 
 * @author bvaradar
 *
 */
public class ResourceRoutingConfig
{
  // Keys to load config
  private static final String NUM_PARTITIONS = "numPartitions";
  private static final String SERVERS_FOR_PARTITIONS = "serversForPartitions";
  private static final String DEFAULT_SERVERS_FOR_PARTITIONS = "default";

  private final Configuration _resourcePartitionCfg;
  private int _numPartitions;
  private List<ServerInstance> _defaultServers;
  private final Map<Partition, List<ServerInstance>> _partitionToInstancesMap;
  private final Map<List<ServerInstance>, PartitionGroup> _instancesToPGMap;

  public ResourceRoutingConfig(Configuration cfg)
  {
    _resourcePartitionCfg = cfg;
    _partitionToInstancesMap = new HashMap<Partition, List<ServerInstance>>();
    _instancesToPGMap = new HashMap<List<ServerInstance>, PartitionGroup>();
    loadConfig();
  }

  @SuppressWarnings("unchecked")
  private void loadConfig()
  {
    _partitionToInstancesMap.clear();
    _instancesToPGMap.clear();

    _numPartitions = _resourcePartitionCfg.getInt(NUM_PARTITIONS);
    for (int i = 0; i < _numPartitions; i++)
    {
      List<String> servers = _resourcePartitionCfg.getList(getKey(SERVERS_FOR_PARTITIONS, i +""));

      if ((null != servers) && (!servers.isEmpty()))
      {
        Partition p = new Partition(i);
        List<ServerInstance> servers2 = getServerInstances(servers);
        _partitionToInstancesMap.put(p, servers2);
      }
    }

    List<String> servers = _resourcePartitionCfg.getList(getKey(SERVERS_FOR_PARTITIONS, DEFAULT_SERVERS_FOR_PARTITIONS));
    if ((null != servers) && (!servers.isEmpty()))
    {
      _defaultServers = getServerInstances(servers);
    }

    //Build inverse Map
    for(Entry<Partition, List<ServerInstance>> e : _partitionToInstancesMap.entrySet())
    {
      List<ServerInstance> instances = e.getValue();
      PartitionGroup pg = _instancesToPGMap.get(instances);
      if ( null != pg)
      {
        pg.addPartition(e.getKey());
      } else {
        pg = new PartitionGroup();
        pg.addPartition(e.getKey());
        _instancesToPGMap.put(e.getValue(), pg);
      }
    }
  }

  private String getKey(String prefix, String suffix)
  {
    String s = prefix + "." + suffix;
    return s;
  }

  public int getNumPartitions() {
    return _numPartitions;
  }

  public List<ServerInstance> getDefaultServers() {
    return _defaultServers;
  }


  public Map<Partition, List<ServerInstance>> getPartitionToInstancesMap() {
    return _partitionToInstancesMap;
  }

  public Map<List<ServerInstance>, PartitionGroup> getInstancesToPartitionGroupMap() {
    return _instancesToPGMap;
  }

  /**
   * Builds a map needed for routing the partitions in the partition-group passed.
   * There could be different set of servers for each partition in the passed partition-group.
   * 
   * @param pg PartitionGroup for which the routing map needs to be built.
   * @return
   */
  public Map<PartitionGroup, List<ServerInstance>> buildRequestRoutingMap(PartitionGroup pg)
  {
    Map<PartitionGroup, List<ServerInstance>> resultMap = new HashMap<PartitionGroup, List<ServerInstance>>();
    Map<List<ServerInstance>, PartitionGroup> resultMap2 = new HashMap<List<ServerInstance>, PartitionGroup>();

    for (Partition p : pg.getPartitions())
    {
      List<ServerInstance> instances = _partitionToInstancesMap.get(p);
      if ( null == instances) {
        instances = _defaultServers;
      }

      if ( (null == instances) || (instances.isEmpty())) {
        throw new RuntimeException("Unable to find servers for partition (" + p + ")");
      }

      PartitionGroup pg2 = resultMap2.get(instances);

      if (null == pg2)
      {
        pg2 = new PartitionGroup();
        resultMap2.put(instances, pg2);
      }
      pg2.addPartition(p);
    }

    for (Entry<List<ServerInstance>, PartitionGroup> e : resultMap2.entrySet())
    {
      resultMap.put(e.getValue(), e.getKey());
    }
    return resultMap;
  }

  /**
   * Generate server instances from their string representations
   * @param servers
   * @return
   */
  private static List<ServerInstance> getServerInstances(List<String> servers)
  {
    List<ServerInstance> servers2 = new ArrayList<ServerInstance>();
    for (String s : servers)
    {
      servers2.add(new ServerInstance(s));
    }
    return servers2;
  }
}