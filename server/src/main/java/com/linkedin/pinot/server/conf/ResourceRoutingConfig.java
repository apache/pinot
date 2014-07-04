package com.linkedin.pinot.server.conf;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

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
  private List<String> _defaultServers;
  private final Map<Integer, List<String>> _partitionServerMap;

  public ResourceRoutingConfig(Configuration cfg)
  {
    _resourcePartitionCfg = cfg;
    _partitionServerMap = new HashMap<Integer, List<String>>();
    loadConfig();
  }

  @SuppressWarnings("unchecked")
  private void loadConfig()
  {
    _numPartitions = _resourcePartitionCfg.getInt(NUM_PARTITIONS);
    for (int i = 0; i < _numPartitions; i++)
    {
      List<String> servers = _resourcePartitionCfg.getList(getKey(SERVERS_FOR_PARTITIONS, i +""));

      if ((null != servers) && (!servers.isEmpty()))
      {
        _partitionServerMap.put(i, servers);
      }
    }

    List<String> servers = _resourcePartitionCfg.getList(getKey(SERVERS_FOR_PARTITIONS, DEFAULT_SERVERS_FOR_PARTITIONS));
    if ((null != servers) && (!servers.isEmpty()))
    {
      _defaultServers = servers;
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

  public List<String> getDefaultServers() {
    return _defaultServers;
  }

  public Map<Integer, List<String>> getPartitionServerMap() {
    return _partitionServerMap;
  }

  @Override
  public String toString() {
    return "ResourceRoutingConfig [_resourcePartitionCfg=" + _resourcePartitionCfg + ", _numPartitions="
        + _numPartitions + ", _defaultServers=" + _defaultServers + ", _partitionServerMap=" + _partitionServerMap
        + "]";
  }
}