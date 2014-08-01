package com.linkedin.pinot.transport.config;

import org.apache.commons.configuration.Configuration;

public class ConnectionPoolConfig {

  // ThreadPool config for the Async Connection Pool 
  public static final String THREAD_POOL_KEY = "threadPool";
  
  // Minimum number of live connections for each server 
  public static final String MIN_CONNECTIONS_PER_SERVER_KEY = "minConnectionsPerServer";

  // Maximum number of live connections for each server 
  public static final String MAX_CONNECTIONS_PER_SERVER_KEY = "maxConnectionsPerServer";
  
  // Maximum number of pending checkout requests before requests starts getting rejected
  public static final String MAX_BACKLOG_PER_SERVER_KEY = "maxBacklogPerServer";
  
  // Idle Timeout (ms) for reaping idle connections
  public static final String IDLE_TIMEOUT_MS_KEY = "idleTimeoutMs";
  
  private final int DEFAULT_MIN_CONNECTIONS_PER_SERVER = 0;
  private final int DEFAULT_MAX_CONNECTIONS_PER_SERVER = 30;
  private final int DEFAULT_MAX_BACKLOG_PER_SERVER = 30;
  private static final long DEFAULT_IDLE_TIMEOUT_MS = 6 * 60L * 60 * 1000L; // 6 hours
  
  // ThreadPool config for the Async Connection Pool 
  private ThreadPoolConfig _threadPool;
  
  // Minimum number of live connections for each server 
  private int _minConnectionsPerServer;
  
  // Maximum number of live connections for each server
  private int _maxConnectionsPerServer;
  
  // Maximum number of pending checkout requests before requests starts getting rejected
  private int _maxBacklogPerServer;
  
  // Idle Timeout (ms) for reaping idle connections
  private long _idleTimeoutMs;

  public ConnectionPoolConfig()
  {
    _threadPool = new ThreadPoolConfig();
    _minConnectionsPerServer = DEFAULT_MIN_CONNECTIONS_PER_SERVER;
    _maxConnectionsPerServer = DEFAULT_MAX_CONNECTIONS_PER_SERVER;
    _maxBacklogPerServer = DEFAULT_MAX_BACKLOG_PER_SERVER;
    _idleTimeoutMs = DEFAULT_IDLE_TIMEOUT_MS;
  }
  
  
  public void init(Configuration cfg)
  {
    if ( cfg.containsKey(THREAD_POOL_KEY))
    {
      _threadPool.init(cfg.subset(THREAD_POOL_KEY));
    }
    
    if(cfg.containsKey(IDLE_TIMEOUT_MS_KEY))
    {
      _idleTimeoutMs = cfg.getLong(IDLE_TIMEOUT_MS_KEY);
    }
    
    if ( cfg.containsKey(MIN_CONNECTIONS_PER_SERVER_KEY))
    {
      _minConnectionsPerServer = cfg.getInt(MIN_CONNECTIONS_PER_SERVER_KEY);
    }
    
    if ( cfg.containsKey(MAX_CONNECTIONS_PER_SERVER_KEY))
    {
      _maxConnectionsPerServer = cfg.getInt(MAX_CONNECTIONS_PER_SERVER_KEY);
    }
    
    if ( cfg.containsKey(MAX_BACKLOG_PER_SERVER_KEY))
    {
      _maxBacklogPerServer = cfg.getInt(MAX_BACKLOG_PER_SERVER_KEY);
    }
  }

  public ThreadPoolConfig getThreadPool() {
    return _threadPool;
  }


  public int getMinConnectionsPerServer() {
    return _minConnectionsPerServer;
  }


  public int getMaxConnectionsPerServer() {
    return _maxConnectionsPerServer;
  }


  public int getMaxBacklogPerServer() {
    return _maxBacklogPerServer;
  }


  public long getIdleTimeoutMs() {
    return _idleTimeoutMs;
  }
  
}
