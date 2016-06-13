/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.transport.config;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private final int DEFAULT_MIN_CONNECTIONS_PER_SERVER = 10;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionPoolConfig.class);

  public ConnectionPoolConfig() {
    _threadPool = new ThreadPoolConfig();
    _minConnectionsPerServer = DEFAULT_MIN_CONNECTIONS_PER_SERVER;
    _maxConnectionsPerServer = DEFAULT_MAX_CONNECTIONS_PER_SERVER;
    _maxBacklogPerServer = DEFAULT_MAX_BACKLOG_PER_SERVER;
    _idleTimeoutMs = DEFAULT_IDLE_TIMEOUT_MS;
  }

  public void init(Configuration cfg) {
    if (cfg.containsKey(THREAD_POOL_KEY)) {
      _threadPool.init(cfg.subset(THREAD_POOL_KEY));
    }

    if (cfg.containsKey(IDLE_TIMEOUT_MS_KEY)) {
      _idleTimeoutMs = cfg.getLong(IDLE_TIMEOUT_MS_KEY);
    }

    if (cfg.containsKey(MIN_CONNECTIONS_PER_SERVER_KEY)) {
      _minConnectionsPerServer = cfg.getInt(MIN_CONNECTIONS_PER_SERVER_KEY);
    }

    if (cfg.containsKey(MAX_CONNECTIONS_PER_SERVER_KEY)) {
      _maxConnectionsPerServer = cfg.getInt(MAX_CONNECTIONS_PER_SERVER_KEY);
    }

    if (cfg.containsKey(MAX_BACKLOG_PER_SERVER_KEY)) {
      _maxBacklogPerServer = cfg.getInt(MAX_BACKLOG_PER_SERVER_KEY);
    }

    if (_minConnectionsPerServer > _maxConnectionsPerServer || _maxConnectionsPerServer <= 0 || _minConnectionsPerServer < 1) {
      LOGGER.warn("Invalid values for " + MIN_CONNECTIONS_PER_SERVER_KEY +  "({}) and " + MAX_CONNECTIONS_PER_SERVER_KEY +
          "({}). Resetting to defaults:", _minConnectionsPerServer, _maxConnectionsPerServer);
      _minConnectionsPerServer = DEFAULT_MIN_CONNECTIONS_PER_SERVER;
      _maxConnectionsPerServer = DEFAULT_MAX_CONNECTIONS_PER_SERVER;
    }
    if (_idleTimeoutMs < 0) {
      LOGGER.warn("Invalid value for " + IDLE_TIMEOUT_MS_KEY +  "({}). Resetting to default.");
      _idleTimeoutMs = DEFAULT_IDLE_TIMEOUT_MS;
    }
    if (_maxBacklogPerServer < 0) {
      LOGGER.warn("Invalid value for " + MAX_BACKLOG_PER_SERVER_KEY + "({}). Resetting to default.");
      _maxBacklogPerServer = DEFAULT_MAX_BACKLOG_PER_SERVER;
    }

    LOGGER.info(toString());
  }

  public String toString() {
    return "threadPool = "+_threadPool+", idleTimeoutMs = "+_idleTimeoutMs+
            ", minConnectionsPerServer = "+_minConnectionsPerServer+
            ", maxConnectionsPerServer = "+_maxConnectionsPerServer+
            ", maxBacklogPerServer = "+_maxBacklogPerServer;
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
