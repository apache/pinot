/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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


public class ThreadPoolConfig {

  /** the number of threads to keep in the pool, even if they are idle **/
  public static final String CORE_POOL_SIZE_KEY = "corePoolSize";

  /** the maximum number of threads to allow in the pool **/
  public static final String MAX_POOL_SIZE_KEY = "maxPoolSize";

  /**
   * when the number of threads is greater than
   * the core, this is the maximum time that excess idle threads
   * will wait for new tasks before terminating.
   */
  public static final String IDLE_TIMEOUT_MS_KEY = "idleTimeoutMs";

  //TODO: Need to revisit if they are good
  private static final int DEFAULT_CORE_POOL_SIZE = 10;
  private static final int DEFAULT_MAX_POOL_SIZE = 10;
  private static final long DEFAULT_IDLE_TIMEOUT_MS = 12 * 60L * 60 * 1000L; // 12 hours

  private int _corePoolSize;
  private int _maxPoolSize;
  private long _idleTimeoutMs;

  public ThreadPoolConfig() {
    _corePoolSize = DEFAULT_CORE_POOL_SIZE;
    _maxPoolSize = DEFAULT_MAX_POOL_SIZE;
    _idleTimeoutMs = DEFAULT_IDLE_TIMEOUT_MS;
  }

  public void init(Configuration cfg) {
    if (cfg.containsKey(CORE_POOL_SIZE_KEY)) {
      _corePoolSize = cfg.getInt(CORE_POOL_SIZE_KEY);
    }

    if (cfg.containsKey(MAX_POOL_SIZE_KEY)) {
      _maxPoolSize = cfg.getInt(MAX_POOL_SIZE_KEY);
    }

    if (cfg.containsKey(IDLE_TIMEOUT_MS_KEY)) {
      _idleTimeoutMs = cfg.getLong(IDLE_TIMEOUT_MS_KEY);
    }
  }

  public int getCorePoolSize() {
    return _corePoolSize;
  }

  public int getMaxPoolSize() {
    return _maxPoolSize;
  }

  public long getIdleTimeoutMs() {
    return _idleTimeoutMs;
  }
}
