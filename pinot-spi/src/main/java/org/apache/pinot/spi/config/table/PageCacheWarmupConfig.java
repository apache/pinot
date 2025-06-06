/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.pinot.spi.config.BaseJsonConfig;

import javax.annotation.Nullable;

/**
 * This configuration is used to control the behavior of the page cache warmup process.
 */
public class PageCacheWarmupConfig extends BaseJsonConfig {

  // Whether page cache warmup be enabled on restart
  private final boolean _restartEnabled;
  // Whether page cache warmup be enabled on refresh
  private final boolean _refreshEnabled;
  // Duration in seconds for which the page cache warmup should run
  private final int _maxWarmupDurationSeconds;
  // Query Selection Policy for page cache warmup, can be null
  // This is used to define a strategy for how queries should be selected for warmup
  @Nullable
  private final String _policy;

  private static final int DEFAULT_WARMUP_DURATION_SECONDS = 180;

  @JsonCreator
  public PageCacheWarmupConfig(@JsonProperty("restartEnabled") boolean restartEnabled,
                               @JsonProperty("refreshEnabled") boolean refreshEnabled,
                               @JsonProperty("maxWarmupDurationSeconds") Integer maxWarmupDurationSeconds,
                               @Nullable @JsonProperty("policy") String policy) {
    _restartEnabled = restartEnabled;
    _refreshEnabled = refreshEnabled;
    _maxWarmupDurationSeconds = (maxWarmupDurationSeconds != null)
        ? maxWarmupDurationSeconds
        : DEFAULT_WARMUP_DURATION_SECONDS;
    _policy = policy;
  }

  // Getters
  @JsonProperty("restartEnabled")
  public boolean isRestartEnabled() {
    return _restartEnabled;
  }

  @JsonProperty("refreshEnabled")
  public boolean isRefreshEnabled() {
    return _refreshEnabled;
  }

  @JsonProperty("maxWarmupDurationSeconds")
  public Integer getMaxWarmupDurationSeconds() {
    return _maxWarmupDurationSeconds;
  }

  @Nullable
  @JsonProperty("policy")
  public String getPolicy() {
    return _policy;
  }

}
