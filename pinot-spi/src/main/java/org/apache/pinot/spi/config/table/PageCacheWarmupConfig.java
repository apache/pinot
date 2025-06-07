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

public class PageCacheWarmupConfig extends BaseJsonConfig {

  private final boolean _restartEnabled;
  private final boolean _refreshEnabled;
  private final String _policy;
  private final int _maxWarmupDurationSeconds;

  private static final int DEFAULT_WARMUP_DURATION_SECONDS = 180;

  @JsonCreator
  public PageCacheWarmupConfig(@JsonProperty("restartEnabled") boolean restartEnabled,
                               @JsonProperty("refreshEnabled") boolean refreshEnabled,
                               @JsonProperty("maxWarmupDurationSeconds") Integer maxWarmupDurationSeconds,
                               @JsonProperty("policy") String policy) {
    _restartEnabled = restartEnabled;
    _refreshEnabled = refreshEnabled;
    _maxWarmupDurationSeconds = (maxWarmupDurationSeconds != null)
        ? maxWarmupDurationSeconds
        : DEFAULT_WARMUP_DURATION_SECONDS;
    _policy = policy != null ? policy : "default"; // Default policy if not specified
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

  @Nullable
  @JsonProperty("policy")
  public String getPolicy() {
    return _policy;
  }

  @Nullable
  @JsonProperty("maxWarmupDurationSeconds")
  public Integer getMaxWarmupDurationSeconds() {
    return _maxWarmupDurationSeconds;
  }

}
