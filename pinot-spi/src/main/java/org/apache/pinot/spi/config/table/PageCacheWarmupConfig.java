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
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;

/**
 * Controls the behavior of the page cache warmup process.
 *
 * <p>Warmup runs in two independent cases that share the same set of knobs: when a server
 * restarts ({@code onRestart}) and after a segment is refreshed ({@code onRefresh}). Each case is
 * configured with its own {@link Spec} block so it can be enabled/disabled and tuned separately.
 * A {@code null}/absent block disables warmup for that case.
 */
public class PageCacheWarmupConfig extends BaseJsonConfig {

  // Warmup settings applied when a server restarts; null disables restart warmup
  @Nullable
  private final Spec _onRestart;
  // Warmup settings applied after a segment refresh; null disables refresh warmup
  @Nullable
  private final Spec _onRefresh;

  @JsonCreator
  public PageCacheWarmupConfig(@JsonProperty("onRestart") @Nullable Spec onRestart,
      @JsonProperty("onRefresh") @Nullable Spec onRefresh) {
    _onRestart = onRestart;
    _onRefresh = onRefresh;
  }

  @Nullable
  @JsonProperty("onRestart")
  public Spec getOnRestart() {
    return _onRestart;
  }

  @Nullable
  @JsonProperty("onRefresh")
  public Spec getOnRefresh() {
    return _onRefresh;
  }

  /**
   * Per-case page cache warmup settings. The same shape is reused for the server-restart and
   * segment-refresh cases (see {@link #getOnRestart()} / {@link #getOnRefresh()}).
   */
  public static class Spec extends BaseJsonConfig {
    private static final int DEFAULT_WARMUP_DURATION_SECONDS = 180;

    // Whether page cache warmup is enabled for this case
    private final boolean _enabled;
    // Duration in seconds for which warmup should run (defaults to 180 when not set)
    private final Integer _maxWarmupDurationSeconds;
    // Warmup QPS limit; defaults to the table QPS limit per replica when not set
    @Nullable
    private final Integer _qpsLimit;
    // Query-selection policy: selects which warmup queries to replay for this case (e.g. choose
    // among the stored query sets). Reserved for future query-selection strategies; currently
    // carried through config without a consumer. Nullable.
    @Nullable
    private final String _policy;

    @JsonCreator
    public Spec(@JsonProperty("enabled") boolean enabled,
        @JsonProperty("maxWarmupDurationSeconds") @Nullable Integer maxWarmupDurationSeconds,
        @JsonProperty("qpsLimit") @Nullable Integer qpsLimit,
        @JsonProperty("policy") @Nullable String policy) {
      _enabled = enabled;
      _maxWarmupDurationSeconds =
          (maxWarmupDurationSeconds != null) ? maxWarmupDurationSeconds : DEFAULT_WARMUP_DURATION_SECONDS;
      _qpsLimit = qpsLimit;
      _policy = policy;
    }

    @JsonProperty("enabled")
    public boolean isEnabled() {
      return _enabled;
    }

    @JsonProperty("maxWarmupDurationSeconds")
    public Integer getMaxWarmupDurationSeconds() {
      return _maxWarmupDurationSeconds;
    }

    @Nullable
    @JsonProperty("qpsLimit")
    public Integer getQpsLimit() {
      return _qpsLimit;
    }

    @Nullable
    @JsonProperty("policy")
    public String getPolicy() {
      return _policy;
    }
  }
}
