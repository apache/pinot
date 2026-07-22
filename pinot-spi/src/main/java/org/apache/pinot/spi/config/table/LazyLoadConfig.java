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
 * Table-level config for lazy segment loading (OFFLINE tables only).
 *
 * <p>When enabled, ONLINE segment assignments register a metadata-only stub on the server instead of downloading
 * the segment from the deep store. The first query that touches a stubbed segment downloads, untars and loads it,
 * after which it serves at native speed. A background sweeper evicts segments that have been idle for longer than
 * {@code idleEvictionSeconds} back to stubs, optionally deleting the local index directory to free disk. The
 * segment stays routable throughout — the authoritative copy remains in the deep store.
 */
public class LazyLoadConfig extends BaseJsonConfig {
  public static final long DEFAULT_IDLE_EVICTION_SECONDS = 3600;

  private final boolean _enabled;
  private final long _idleEvictionSeconds;
  private final boolean _deleteLocalOnEvict;

  @JsonCreator
  public LazyLoadConfig(@JsonProperty("enabled") boolean enabled,
      @JsonProperty("idleEvictionSeconds") @Nullable Long idleEvictionSeconds,
      @JsonProperty("deleteLocalOnEvict") @Nullable Boolean deleteLocalOnEvict) {
    _enabled = enabled;
    _idleEvictionSeconds = idleEvictionSeconds != null ? idleEvictionSeconds : DEFAULT_IDLE_EVICTION_SECONDS;
    // Default true: eviction frees disk as well as memory. Set to false for two-tier mode where the local index
    // directory is kept for a fast re-warm with zero deep-store traffic.
    _deleteLocalOnEvict = deleteLocalOnEvict == null || deleteLocalOnEvict;
  }

  public boolean isEnabled() {
    return _enabled;
  }

  public long getIdleEvictionSeconds() {
    return _idleEvictionSeconds;
  }

  public boolean isDeleteLocalOnEvict() {
    return _deleteLocalOnEvict;
  }
}
