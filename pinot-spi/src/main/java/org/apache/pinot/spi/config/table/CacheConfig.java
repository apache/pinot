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
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Class representing configurations related to table query cache.
 */
public class CacheConfig extends BaseJsonConfig {

  public static final boolean DEFAULT_CACHE_ENABLED = true;
  public static final String DEFAULT_CACHE_TYPE = "InMemory";
  public static final int DEFAULT_CACHE_MAX_SIZE = 1000;
  public static final int DEFAULT_CACHE_TTL = 86400; // 1 day

  @JsonPropertyDescription("Enable or disable the query cache")
  private final boolean _enabled;

  @JsonPropertyDescription("The type of the cache (e.g. 'LRU', 'LFU')")
  private final String _type;

  @JsonPropertyDescription("The maximum size of the cache")
  private final int _maxSize;

  @JsonPropertyDescription("The time to live for the cache entries in seconds")
  private final int _ttl;

  public CacheConfig() {
    this(DEFAULT_CACHE_ENABLED, DEFAULT_CACHE_TYPE, DEFAULT_CACHE_MAX_SIZE, DEFAULT_CACHE_TTL);
  }

  @JsonCreator
  public CacheConfig(@JsonProperty(value = "enabled") boolean enabled,
      @JsonProperty(value = "type") String type,
      @JsonProperty(value = "maxSize") int maxSize,
      @JsonProperty(value = "ttl") int ttl) {
    _enabled = enabled;
    _type = type;
    _maxSize = maxSize;
    _ttl = ttl;
  }

  public boolean isEnabled() {
    return _enabled;
  }

  public String getType() {
    return _type;
  }

  public int getMaxSize() {
    return _maxSize;
  }

  public int getTtl() {
    return _ttl;
  }
}
