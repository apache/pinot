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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.utils.DataSizeUtils;


/**
 * Class representing table quota configuration
 */
public class QuotaConfig extends BaseJsonConfig {
  private static final long INVALID_STORAGE_IN_BYTES = -1L;
  private static final double INVALID_RATELIMITER_DURATION = -1;
  private static final double INVALID_RATELIMITS = -1;

  @JsonPropertyDescription("Storage allocated for this table, e.g. \"10G\"")
  private final String _storage;

  @Deprecated
  private final String _maxQueriesPerSecond = null;

  @Deprecated
  private transient final double _maxQPS = -1d;

  private final TimeUnit _rateLimiterUnit;
  private final double _rateLimiterDuration;
  private final double _rateLimits;

  // NOTE: These two fields are not to be serialized
  private transient final long _storageInBytes;

  @Deprecated
  public QuotaConfig(@JsonProperty("storage") @Nullable String storage,
      @JsonProperty("maxQueriesPerSecond") @Nullable String maxQueriesPerSecond) {
    this(storage, null, null, null);
  }

  @JsonCreator
  public QuotaConfig(@JsonProperty("storage") @Nullable String storage,
      @JsonProperty("rateLimiterUnit") @Nullable TimeUnit rateLimiterUnit,
      @JsonProperty("rateLimiterDuration") @Nullable Double rateLimiterDuration,
      @JsonProperty("rateLimits") @Nullable Double rateLimits) {
    // Validate and standardize the value
    if (storage != null) {
      try {
        _storageInBytes = DataSizeUtils.toBytes(storage);
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid 'storage': " + storage);
      }
      _storage = DataSizeUtils.fromBytes(_storageInBytes);
    } else {
      _storageInBytes = INVALID_STORAGE_IN_BYTES;
      _storage = null;
    }

    if (rateLimiterUnit != null && rateLimiterDuration != null && rateLimits != null) {
      try {
        Preconditions.checkArgument(rateLimiterDuration > 0);
        Preconditions.checkArgument(rateLimits > 0);
        _rateLimiterUnit = rateLimiterUnit;
        _rateLimiterDuration = rateLimiterDuration;
        _rateLimits = rateLimits;
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid 'ratelimits arguments': " + storage);
      }
    } else {
      _rateLimiterUnit = null;
      _rateLimiterDuration = INVALID_RATELIMITER_DURATION;
      _rateLimits = INVALID_RATELIMITS;
    }
  }

  @Nullable
  public String getStorage() {
    return _storage;
  }

  @JsonIgnore
  public long getStorageInBytes() {
    return _storageInBytes;
  }

  public TimeUnit getRateLimiterUnit() {
    return _rateLimiterUnit;
  }

  public double getRateLimiterDuration() {
    return _rateLimiterDuration;
  }

  public double getRateLimits() {
    return _rateLimits;
  }

  public boolean isQuotaConfigSet() {
    return _rateLimits != INVALID_RATELIMITS;
  }

  @Deprecated
  public String getMaxQueriesPerSecond() {
    return _maxQueriesPerSecond;
  }

  @Deprecated
  public double getMaxQPS() {
    return _maxQPS;
  }
}
