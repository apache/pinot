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
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.utils.DataSizeUtils;


/**
 * Class representing table quota configuration
 */
public class QuotaConfig extends BaseJsonConfig {
  private static final long INVALID_STORAGE_IN_BYTES = -1L;
  private static final double INVALID_MAX_QPS = -1.0;

  @JsonPropertyDescription("Storage allocated for this table, e.g. \"10G\"")
  private final String _storage;

  private final String _maxQueriesPerSecond;

  // NOTE: These two fields are not to be serialized
  private transient final long _storageInBytes;
  private transient final double _maxQPS;

  @JsonCreator
  public QuotaConfig(@JsonProperty("storage") @Nullable String storage,
      @JsonProperty("maxQueriesPerSecond") @Nullable String maxQueriesPerSecond) {
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
    if (maxQueriesPerSecond != null) {
      try {
        _maxQPS = Double.parseDouble(maxQueriesPerSecond);
        Preconditions.checkArgument(_maxQPS > 0);
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid 'maxQueriesPerSecond': " + storage);
      }
      _maxQueriesPerSecond = Double.toString(_maxQPS);
    } else {
      _maxQPS = INVALID_MAX_QPS;
      _maxQueriesPerSecond = null;
    }
  }

  @Nullable
  public String getStorage() {
    return _storage;
  }

  @Nullable
  public String getMaxQueriesPerSecond() {
    return _maxQueriesPerSecond;
  }

  @JsonIgnore
  public long getStorageInBytes() {
    return _storageInBytes;
  }

  @JsonIgnore
  public double getMaxQPS() {
    return _maxQPS;
  }
}
