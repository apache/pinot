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
import com.google.common.base.Preconditions;
import java.util.Objects;


public class BloomFilterConfig extends IndexConfig {
  public static final double DEFAULT_FPP = 0.05;
  public static final BloomFilterConfig DEFAULT = new BloomFilterConfig(BloomFilterConfig.DEFAULT_FPP, 0, false);
  public static final BloomFilterConfig DISABLED = new BloomFilterConfig(true, BloomFilterConfig.DEFAULT_FPP, 0,
      false);

  private final double _fpp;
  private final int _maxSizeInBytes;
  private final boolean _loadOnHeap;

  public BloomFilterConfig(double fpp, int maxSizeInBytes, boolean loadOnHeap) {
    this(false, fpp, maxSizeInBytes, loadOnHeap);
  }

  @JsonCreator
  public BloomFilterConfig(@JsonProperty("disabled") Boolean disabled, @JsonProperty(value = "fpp") double fpp,
      @JsonProperty(value = "maxSizeInBytes") int maxSizeInBytes,
      @JsonProperty(value = "loadOnHeap") boolean loadOnHeap) {
    super(disabled);
    if (fpp != 0.0) {
      Preconditions.checkArgument(fpp > 0.0 && fpp < 1.0, "Invalid fpp (false positive probability): %s", fpp);
      _fpp = fpp;
    } else {
      _fpp = DEFAULT_FPP;
    }
    _maxSizeInBytes = maxSizeInBytes;
    _loadOnHeap = loadOnHeap;
  }

  public double getFpp() {
    return _fpp;
  }

  public int getMaxSizeInBytes() {
    return _maxSizeInBytes;
  }

  public boolean isLoadOnHeap() {
    return _loadOnHeap;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    BloomFilterConfig that = (BloomFilterConfig) o;
    return Double.compare(that._fpp, _fpp) == 0 && _maxSizeInBytes == that._maxSizeInBytes
        && _loadOnHeap == that._loadOnHeap && isEnabled() == that.isEnabled();
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _fpp, _maxSizeInBytes, _loadOnHeap, isEnabled());
  }
}
