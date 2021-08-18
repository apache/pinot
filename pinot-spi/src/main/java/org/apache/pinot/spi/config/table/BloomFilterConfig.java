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
import org.apache.pinot.spi.config.BaseJsonConfig;


public class BloomFilterConfig extends BaseJsonConfig {
  public static final double DEFAULT_FPP = 0.05;

  private final double _fpp;
  private final int _maxSizeInBytes;
  private final boolean _loadOnHeap;

  @JsonCreator
  public BloomFilterConfig(@JsonProperty(value = "fpp") double fpp, @JsonProperty(value = "maxSizeInBytes") int maxSizeInBytes,
      @JsonProperty(value = "loadOnHeap") boolean loadOnHeap) {
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
}
