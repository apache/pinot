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
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.util.Objects;


public class BloomFilterConfig extends IndexConfig {
  public static final double DEFAULT_FPP = 0.05;
  public static final BloomFilterConfig DEFAULT = new BloomFilterConfig(null, null, null, null);
  public static final BloomFilterConfig DISABLED = new BloomFilterConfig(true, null, null, null);

  private final Double _fpp;
  private final Integer _maxSizeInBytes;
  private final Boolean _loadOnHeap;

  public BloomFilterConfig(double fpp, int maxSizeInBytes, boolean loadOnHeap) {
    // Forward to the wrapper @JsonCreator. Boolean.FALSE for arg0 disambiguates from the deprecated
    // primitive overload below (which would otherwise tie on overload resolution).
    this(Boolean.FALSE, Double.valueOf(fpp), Integer.valueOf(maxSizeInBytes), Boolean.valueOf(loadOnHeap));
  }

  /// Binary-compat delegate for the pre-wrapper `(Boolean, double, int, boolean)` ctor. Kept so existing
  /// compiled call sites resolved to the primitive signature continue to link. Prefer the wrapper-typed
  /// `@JsonCreator` ctor below for new code.
  @Deprecated
  public BloomFilterConfig(Boolean disabled, double fpp, int maxSizeInBytes, boolean loadOnHeap) {
    this(disabled, Double.valueOf(fpp), Integer.valueOf(maxSizeInBytes), Boolean.valueOf(loadOnHeap));
  }

  @JsonCreator
  public BloomFilterConfig(@JsonProperty("disabled") Boolean disabled, @JsonProperty(value = "fpp") Double fpp,
      @JsonProperty(value = "maxSizeInBytes") Integer maxSizeInBytes,
      @JsonProperty(value = "loadOnHeap") Boolean loadOnHeap) {
    super(disabled);
    if (fpp != null && fpp != 0.0) {
      Preconditions.checkArgument(fpp > 0.0 && fpp < 1.0, "Invalid fpp (false positive probability): %s", fpp);
    }
    _fpp = fpp;
    _maxSizeInBytes = maxSizeInBytes;
    _loadOnHeap = loadOnHeap;
  }

  public double getFpp() {
    return _fpp != null && _fpp != 0.0 ? _fpp : DEFAULT_FPP;
  }

  public int getMaxSizeInBytes() {
    return _maxSizeInBytes != null ? _maxSizeInBytes : 0;
  }

  public boolean isLoadOnHeap() {
    return Boolean.TRUE.equals(_loadOnHeap);
  }

  /// Curated slim serializer. See [IndexConfig#toJsonObject()] for the rationale.
  ///
  /// Each field is emitted only when it was explicitly configured (non-null). Sentinel `fpp == 0.0` is also
  /// emitted as-is — the runtime getter still resolves to [#DEFAULT_FPP] for that legacy sentinel.
  @Override
  @JsonValue
  public ObjectNode toJsonObject() {
    ObjectNode node = super.toJsonObject();
    if (_fpp != null) {
      node.put("fpp", _fpp);
    }
    if (_maxSizeInBytes != null) {
      node.put("maxSizeInBytes", _maxSizeInBytes);
    }
    if (_loadOnHeap != null) {
      node.put("loadOnHeap", _loadOnHeap);
    }
    return node;
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
    return Objects.equals(_fpp, that._fpp) && Objects.equals(_maxSizeInBytes, that._maxSizeInBytes)
        && Objects.equals(_loadOnHeap, that._loadOnHeap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _fpp, _maxSizeInBytes, _loadOnHeap);
  }
}
