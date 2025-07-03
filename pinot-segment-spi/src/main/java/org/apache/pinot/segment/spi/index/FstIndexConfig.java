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

package org.apache.pinot.segment.spi.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.IndexConfig;


public class FstIndexConfig extends IndexConfig {
  public static final FstIndexConfig DISABLED = new FstIndexConfig(true, null, true);
  private final FSTType _fstType;
  private final boolean _caseSensitive;

  public FstIndexConfig(@JsonProperty("type") @Nullable FSTType fstType) {
    this(false, fstType, true);
  }

  /**
   * @deprecated Use {@link #FstIndexConfig(Boolean, FSTType, Boolean)} instead
   */
  @Deprecated
  public FstIndexConfig(@Nullable Boolean disabled, @Nullable FSTType fstType) {
    this(disabled, fstType, true);
  }

  @JsonCreator
  public FstIndexConfig(@JsonProperty("disabled") @Nullable Boolean disabled,
      @JsonProperty("type") @Nullable FSTType fstType,
      @JsonProperty("caseSensitive") @Nullable Boolean caseSensitive) {
    super(disabled);
    _fstType = fstType;
    _caseSensitive = caseSensitive != null ? caseSensitive : true;
  }

  @JsonProperty("type")
  public FSTType getFstType() {
    return _fstType;
  }

  @JsonProperty("caseSensitive")
  public boolean isCaseSensitive() {
    return _caseSensitive;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FstIndexConfig that = (FstIndexConfig) o;
    return _fstType == that._fstType && _caseSensitive == that._caseSensitive;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_fstType, _caseSensitive);
  }

  @Override
  public String toString() {
    return "FstIndexConfig{\"fstType\":" + _fstType + ",\"caseSensitive\":" + _caseSensitive + '}';
  }
}
