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
  public static final FstIndexConfig DISABLED = new FstIndexConfig(true, null);
  private final FSTType _fstType;

  public FstIndexConfig(@JsonProperty("type") @Nullable FSTType fstType) {
    this(false, fstType);
  }

  @JsonCreator
  public FstIndexConfig(@JsonProperty("disabled") @Nullable Boolean disabled,
      @JsonProperty("type") @Nullable FSTType fstType) {
    super(disabled);
    _fstType = fstType;
  }

  @JsonProperty("type")
  public FSTType getFstType() {
    return _fstType;
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
    return _fstType == that._fstType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_fstType);
  }

  @Override
  public String toString() {
    return "FstIndexConfig{\"fstType\":" + _fstType + '}';
  }
}
