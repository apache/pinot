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
import org.apache.pinot.spi.config.table.IndexConfig;


public class DictionaryIndexConfig extends IndexConfig {

  public static final DictionaryIndexConfig DEFAULT = new DictionaryIndexConfig(false, false, false);
  public static final DictionaryIndexConfig DISABLED = new DictionaryIndexConfig(true, false, false);

  private final boolean _onHeap;
  private final boolean _useVarLengthDictionary;

  public DictionaryIndexConfig(Boolean onHeap, @Nullable Boolean useVarLengthDictionary) {
    this(false, onHeap, useVarLengthDictionary);
  }

  @JsonCreator
  public DictionaryIndexConfig(@JsonProperty("disabled") Boolean disabled, @JsonProperty("onHeap") Boolean onHeap,
      @JsonProperty("useVarLengthDictionary") @Nullable Boolean useVarLengthDictionary) {
    super(disabled);
    _onHeap = onHeap != null && onHeap;
    _useVarLengthDictionary = Boolean.TRUE.equals(useVarLengthDictionary);
  }

  public static DictionaryIndexConfig disabled() {
    return DISABLED;
  }

  public boolean isOnHeap() {
    return _onHeap;
  }

  public boolean getUseVarLengthDictionary() {
    return _useVarLengthDictionary;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DictionaryIndexConfig that = (DictionaryIndexConfig) o;
    return _onHeap == that._onHeap && _useVarLengthDictionary == that._useVarLengthDictionary;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_onHeap, _useVarLengthDictionary);
  }

  @Override
  public String toString() {
    if (isEnabled()) {
      return "DictionaryIndexConfig{" + "\"onHeap\":" + _onHeap + ", \"useVarLengthDictionary\":"
          + _useVarLengthDictionary + "}";
    } else {
      return "DictionaryIndexConfig{" + "\"disabled\": true}";
    }
  }
}
