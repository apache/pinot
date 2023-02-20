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


public class DictionaryIndexConfig {

  public static final DictionaryIndexConfig DEFAULT = new DictionaryIndexConfig(false, false);

  private final boolean _onHeap;
  private final boolean _useVarLengthDictionary;

  @JsonCreator
  public DictionaryIndexConfig(@JsonProperty("onHeap") boolean onHeap,
      @JsonProperty("useVarLengthDictionary") @Nullable Boolean useVarLengthDictionary) {
    _onHeap = onHeap;
    _useVarLengthDictionary = Boolean.TRUE.equals(useVarLengthDictionary);
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
    return "DictionaryIndexConfig{" + "\"onHeap\":" + _onHeap + ", \"useVarLengthDictionary\":"
        + _useVarLengthDictionary + "}";
  }
}
