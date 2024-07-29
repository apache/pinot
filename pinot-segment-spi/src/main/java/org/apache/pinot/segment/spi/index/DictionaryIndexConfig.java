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
import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.Intern;


public class DictionaryIndexConfig extends IndexConfig {

  public static final DictionaryIndexConfig DEFAULT = new DictionaryIndexConfig(false, false, false, Intern.DISABLED);
  public static final DictionaryIndexConfig DISABLED = new DictionaryIndexConfig(true, false, false, Intern.DISABLED);

  private final boolean _onHeap;
  private final boolean _useVarLengthDictionary;
  private final Intern _intern;

  public DictionaryIndexConfig(Boolean onHeap, @Nullable Boolean useVarLengthDictionary) {
    this(onHeap, useVarLengthDictionary, null);
  }

  public DictionaryIndexConfig(Boolean onHeap, @Nullable Boolean useVarLengthDictionary, Intern intern) {
    this(false, onHeap, useVarLengthDictionary, intern);
  }

  @JsonCreator
  public DictionaryIndexConfig(@JsonProperty("disabled") Boolean disabled, @JsonProperty("onHeap") Boolean onHeap,
      @JsonProperty("useVarLengthDictionary") @Nullable Boolean useVarLengthDictionary,
      @JsonProperty("intern") @Nullable Intern intern) {
    super(disabled);

    if (intern != null) {
      // Intern configs only work with onHeapDictionary. This precondition can be removed when/if we support interning
      // for off-heap dictionary.
      Preconditions.checkState(intern.isDisabled() || Boolean.TRUE.equals(onHeap),
          "Intern configs only work with on-heap dictionary");
    }

    _onHeap = onHeap != null && onHeap;
    _useVarLengthDictionary = Boolean.TRUE.equals(useVarLengthDictionary);
    _intern = intern;
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

  public Intern getIntern() {
    return _intern;
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
    return _onHeap == that._onHeap && _useVarLengthDictionary == that._useVarLengthDictionary && Objects.equals(_intern,
        that._intern);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_onHeap, _useVarLengthDictionary, _intern);
  }

  @Override
  public String toString() {
    if (isEnabled()) {
      String internStr = _intern == null ? "null" : _intern.toString();
      return "DictionaryIndexConfig{" + "\"onHeap\":" + _onHeap + ", \"useVarLengthDictionary\":"
          + _useVarLengthDictionary + ", \"intern\":" + internStr + "}";
    } else {
      return "DictionaryIndexConfig{" + "\"disabled\": true}";
    }
  }
}
