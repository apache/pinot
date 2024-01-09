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


/**
 * Class that holds the configurations for onheap dictionary.
 */
public class OnHeapDictionaryConfig {
  private boolean _enableInterning;
  private int _internerCapacity;

  @JsonCreator
  public OnHeapDictionaryConfig(@JsonProperty("enableInterning") boolean enableInterning,
      @JsonProperty("internerCapacity") int internerCapacity) {
    Preconditions.checkState(internerCapacity > 0 || !enableInterning,
        "Invalid interner capacity: " + internerCapacity);
    Preconditions.checkState(internerCapacity == 0 || enableInterning, "Enable interning to use internerCapacity.");

    if (internerCapacity > 0 && !enableInterning) {
      throw new IllegalArgumentException("Invalid interner capacity: " + internerCapacity);
    }


    _enableInterning = enableInterning;
    _internerCapacity = internerCapacity;
  }

  public boolean isEnableInterning() {
    return _enableInterning;
  }

  public int getInternerCapacity() {
    return _internerCapacity;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OnHeapDictionaryConfig that = (OnHeapDictionaryConfig) o;
    return _enableInterning == that._enableInterning && _internerCapacity == that._internerCapacity;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _enableInterning, _internerCapacity);
  }

  @Override
  public String toString() {
    return "\"enableInterning\":" + _enableInterning + ", \"internerCapacity\":" + _internerCapacity;
  }
}
