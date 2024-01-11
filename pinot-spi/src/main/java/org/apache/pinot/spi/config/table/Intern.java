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
 * Class that holds the configurations regarding interning.
 */
public class Intern {
  public static final Intern DISABLED = new Intern(true, 0);

  private boolean _disabled;
  private int _capacity;

  public Intern(int capacity) {
    this(false, capacity);
  }

  @JsonCreator
  public Intern(@JsonProperty("disabled") boolean disabled, @JsonProperty("capacity") int capacity) {
    Preconditions.checkState(capacity > 0 || disabled, "Invalid interner capacity: " + capacity);
    Preconditions.checkState(capacity == 0 || !disabled, "Enable interning to use capacity > 0");

    _disabled = disabled;
    _capacity = capacity;
  }

  public boolean isDisabled() {
    return _disabled;
  }

  public int getCapacity() {
    return _capacity;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Intern that = (Intern) o;
    return _disabled == that._disabled && _capacity == that._capacity;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _disabled, _capacity);
  }

  @Override
  public String toString() {
    return "\"disabled\":" + _disabled + ", \"internerCapacity\":" + _capacity;
  }
}
