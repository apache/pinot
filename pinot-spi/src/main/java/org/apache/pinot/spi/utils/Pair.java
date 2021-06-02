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
package org.apache.pinot.spi.utils;

import java.io.Serializable;
import java.util.Objects;


public class Pair<T1 extends Serializable, T2 extends Serializable> implements Serializable {
  private static final long serialVersionUID = -2776898111501466320L;

  private T1 _first;
  private T2 _second;

  public Pair(T1 first, T2 second) {
    _first = first;
    _second = second;
  }

  public T1 getFirst() {
    return _first;
  }

  public T2 getSecond() {
    return _second;
  }

  public void setFirst(T1 first) {
    _first = first;
  }

  public void setSecond(T2 second) {
    _second = second;
  }

  @Override
  public String toString() {
    return "first=" + _first + ", second=" + _second;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Pair<?, ?> pair = (Pair<?, ?>) o;
    return Objects.equals(_first, pair._first) && Objects.equals(_second, pair._second);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_first, _second);
  }
}
