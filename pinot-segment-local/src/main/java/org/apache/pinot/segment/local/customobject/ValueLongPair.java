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
package org.apache.pinot.segment.local.customobject;

public abstract class ValueLongPair<V extends Comparable<V>> implements Comparable<ValueLongPair<V>> {
  protected V _value;
  protected long _time;

  public ValueLongPair(V value, long time) {
    _value = value;
    _time = time;
  }

  public V getValue() {
    return _value;
  }

  public long getTime() {
    return _time;
  }

  public void setValue(V value) {
    _value = value;
  }

  public void setTime(long time) {
    _time = time;
  }

  abstract public byte[] toBytes();

  @Override
  public int compareTo(ValueLongPair<V> o) {
    if (_time < o.getTime()) {
      return -1;
    } else if (_time > o.getTime()) {
      return 1;
    } else {
      return _value.compareTo(o.getValue());
    }
  }
}
