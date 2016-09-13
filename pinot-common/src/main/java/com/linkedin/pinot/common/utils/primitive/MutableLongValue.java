/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils.primitive;

import com.linkedin.pinot.common.utils.EqualityUtils;
import java.io.Serializable;
import java.nio.ByteBuffer;


/**
 * Simple wrapper for a mutable long value.
 */
public class MutableLongValue extends Number implements Serializable, Comparable<MutableLongValue> {
  private static final long serialVersionUID = 5072151922348726313L;

  private long value;

  public MutableLongValue(long value) {
    this.value = value;
  }

  public MutableLongValue() {
    value = 0L;
  }

  @Override
  public int intValue() {
    return (int) value;
  }

  @Override
  public long longValue() {
    return value;
  }

  @Override
  public float floatValue() {
    return value;
  }

  @Override
  public double doubleValue() {
    return value;
  }

  public long getValue() {
    return value;
  }

  public void setValue(long value) {
    this.value = value;
  }

  public void addToValue(long otherValue) {
    value += otherValue;
  }

  @Override
  public String toString() {
    return Long.toString(value);
  }

  @Override
  public int compareTo(MutableLongValue o) {
    return Long.compare(value, o.value);
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    MutableLongValue that = (MutableLongValue) o;
    return EqualityUtils.isEqual(value, that.value);
  }

  @Override
  public int hashCode() {
    return EqualityUtils.hashCodeOf(value);
  }

  /**
   * Serializer for instance of the class.
   * Serialized byte-array can be de-serialized by {@ref fromBytes}
   *
   * @return Serialized byte array for the instance.
   */
  public byte[] toBytes() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Long.SIZE >> 3);
    byteBuffer.putLong(value);
    return byteBuffer.array();
  }

  /**
   * De-serializer for instance of the class.
   * Assumes that byte-array was serialized by {@ref toBytes}
   *
   * @param bytes Serialized bytes for the instance
   * @return De-serialized object from the provided byte-array.
   */
  public static MutableLongValue fromBytes(byte[] bytes) {
    long value = ByteBuffer.wrap(bytes).getLong();
    return new MutableLongValue(value);
  }
}
