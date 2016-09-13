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
package com.linkedin.pinot.common.utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


/**
 * Interface for serialization/de-serialization of objects in data table.
 *
 */
public interface DataTableSerDe {

  /**
   * Enum for data types supported for serialization/de-serialization.
   */
  enum DataType {
    Object(-1),
    String(0),
    MutableLong(1),
    Double(2),
    DoubleArrayList(3),
    AvgPair(4),
    MinMaxRangePair(5),
    HyperLogLog(6),
    QuantileDigest(7),
    HashMap(8),
    IntOpenHashSet(9);

    private int _value;
    private static Map<Integer, DataType> _map = new HashMap<>();

    // Map for integer to version for valueOf().
    static {
      for (DataType version : DataType.values()) {
        _map.put(version._value, version);
      }
    }

    DataType(int value) {
      this._value = value;
    }

    public int getValue() {
      return _value;
    }

    public static DataType valueOf(int value) {
      if (!_map.containsKey(value)) {
        throw new IllegalArgumentException("Illegal argument for valueOf enum " + value);
      }
      return _map.get(value);
    }
  }

  /**
   * Serialize the specified object into a byte-array.
   * The byte-array can be de-serialized by {@ref deserialize}.
   *
   * @param object Object to serialize
   * @return Serialized byte-array for the object
   */
  byte[] serialize(Object object);

  /**
   * De-serialize the specified byte-array into object of type T.
   * Assumes that the object was serialized by {@ref serialize}.
   *
   * @param bytes Serialized byte-array
   * @param dataType Enum type of the object
   * @param <T> Class of the object
   * @return De-serialized object
   */
  <T extends Serializable> T deserialize(byte[] bytes, DataType dataType);

  DataType getObjectType(Object value);
}
