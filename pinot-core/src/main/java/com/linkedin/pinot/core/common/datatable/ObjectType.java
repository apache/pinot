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
package com.linkedin.pinot.core.common.datatable;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * Enum for object types supported for serialization/de-serialization.
 */
public enum ObjectType {
  String(0),
  Long(1),
  Double(2),
  DoubleArrayList(3),
  AvgPair(4),
  MinMaxRangePair(5),
  HyperLogLog(6),
  QuantileDigest(7),
  HashMap(8),
  IntOpenHashSet(9);

  // Map from type value to type.
  private static Map<Integer, ObjectType> _objectTypeMap = new HashMap<>();

  static {
    for (ObjectType objectType : ObjectType.values()) {
      _objectTypeMap.put(objectType._value, objectType);
    }
  }

  private int _value;

  ObjectType(int value) {
    _value = value;
  }

  public int getValue() {
    return _value;
  }

  @Nonnull
  public static ObjectType getObjectType(int value) {
    ObjectType objectType = _objectTypeMap.get(value);
    if (objectType == null) {
      throw new IllegalArgumentException("Illegal value for object type: " + value);
    }
    return objectType;
  }
}
