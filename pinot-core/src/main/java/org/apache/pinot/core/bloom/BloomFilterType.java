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
package org.apache.pinot.core.bloom;

import java.util.HashMap;
import java.util.Map;


/**
 * Enum for bloom filter type
 */
public enum BloomFilterType {
  // NOTE: Do not change the value of bloom filter type when adding a new type since we are writing/checking type value
  // when serializing/deserializing a bloom filter
  GUAVA_ON_HEAP(1);

  private int _value;
  private static Map<Integer, BloomFilterType> _bloomFilterTypeMap = new HashMap<>();

  BloomFilterType(int value) {
    _value = value;
  }

  static {
    for (BloomFilterType pageType : BloomFilterType.values()) {
      _bloomFilterTypeMap.put(pageType._value, pageType);
    }
  }

  public static BloomFilterType valueOf(int pageType) {
    return _bloomFilterTypeMap.get(pageType);
  }

  public int getValue() {
    return _value;
  }
}
