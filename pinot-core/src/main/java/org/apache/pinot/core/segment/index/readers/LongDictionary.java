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
package org.apache.pinot.core.segment.index.readers;

import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class LongDictionary extends BaseImmutableDictionary {

  public LongDictionary(PinotDataBuffer dataBuffer, int length) {
    super(dataBuffer, length, Long.BYTES, (byte) 0);
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    return binarySearch(Long.parseLong(stringValue));
  }

  @Override
  public DataType getValueType() {
    return DataType.LONG;
  }

  @Override
  public Long get(int dictId) {
    return getLong(dictId);
  }

  @Override
  public int getIntValue(int dictId) {
    return (int) getLong(dictId);
  }

  @Override
  public long getLongValue(int dictId) {
    return getLong(dictId);
  }

  @Override
  public float getFloatValue(int dictId) {
    return getLong(dictId);
  }

  @Override
  public double getDoubleValue(int dictId) {
    return getLong(dictId);
  }

  @Override
  public String getStringValue(int dictId) {
    return Long.toString(getLong(dictId));
  }
}
