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
package org.apache.pinot.segment.local.segment.index.readers;

import java.math.BigDecimal;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class IntDictionary extends BaseImmutableDictionary {

  public IntDictionary(PinotDataBuffer dataBuffer, int length) {
    super(dataBuffer, length, Integer.BYTES, (byte) 0);
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    return binarySearch(Integer.parseInt(stringValue));
  }

  @Override
  public DataType getValueType() {
    return DataType.INT;
  }

  @Override
  public Integer get(int dictId) {
    return getInt(dictId);
  }

  @Override
  public int getIntValue(int dictId) {
    return getInt(dictId);
  }

  @Override
  public long getLongValue(int dictId) {
    return getInt(dictId);
  }

  @Override
  public float getFloatValue(int dictId) {
    return getInt(dictId);
  }

  @Override
  public double getDoubleValue(int dictId) {
    return getInt(dictId);
  }

  @Override
  public String getStringValue(int dictId) {
    return Integer.toString(getInt(dictId));
  }

  @Override
  public BigDecimal getBigDecimalValue(int dictId) {
    return BigDecimal.valueOf(getInt(dictId));
  }
}
