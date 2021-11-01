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
import org.apache.pinot.spi.utils.BigDecimalUtils;


public class BigDecimalDictionary extends BaseImmutableDictionary {

  public BigDecimalDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue) {
    super(dataBuffer, length, numBytesPerValue, (byte) 0);
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    return binarySearch(BigDecimalUtils.toBigDecimal(stringValue));
  }

  @Override
  public DataType getValueType() {
    return DataType.BIGDECIMAL;
  }

  @Override
  public BigDecimal get(int dictId) {
    return getBigDecimal(dictId);
  }

  @Override
  public int getIntValue(int dictId) {
    return getBigDecimal(dictId).intValue();
  }

  @Override
  public long getLongValue(int dictId) {
    return getBigDecimal(dictId).longValue();
  }

  @Override
  public float getFloatValue(int dictId) {
    return getBigDecimal(dictId).floatValue();
  }

  @Override
  public double getDoubleValue(int dictId) {
    return getBigDecimal(dictId).doubleValue();
  }

  @Override
  public String getStringValue(int dictId) {
    return getBigDecimal(dictId).toString();
  }

  @Override
  public BigDecimal getBigDecimalValue(int dictId) {
    return getBigDecimal(dictId);
  }
}
