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

import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * DocId dictionary for the segment
 */
public class DocIdDictionary extends BaseImmutableDictionary {
  final int _numDocs;

  public DocIdDictionary(int numDocs) {
    super(numDocs);
    _numDocs = numDocs;
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    int intValue = Integer.parseInt(stringValue);
    if (intValue < 0) {
      return -1;
    } else if (intValue >= _numDocs) {
      return -(_numDocs + 1);
    } else {
      return intValue;
    }
  }

  @Override
  public DataType getValueType() {
    return DataType.INT;
  }

  @Override
  public Integer get(int dictId) {
    return dictId;
  }

  @Override
  public int getIntValue(int dictId) {
    return dictId;
  }

  @Override
  public long getLongValue(int dictId) {
    return dictId;
  }

  @Override
  public float getFloatValue(int dictId) {
    return dictId;
  }

  @Override
  public double getDoubleValue(int dictId) {
    return dictId;
  }

  @Override
  public String getStringValue(int dictId) {
    return Integer.toString(dictId);
  }
}
