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
package org.apache.pinot.core.operator.blocks;

import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.core.common.BlockMetadata;
import org.apache.pinot.core.segment.index.readers.Dictionary;


public final class BlockMetadataImpl implements BlockMetadata {
  private final int _numDocs;
  private final boolean _isSingleValue;
  private final int _maxNumMultiValues;
  private final FieldSpec.DataType _dataType;
  private final Dictionary _dictionary;

  public BlockMetadataImpl(int numDocs, boolean isSingleValue, int maxNumMultiValues, FieldSpec.DataType dataType,
      Dictionary dictionary) {
    _numDocs = numDocs;
    _isSingleValue = isSingleValue;
    _maxNumMultiValues = maxNumMultiValues;
    _dataType = dataType;
    _dictionary = dictionary;
  }

  @Override
  public int getLength() {
    return _numDocs;
  }

  @Override
  public int getStartDocId() {
    return 0;
  }

  @Override
  public int getEndDocId() {
    return _numDocs - 1;
  }

  @Override
  public FieldSpec.DataType getDataType() {
    return _dataType;
  }

  @Override
  public boolean isSingleValue() {
    return _isSingleValue;
  }

  @Override
  public int getMaxNumberOfMultiValues() {
    return _maxNumMultiValues;
  }

  @Override
  public boolean hasDictionary() {
    return _dictionary != null;
  }

  @Override
  public Dictionary getDictionary() {
    return _dictionary;
  }
}
