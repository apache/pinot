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
package com.linkedin.pinot.core.operator.blocks;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


/**
 * This class encapsulates meta-data for a transformed block.
 */
public class TransformBlockMetadata implements BlockMetadata {

  private final int _numDocs;
  private final FieldSpec.DataType _dataType;

  /**
   * Constructor for the class.
   * @param numDocs Number of docs in the block
   * @param dataType Type of data in the block
   */
  public TransformBlockMetadata(int numDocs, FieldSpec.DataType dataType) {
    _numDocs = numDocs;
    _dataType = dataType;
  }

  @Override
  public int getSize() {
    return _numDocs;
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
    return 0;
  }

  @Override
  public boolean isSorted() {
    return false;
  }

  @Override
  public boolean isSparse() {
    return false;
  }

  @Override
  public boolean hasInvertedIndex() {
    return false;
  }

  @Override
  public boolean hasDictionary() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public Dictionary getDictionary() {
    return null;
  }

  @Override
  public int getMaxNumberOfMultiValues() {
    return 0;
  }

  @Override
  public FieldSpec.DataType getDataType() {
    return _dataType;
  }
}
