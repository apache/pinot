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

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;

/**
 * Blockmetadata wrapper on column metadata. Currently we support only one block per segment. We
 * will have to change this when we start supporting multiple blocks per segment
 */
public final class BlockMetadataImpl implements BlockMetadata {

  private final ColumnMetadata columnMetadata;
  private final Dictionary dictionary;

  public BlockMetadataImpl(ColumnMetadata columnMetadata, Dictionary dictionary) {
    this.columnMetadata = columnMetadata;
    this.dictionary = dictionary;
  }

  @Override
  public boolean isSparse() {
    return false;
  }

  @Override
  public boolean isSorted() {
    return columnMetadata.isSorted();
  }

  @Override
  public boolean hasInvertedIndex() {
    return columnMetadata.hasInvertedIndex();
  }

  @Override
  public int getStartDocId() {
    return 0;
  }

  @Override
  public int getSize() {
    return columnMetadata.getTotalDocs();
  }

  @Override
  public int getLength() {
    return columnMetadata.getTotalDocs();
  }

  @Override
  public int getEndDocId() {
    return columnMetadata.getTotalDocs() - 1;
  }

  @Override
  public boolean hasDictionary() {
    return true;
  }

  @Override
  public boolean isSingleValue() {
    return columnMetadata.isSingleValue();
  }

  @Override
  public Dictionary getDictionary() {
    return dictionary;
  }

  @Override
  public int getMaxNumberOfMultiValues() {
    return columnMetadata.getMaxNumberOfMultiValues();
  }

  @Override
  public DataType getDataType() {
    return columnMetadata.getDataType();
  }
}
