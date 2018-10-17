/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.startree.v2.store;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;
import com.linkedin.pinot.core.operator.blocks.SingleValueBlock;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;


/**
 * The {@code StarTreeDimensionDataSource} class represents the data source for a dimension in star-tree.
 */
public class StarTreeDimensionDataSource extends DataSource {
  private final int _numDocs;
  private final FieldSpec.DataType _dataType;
  private final Dictionary _dictionary;
  private final FixedBitSingleValueReader _forwardIndex;
  private final DataSourceMetadata _metadata;
  private final String _operatorName;

  public StarTreeDimensionDataSource(PinotDataBuffer dataBuffer, String dimension, int numDocs,
      FieldSpec.DataType dataType, Dictionary dictionary, int numBitsPerValue, int cardinality) {
    _numDocs = numDocs;
    _dataType = dataType;
    _dictionary = dictionary;
    _forwardIndex = new FixedBitSingleValueReader(dataBuffer, numDocs, numBitsPerValue);

    _metadata = new DataSourceMetadata() {
      @Override
      public FieldSpec.DataType getDataType() {
        return _dataType;
      }

      @Override
      public boolean isSingleValue() {
        return true;
      }

      @Override
      public boolean isSorted() {
        return false;
      }

      @Override
      public int getNumDocs() {
        return _numDocs;
      }

      @Override
      public int getMaxNumMultiValues() {
        return 0;
      }

      @Override
      public boolean hasInvertedIndex() {
        return false;
      }

      @Override
      public boolean hasDictionary() {
        return true;
      }

      @Override
      public int getCardinality() {
        return cardinality;
      }
    };

    _operatorName = "StarTreeDimensionDataSource [" + dimension + "]";
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata() {
    return _metadata;
  }

  @Override
  public InvertedIndexReader getInvertedIndex() {
    return null;
  }

  @Override
  public Dictionary getDictionary() {
    return _dictionary;
  }

  @Override
  protected Block getNextBlock() {
    return new SingleValueBlock(_forwardIndex, _numDocs, _dataType, _dictionary);
  }

  @Override
  public String getOperatorName() {
    return _operatorName;
  }
}
