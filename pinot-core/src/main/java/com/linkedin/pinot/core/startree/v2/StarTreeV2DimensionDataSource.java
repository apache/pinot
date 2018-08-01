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

package com.linkedin.pinot.core.startree.v2;

import java.io.IOException;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.operator.blocks.SingleValueBlock;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;


public class StarTreeV2DimensionDataSource extends DataSource {

  private int _numDocs;
  private int _maxNumMultiValues;
  private final boolean _isSorted;
  private final boolean _isSingleValue;
  private FieldSpec.DataType _dataType;
  private InvertedIndexReader _invertedIndex;

  private String _operatorName;
  private Dictionary _dictionary;
  private DataFileReader _forwardIndex;
  private DataSourceMetadata _metadata;

  public StarTreeV2DimensionDataSource(PinotDataBuffer buffer, String columnName, ImmutableSegment obj,
      ColumnMetadata columnMetadata, int numDocs, int bits) throws IOException {
    _operatorName = "ColumnDataSource [" + columnName + "]";
    _forwardIndex = new FixedBitSingleValueReader(buffer, numDocs, bits);

    _isSorted = false;
    _dictionary = obj.getDictionary(columnName);
    _numDocs = numDocs;
    _invertedIndex = null;
    _dataType = columnMetadata.getDataType();
    _isSingleValue = columnMetadata.isSingleValue();
    _maxNumMultiValues = columnMetadata.getMaxNumberOfMultiValues();

    _metadata = new DataSourceMetadata() {
      @Override
      public FieldSpec.DataType getDataType() {
        return _dataType;
      }

      @Override
      public boolean isSingleValue() {
        return _isSingleValue;
      }

      @Override
      public boolean isSorted() {
        return _isSorted;
      }

      @Override
      public int getNumDocs() {
        return _numDocs;
      }

      @Override
      public int getMaxNumMultiValues() {
        return _maxNumMultiValues;
      }

      @Override
      public boolean hasInvertedIndex() {
        return _invertedIndex != null;
      }

      @Override
      public boolean hasDictionary() {
        return _dictionary != null;
      }
    };
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
    return new SingleValueBlock((SingleColumnSingleValueReader<? super ReaderContext>) _forwardIndex, _numDocs,
        _dataType, _dictionary);
  }

  @Override
  public String getOperatorName() {
    return _operatorName;
  }

  public DataFileReader getForwardIndex() {
    return _forwardIndex;
  }
}
