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

package com.linkedin.pinot.core.startreeV2;

import java.io.IOException;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.operator.blocks.SingleValueBlock;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import com.linkedin.pinot.core.io.reader.impl.v1.VarByteChunkSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedByteChunkSingleValueReader;


public class StarTreeV2AggfunColumnPairDataSource extends DataSource {

  private int _numDocs;
  private boolean _isSingleValue;
  private FieldSpec.DataType _dataType;

  private String _operatorName;
  private DataFileReader _forwardIndex;
  private DataSourceMetadata _metadata;

  public StarTreeV2AggfunColumnPairDataSource(PinotDataBuffer buffer, String columnName, int numDocs, FieldSpec.DataType dataType) throws IOException {
    _operatorName = "ColumnDataSource [" + columnName + "]";

    if (dataType.equals(FieldSpec.DataType.BYTES)) {
      _forwardIndex = new VarByteChunkSingleValueReader(buffer);
    } else {
      _forwardIndex = new FixedByteChunkSingleValueReader(buffer);
    }
    _numDocs = numDocs;
    _isSingleValue = false;
    _dataType = dataType;

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
        return false;
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
    return null;
  }

  @Override
  protected Block getNextBlock() {
    return new SingleValueBlock((SingleColumnSingleValueReader<? super ReaderContext>) _forwardIndex, _numDocs,
        _dataType, null);
  }

  @Override
  public String getOperatorName() {
    return _operatorName;
  }

  public DataFileReader getForwardIndex() {
    return _forwardIndex;
  }
}
