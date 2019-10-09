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
package org.apache.pinot.core.startree.v2.store;

import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.Constants;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.common.DataSourceMetadata;
import org.apache.pinot.core.io.reader.impl.v1.BaseChunkSingleValueReader;
import org.apache.pinot.core.io.reader.impl.v1.FixedByteChunkSingleValueReader;
import org.apache.pinot.core.io.reader.impl.v1.VarByteChunkSingleValueReader;
import org.apache.pinot.core.operator.blocks.SingleValueBlock;
import org.apache.pinot.core.segment.index.readers.BloomFilterReader;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.core.segment.index.readers.TextIndexReader;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


/**
 * The {@code StarTreeMetricDataSource} class represents the data source for a metric (function-column pair) in
 * star-tree.
 */
public class StarTreeMetricDataSource extends DataSource {
  private final int _numDocs;
  private final DataType _dataType;
  private final BaseChunkSingleValueReader _forwardIndex;
  private final DataSourceMetadata _metadata;
  private final String _operatorName;

  public StarTreeMetricDataSource(PinotDataBuffer dataBuffer, String metric, int numDocs, DataType dataType) {
    _numDocs = numDocs;
    _dataType = dataType;
    if (dataType == DataType.BYTES) {
      _forwardIndex = new VarByteChunkSingleValueReader(dataBuffer);
    } else {
      _forwardIndex = new FixedByteChunkSingleValueReader(dataBuffer);
    }

    _metadata = new DataSourceMetadata() {
      @Override
      public DataType getDataType() {
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
        return false;
      }

      @Override
      public int getCardinality() {
        return Constants.UNKNOWN_CARDINALITY;
      }
    };

    _operatorName = "StarTreeMetricDataSource [" + metric + "]";
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata() {
    return _metadata;
  }

  @Override
  public Dictionary getDictionary() {
    return null;
  }

  @Override
  public InvertedIndexReader getInvertedIndex() {
    return null;
  }

  @Override
  public TextIndexReader getTextIndex() {
    return null;
  }

  @Override
  public BloomFilterReader getBloomFilter() {
    return null;
  }

  @Override
  protected Block getNextBlock() {
    return new SingleValueBlock(_forwardIndex, _numDocs, _dataType, null);
  }

  @Override
  public String getOperatorName() {
    return _operatorName;
  }
}
