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
package org.apache.pinot.core.segment.index.datasource;

import javax.annotation.Nullable;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.common.DataSourceMetadata;
import org.apache.pinot.core.io.reader.DataFileReader;
import org.apache.pinot.core.io.reader.SingleColumnMultiValueReader;
import org.apache.pinot.core.io.reader.SingleColumnSingleValueReader;
import org.apache.pinot.core.operator.blocks.MultiValueBlock;
import org.apache.pinot.core.operator.blocks.SingleValueBlock;
import org.apache.pinot.core.segment.index.readers.BloomFilterReader;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.core.segment.index.readers.NullValueVectorReader;
import org.apache.pinot.spi.data.FieldSpec;


public abstract class BaseDataSource extends DataSource {
  private final DataSourceMetadata _dataSourceMetadata;
  private final DataFileReader _forwardIndex;
  private final Dictionary _dictionary;
  private final InvertedIndexReader _invertedIndex;
  private final InvertedIndexReader _rangeIndex;
  private final BloomFilterReader _bloomFilter;
  private final NullValueVectorReader _nullValueVector;
  private final String _operatorName;

  public BaseDataSource(DataSourceMetadata dataSourceMetadata, DataFileReader forwardIndex,
      @Nullable Dictionary dictionary, @Nullable InvertedIndexReader invertedIndex,
      @Nullable InvertedIndexReader rangeIndex, @Nullable BloomFilterReader bloomFilter,
      @Nullable NullValueVectorReader nullValueVector, String operatorName) {
    _dataSourceMetadata = dataSourceMetadata;
    _forwardIndex = forwardIndex;
    _dictionary = dictionary;
    _invertedIndex = invertedIndex;
    _rangeIndex = rangeIndex;
    _bloomFilter = bloomFilter;
    _nullValueVector = nullValueVector;
    _operatorName = operatorName;
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata() {
    return _dataSourceMetadata;
  }

  @Override
  public DataFileReader getForwardIndex() {
    return _forwardIndex;
  }

  @Nullable
  @Override
  public Dictionary getDictionary() {
    return _dictionary;
  }

  @Nullable
  @Override
  public InvertedIndexReader getInvertedIndex() {
    return _invertedIndex;
  }

  @Nullable
  @Override
  public InvertedIndexReader getRangeIndex() {
    return _rangeIndex;
  }

  @Nullable
  @Override
  public BloomFilterReader getBloomFilter() {
    return _bloomFilter;
  }

  @Nullable
  @Override
  public NullValueVectorReader getNullValueVector() {
    return _nullValueVector;
  }

  @Override
  protected Block getNextBlock() {
    FieldSpec.DataType dataType = _dataSourceMetadata.getDataType();
    if (_dataSourceMetadata.isSingleValue()) {
      return new SingleValueBlock((SingleColumnSingleValueReader) _forwardIndex, _dataSourceMetadata.getNumDocs(),
          dataType, _dictionary);
    } else {
      return new MultiValueBlock((SingleColumnMultiValueReader) _forwardIndex, _dataSourceMetadata.getNumDocs(),
          _dataSourceMetadata.getMaxNumValuesPerMVEntry(), dataType, _dictionary);
    }
  }

  @Override
  public String getOperatorName() {
    return _operatorName;
  }
}
