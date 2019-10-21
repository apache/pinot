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
package org.apache.pinot.core.segment.index.data.source;

import com.google.common.base.Preconditions;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.Constants;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.common.DataSourceMetadata;
import org.apache.pinot.core.io.reader.DataFileReader;
import org.apache.pinot.core.io.reader.SingleColumnMultiValueReader;
import org.apache.pinot.core.io.reader.SingleColumnSingleValueReader;
import org.apache.pinot.core.io.reader.impl.v1.SortedIndexReader;
import org.apache.pinot.core.operator.blocks.MultiValueBlock;
import org.apache.pinot.core.operator.blocks.SingleValueBlock;
import org.apache.pinot.core.realtime.impl.dictionary.BaseMutableDictionary;
import org.apache.pinot.core.segment.index.ColumnMetadata;
import org.apache.pinot.core.segment.index.column.ColumnIndexContainer;
import org.apache.pinot.core.segment.index.readers.BloomFilterReader;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;


public final class ColumnDataSource extends DataSource {
  private final String _operatorName;
  private final FieldSpec.DataType _dataType;
  private final boolean _isSingleValue;
  private final boolean _isSorted;
  private final int _numDocs;
  private final int _maxNumMultiValues;
  private final DataFileReader _forwardIndex;
  private final InvertedIndexReader _invertedIndex;
  private final Dictionary _dictionary;
  private final BloomFilterReader _bloomFilter;
  private final int _cardinality;
  private final DataSourceMetadata _metadata;

  /**
   * For OFFLINE segment.
   */
  public ColumnDataSource(ColumnIndexContainer indexContainer, ColumnMetadata metadata) {
    this(metadata.getColumnName(), metadata.getDataType(), metadata.isSingleValue(), metadata.isSorted(),
        metadata.getTotalDocs(), metadata.getMaxNumberOfMultiValues(), indexContainer.getForwardIndex(),
        indexContainer.getInvertedIndex(), indexContainer.getDictionary(), indexContainer.getBloomFilter(),
        metadata.getCardinality());
  }

  /**
   * For REALTIME segment.
   */
  public ColumnDataSource(FieldSpec fieldSpec, int numDocs, int maxNumMultiValues, DataFileReader forwardIndex,
      InvertedIndexReader invertedIndex, BaseMutableDictionary dictionary, BloomFilterReader bloomFilter) {
    this(fieldSpec.getName(), fieldSpec.getDataType(), fieldSpec.isSingleValueField(), false, numDocs,
        maxNumMultiValues, forwardIndex, invertedIndex, dictionary, bloomFilter, Constants.UNKNOWN_CARDINALITY);
  }

  private ColumnDataSource(String columnName, FieldSpec.DataType dataType, boolean isSingleValue, boolean isSorted,
      int numDocs, int maxNumMultiValues, DataFileReader forwardIndex, InvertedIndexReader invertedIndex,
      Dictionary dictionary, BloomFilterReader bloomFilterReader, int cardinality) {
    // Sanity check
    if (isSingleValue) {
      Preconditions.checkState(forwardIndex instanceof SingleColumnSingleValueReader);
    } else {
      Preconditions.checkState(forwardIndex instanceof SingleColumnMultiValueReader);
    }
    if (dictionary != null) {
      // Dictionary-based index
      if (isSorted) {
        Preconditions.checkState(invertedIndex instanceof SortedIndexReader);
      }
    } else {
      // Raw index
      Preconditions.checkState(invertedIndex == null);
    }

    _operatorName = "ColumnDataSource [" + columnName + "]";
    _dataType = dataType;
    _isSingleValue = isSingleValue;
    _isSorted = isSorted;
    _numDocs = numDocs;
    _maxNumMultiValues = maxNumMultiValues;
    _forwardIndex = forwardIndex;
    _invertedIndex = invertedIndex;
    _dictionary = dictionary;
    _bloomFilter = bloomFilterReader;
    _cardinality = cardinality;

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

      @Override
      public int getCardinality() {
        return _cardinality;
      }
    };
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata() {
    return _metadata;
  }

  @Override
  public Dictionary getDictionary() {
    return _dictionary;
  }

  @Override
  public InvertedIndexReader getInvertedIndex() {
    return _invertedIndex;
  }

  @Override
  public BloomFilterReader getBloomFilter() {
    return _bloomFilter;
  }

  @Override
  protected Block getNextBlock() {
    if (_isSingleValue) {
      return new SingleValueBlock((SingleColumnSingleValueReader) _forwardIndex, _numDocs, _dataType, _dictionary);
    } else {
      return new MultiValueBlock((SingleColumnMultiValueReader) _forwardIndex, _numDocs, _maxNumMultiValues, _dataType,
          _dictionary);
    }
  }

  @Override
  public String getOperatorName() {
    return _operatorName;
  }
}
