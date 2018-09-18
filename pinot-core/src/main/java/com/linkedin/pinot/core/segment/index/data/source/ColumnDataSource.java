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
package com.linkedin.pinot.core.segment.index.data.source;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.SortedIndexReader;
import com.linkedin.pinot.core.operator.blocks.MultiValueBlock;
import com.linkedin.pinot.core.operator.blocks.SingleValueBlock;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionary;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.column.ColumnIndexContainer;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;


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
  private final int _cardinality;
  private final DataSourceMetadata _metadata;

  /**
   * For OFFLINE segment.
   */
  public ColumnDataSource(ColumnIndexContainer indexContainer, ColumnMetadata metadata) {
    this(metadata.getColumnName(), metadata.getDataType(), metadata.isSingleValue(), metadata.isSorted(),
        metadata.getTotalDocs(), metadata.getMaxNumberOfMultiValues(), indexContainer.getForwardIndex(),
        indexContainer.getInvertedIndex(), indexContainer.getDictionary(), metadata.getCardinality());
  }

  /**
   * For REALTIME segment.
   */
  public ColumnDataSource(FieldSpec fieldSpec, int numDocs, int maxNumMultiValues, DataFileReader forwardIndex,
      InvertedIndexReader invertedIndex, MutableDictionary dictionary) {
    this(fieldSpec.getName(), fieldSpec.getDataType(), fieldSpec.isSingleValueField(), false, numDocs,
        maxNumMultiValues, forwardIndex, invertedIndex, dictionary, Constants.UNKNOWN_CARDINALITY);
  }

  private ColumnDataSource(String columnName, FieldSpec.DataType dataType, boolean isSingleValue, boolean isSorted,
      int numDocs, int maxNumMultiValues, DataFileReader forwardIndex, InvertedIndexReader invertedIndex,
      Dictionary dictionary, int cardinality) {
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
  public InvertedIndexReader getInvertedIndex() {
    return _invertedIndex;
  }

  @Override
  public Dictionary getDictionary() {
    return _dictionary;
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
