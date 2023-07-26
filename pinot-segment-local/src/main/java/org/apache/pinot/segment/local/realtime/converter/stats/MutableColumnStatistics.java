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
package org.apache.pinot.segment.local.realtime.converter.stats;

import com.google.common.base.Preconditions;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;


/**
 * Column statistics for a column coming from an in-memory realtime segment.
 *
 * TODO: Gather more info on the fly to avoid scanning the segment
 */
public class MutableColumnStatistics implements ColumnStatistics {
  private final DataSource _dataSource;
  private final int[] _sortedDocIdIterationOrder;

  // NOTE: For new added columns during the ingestion, this will be constant value dictionary instead of mutable
  //       dictionary.
  private final Dictionary _dictionary;

  private int _minElementLength = -1;
  private int _maxElementLength = -1;

  public MutableColumnStatistics(DataSource dataSource, @Nullable int[] sortedDocIdIterationOrder) {
    _dataSource = dataSource;
    _sortedDocIdIterationOrder = sortedDocIdIterationOrder;
    _dictionary = dataSource.getDictionary();
  }

  @Override
  public Object getMinValue() {
    return _dictionary.getMinVal();
  }

  @Override
  public Object getMaxValue() {
    return _dictionary.getMaxVal();
  }

  @Override
  public Object getUniqueValuesSet() {
    return _dictionary.getSortedValues();
  }

  @Override
  public int getCardinality() {
    return _dictionary.length();
  }

  @Override
  public int getLengthOfShortestElement() {
    collectElementLengthIfNeeded();
    return _minElementLength;
  }

  @Override
  public int getLengthOfLargestElement() {
    collectElementLengthIfNeeded();
    return _maxElementLength;
  }

  private void collectElementLengthIfNeeded() {
    if (_minElementLength >= 0) {
      return;
    }

    DataType storedType = _dictionary.getValueType();
    if (storedType.isFixedWidth()) {
      _minElementLength = storedType.size();
      _maxElementLength = storedType.size();
      return;
    }

    // If the stored type is not fixed width, iterate over the dictionary to find the min/max element length
    _minElementLength = Integer.MAX_VALUE;
    _maxElementLength = 0;
    int length = _dictionary.length();
    switch (storedType) {
      case BIG_DECIMAL:
        for (int i = 0; i < length; i++) {
          int elementLength = BigDecimalUtils.byteSize(_dictionary.getBigDecimalValue(i));
          _minElementLength = Math.min(_minElementLength, elementLength);
          _maxElementLength = Math.max(_maxElementLength, elementLength);
        }
        break;
      case STRING:
        for (int i = 0; i < length; i++) {
          int elementLength = _dictionary.getStringValue(i).getBytes(StandardCharsets.UTF_8).length;
          _minElementLength = Math.min(_minElementLength, elementLength);
          _maxElementLength = Math.max(_maxElementLength, elementLength);
        }
        break;
      case BYTES:
        for (int i = 0; i < length; i++) {
          int elementLength = _dictionary.getBytesValue(i).length;
          _minElementLength = Math.min(_minElementLength, elementLength);
          _maxElementLength = Math.max(_maxElementLength, elementLength);
        }
        break;
      default:
        throw new IllegalStateException("Unsupported stored type: " + storedType);
    }
  }

  @Override
  public boolean isSorted() {
    DataSourceMetadata dataSourceMetadata = _dataSource.getDataSourceMetadata();

    // Multi-valued column cannot be sorted
    if (!dataSourceMetadata.isSingleValue()) {
      return false;
    }

    // If there is only one distinct value, then it is sorted
    if (getCardinality() == 1) {
      return true;
    }

    // Iterate over all data to figure out whether or not it's in sorted order
    MutableForwardIndex mutableForwardIndex = (MutableForwardIndex) _dataSource.getForwardIndex();
    Preconditions.checkState(mutableForwardIndex != null,
        String.format("Forward index should not be null for column: %s", dataSourceMetadata.getFieldSpec().getName()));
    int numDocs = dataSourceMetadata.getNumDocs();
    // Iterate with the sorted order if provided
    if (_sortedDocIdIterationOrder != null) {
      int previousDictId = mutableForwardIndex.getDictId(_sortedDocIdIterationOrder[0]);
      for (int i = 1; i < numDocs; i++) {
        int currentDictId = mutableForwardIndex.getDictId(_sortedDocIdIterationOrder[i]);
        if (_dictionary.compare(previousDictId, currentDictId) > 0) {
          return false;
        }
        previousDictId = currentDictId;
      }
    } else {
      int previousDictId = mutableForwardIndex.getDictId(0);
      for (int i = 1; i < numDocs; i++) {
        int currentDictId = mutableForwardIndex.getDictId(i);
        if (_dictionary.compare(previousDictId, currentDictId) > 0) {
          return false;
        }
        previousDictId = currentDictId;
      }
    }

    return true;
  }

  @Override
  public int getTotalNumberOfEntries() {
    return _dataSource.getDataSourceMetadata().getNumValues();
  }

  @Override
  public int getMaxNumberOfMultiValues() {
    return _dataSource.getDataSourceMetadata().getMaxNumValuesPerMVEntry();
  }

  @Override
  public PartitionFunction getPartitionFunction() {
    return _dataSource.getDataSourceMetadata().getPartitionFunction();
  }

  @Override
  public int getMaxRowLengthInBytes() {
    return _dataSource.getDataSourceMetadata().getMaxRowLengthInBytes();
  }

  @Override
  public int getNumPartitions() {
    PartitionFunction partitionFunction = _dataSource.getDataSourceMetadata().getPartitionFunction();
    if (partitionFunction != null) {
      return partitionFunction.getNumPartitions();
    } else {
      return 0;
    }
  }

  @Override
  public Map<String, String> getPartitionFunctionConfig() {
    PartitionFunction partitionFunction = _dataSource.getDataSourceMetadata().getPartitionFunction();
    return partitionFunction != null ? partitionFunction.getFunctionConfig() : null;
  }

  @Override
  public Set<Integer> getPartitions() {
    return _dataSource.getDataSourceMetadata().getPartitions();
  }
}
