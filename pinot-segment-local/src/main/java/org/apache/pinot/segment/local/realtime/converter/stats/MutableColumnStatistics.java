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

import java.util.Set;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.MutableForwardIndex;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;


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

  public MutableColumnStatistics(DataSource dataSource, int[] sortedDocIdIterationOrder) {
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
    // Length of longest string
    int minStringLength = Integer.MAX_VALUE;

    // If this column is a string/bytes column, iterate over the dictionary to find the maximum length
    FieldSpec.DataType dataType = _dataSource.getDataSourceMetadata().getDataType();
    int length = _dictionary.length();

    if (dataType.equals(FieldSpec.DataType.STRING)) {
      for (int i = 0; i < length; i++) {
        minStringLength = Math.min(_dictionary.getStringValue(i).length(), minStringLength);
      }
    } else if (dataType.equals(FieldSpec.DataType.BYTES)) {
      for (int i = 0; i < length; i++) {
        minStringLength = Math.min(_dictionary.getBytesValue(i).length, minStringLength);
      }
    }

    return minStringLength;
  }

  @Override
  public int getLengthOfLargestElement() {
    // Length of longest string
    int maximumStringLength = 0;

    // If this column is a string/bytes column, iterate over the dictionary to find the maximum length
    FieldSpec.DataType dataType = _dataSource.getDataSourceMetadata().getDataType();
    int length = _dictionary.length();

    if (dataType.equals(FieldSpec.DataType.STRING)) {
      for (int i = 0; i < length; i++) {
        maximumStringLength = Math.max(_dictionary.getStringValue(i).length(), maximumStringLength);
      }
    } else if (dataType.equals(FieldSpec.DataType.BYTES)) {
      for (int i = 0; i < length; i++) {
        maximumStringLength = Math.max(_dictionary.getBytesValue(i).length, maximumStringLength);
      }
    }

    return maximumStringLength;
  }

  @Override
  public long getTotalLengthOfAllElements() {
    return -1;
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
  public boolean hasNull() {
    return false;
  }

  @Override
  public PartitionFunction getPartitionFunction() {
    return _dataSource.getDataSourceMetadata().getPartitionFunction();
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
  public Set<Integer> getPartitions() {
    return _dataSource.getDataSourceMetadata().getPartitions();
  }
}
