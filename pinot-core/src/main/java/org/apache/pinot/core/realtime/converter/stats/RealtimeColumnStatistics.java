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
package org.apache.pinot.core.realtime.converter.stats;

import java.util.Set;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.BlockMetadata;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.data.partition.PartitionFunction;
import org.apache.pinot.core.io.reader.SingleColumnSingleValueReader;
import org.apache.pinot.core.operator.blocks.SingleValueBlock;
import org.apache.pinot.core.realtime.impl.dictionary.BaseMutableDictionary;
import org.apache.pinot.core.segment.creator.ColumnStatistics;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Column statistics for a column coming from an in-memory realtime segment.
 *
 * TODO: Gather more info on the fly to avoid scanning the segment
 */
public class RealtimeColumnStatistics implements ColumnStatistics {
  private final DataSource _dataSource;
  private final int[] _sortedDocIdIterationOrder;
  private final BaseMutableDictionary _mutableDictionary;

  public RealtimeColumnStatistics(DataSource dataSource, int[] sortedDocIdIterationOrder) {
    _dataSource = dataSource;
    _sortedDocIdIterationOrder = sortedDocIdIterationOrder;
    _mutableDictionary = (BaseMutableDictionary) dataSource.getDictionary();
  }

  @Override
  public Object getMinValue() {
    return _mutableDictionary.getMinVal();
  }

  @Override
  public Object getMaxValue() {
    return _mutableDictionary.getMaxVal();
  }

  @Override
  public Object getUniqueValuesSet() {
    return _mutableDictionary.getSortedValues();
  }

  @Override
  public int getCardinality() {
    return _mutableDictionary.length();
  }

  @Override
  public int getLengthOfShortestElement() {
    // Length of longest string
    int minStringLength = Integer.MAX_VALUE;

    // If this column is a string/bytes column, iterate over the dictionary to find the maximum length
    FieldSpec.DataType dataType = _dataSource.getDataSourceMetadata().getDataType();
    int length = _mutableDictionary.length();

    if (dataType.equals(FieldSpec.DataType.STRING)) {
      for (int i = 0; i < length; i++) {
        minStringLength = Math.min(_mutableDictionary.getStringValue(i).length(), minStringLength);
      }
    } else if (dataType.equals(FieldSpec.DataType.BYTES)) {
      for (int i = 0; i < length; i++) {
        minStringLength = Math.min(_mutableDictionary.getBytesValue(i).length, minStringLength);
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
    int length = _mutableDictionary.length();

    if (dataType.equals(FieldSpec.DataType.STRING)) {
      for (int i = 0; i < length; i++) {
        maximumStringLength = Math.max(_mutableDictionary.getStringValue(i).length(), maximumStringLength);
      }
    } else if (dataType.equals(FieldSpec.DataType.BYTES)) {
      for (int i = 0; i < length; i++) {
        maximumStringLength = Math.max(_mutableDictionary.getBytesValue(i).length, maximumStringLength);
      }
    }

    return maximumStringLength;
  }

  @Override
  public boolean isSorted() {
    Block block = _dataSource.nextBlock();
    BlockMetadata blockMetadata = block.getMetadata();

    // Multi-valued column cannot be sorted
    if (!blockMetadata.isSingleValue()) {
      return false;
    }

    // If there is only one distinct value, then it is sorted
    if (getCardinality() == 1) {
      return true;
    }

    // Iterate over all data to figure out whether or not it's in sorted order
    SingleColumnSingleValueReader singleValueReader = ((SingleValueBlock) block).getReader();

    int numDocs = blockMetadata.getLength();
    // Iterate with the sorted order if provided
    if (_sortedDocIdIterationOrder != null) {
      int previousDictId = singleValueReader.getInt(_sortedDocIdIterationOrder[0]);
      for (int i = 1; i < numDocs; i++) {
        int currentDictId = singleValueReader.getInt(_sortedDocIdIterationOrder[i]);
        if (_mutableDictionary.compare(previousDictId, currentDictId) > 0) {
          return false;
        }
        previousDictId = currentDictId;
      }
    } else {
      int previousDictId = singleValueReader.getInt(0);
      for (int i = 1; i < numDocs; i++) {
        int currentDictId = singleValueReader.getInt(i);
        if (_mutableDictionary.compare(previousDictId, currentDictId) > 0) {
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
