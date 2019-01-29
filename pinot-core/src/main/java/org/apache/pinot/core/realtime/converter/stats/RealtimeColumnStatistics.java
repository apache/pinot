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

import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.common.config.ColumnPartitionConfig;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.BlockMultiValIterator;
import org.apache.pinot.core.data.partition.PartitionFunction;
import org.apache.pinot.core.data.partition.PartitionFunctionFactory;
import org.apache.pinot.core.io.reader.SingleColumnSingleValueReader;
import org.apache.pinot.core.operator.blocks.SingleValueBlock;
import org.apache.pinot.core.realtime.impl.dictionary.MutableDictionary;
import org.apache.pinot.core.segment.creator.ColumnStatistics;
import org.apache.pinot.core.segment.index.data.source.ColumnDataSource;


/**
 * Column statistics for a column coming from an in-memory realtime segment.
 */
public class RealtimeColumnStatistics implements ColumnStatistics {

  private final ColumnDataSource _dataSource;
  private final int[] _sortedDocIdIterationOrder;
  private final MutableDictionary _dictionaryReader;
  private final Block _block;
  private PartitionFunction _partitionFunction;
  private int _numPartitions;
  private Set<Integer> _partitions;

  public RealtimeColumnStatistics(ColumnDataSource dataSource, int[] sortedDocIdIterationOrder,
      ColumnPartitionConfig columnPartitionConfig) {
    _dataSource = dataSource;
    _sortedDocIdIterationOrder = sortedDocIdIterationOrder;
    _dictionaryReader = (MutableDictionary) dataSource.getDictionary();
    _block = dataSource.nextBlock();
    if (columnPartitionConfig != null) {
      String functionName = columnPartitionConfig.getFunctionName();
      _numPartitions = columnPartitionConfig.getNumPartitions();
      _partitionFunction =
          (functionName != null) ? PartitionFunctionFactory.getPartitionFunction(functionName, _numPartitions) : null;
      if (_partitionFunction != null) {
        // Iterate over the dictionary to check the partitioning
        _partitions = new HashSet<>();
        int length = _dictionaryReader.length();
        for (int i = 0; i < length; i++) {
          _partitions.add(_partitionFunction.getPartition(_dictionaryReader.get(i)));
        }
      }
    }
  }

  @Override
  public Object getMinValue() {
    return _dictionaryReader.getMinVal();
  }

  @Override
  public Object getMaxValue() {
    return _dictionaryReader.getMaxVal();
  }

  @Override
  public Object getUniqueValuesSet() {
    return _dictionaryReader.getSortedValues();
  }

  @Override
  public int getCardinality() {
    return _dictionaryReader.length();
  }

  @Override
  public int getLengthOfShortestElement() {
    // Length of longest string
    int minStringLength = Integer.MAX_VALUE;

    // If this column is a string/bytes column, iterate over the dictionary to find the maximum length
    FieldSpec.DataType dataType = _dataSource.getDataSourceMetadata().getDataType();
    final int length = _dictionaryReader.length();

    if (dataType.equals(FieldSpec.DataType.STRING)) {
      for (int i = 0; i < length; i++) {
        minStringLength = Math.min(_dictionaryReader.getStringValue(i).length(), minStringLength);
      }
    } else if (dataType.equals(FieldSpec.DataType.BYTES)) {
      for (int i = 0; i < length; i++) {
        minStringLength = Math.min(_dictionaryReader.getBytesValue(i).length, minStringLength);
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
    final int length = _dictionaryReader.length();

    if (dataType.equals(FieldSpec.DataType.STRING)) {
      for (int i = 0; i < length; i++) {
        maximumStringLength = Math.max(_dictionaryReader.getStringValue(i).length(), maximumStringLength);
      }
    } else if (dataType.equals(FieldSpec.DataType.BYTES)) {
      for (int i = 0; i < length; i++) {
        maximumStringLength = Math.max(_dictionaryReader.getBytesValue(i).length, maximumStringLength);
      }
    }

    return maximumStringLength;
  }

  @Override
  public boolean isSorted() {
    // Multivalue columns can't be in sorted order
    if (!_block.getMetadata().isSingleValue()) {
      return false;
    }

    // If this is a single value, then by definition the data is sorted
    final int blockLength = _block.getMetadata().getLength();
    if (blockLength <= 1 || getCardinality() <= 1) {
      return true;
    }

    // Iterate over all data to figure out whether or not it's in sorted order
    SingleColumnSingleValueReader singleValueReader = ((SingleValueBlock) _block).getReader();

    int docIdIndex = _sortedDocIdIterationOrder != null ? _sortedDocIdIterationOrder[0] : 0;
    int dictionaryId = singleValueReader.getInt(docIdIndex);
    Comparable previousValue = (Comparable) _dictionaryReader.get(dictionaryId);
    for (int i = 1; i < blockLength; i++) {
      docIdIndex = _sortedDocIdIterationOrder != null ? _sortedDocIdIterationOrder[i] : i;
      dictionaryId = singleValueReader.getInt(docIdIndex);
      Comparable currentValue = (Comparable) _dictionaryReader.get(dictionaryId);
      // If previousValue is greater than currentValue
      if (0 < previousValue.compareTo(currentValue)) {
        return false;
      } else {
        previousValue = currentValue;
      }
    }

    return true;
  }

  @Override
  public int getTotalNumberOfEntries() {
    // Number of multivalue entries
    int multivalueEntryCount = 0;

    // If this column is a multivalue column, iterate over all data to find the total number of multivalue entries (this
    // information doesn't seem to be exposed via an API)
    if (!_block.getMetadata().isSingleValue()) {
      int[] dictionaryIds = new int[getMaxNumberOfMultiValues()];

      BlockMultiValIterator valIterator = (BlockMultiValIterator) _block.getBlockValueSet().iterator();
      while (valIterator.hasNext()) {
        multivalueEntryCount += valIterator.nextIntVal(dictionaryIds);
      }
    }

    return multivalueEntryCount;
  }

  @Override
  public int getMaxNumberOfMultiValues() {
    return _block.getMetadata().getMaxNumberOfMultiValues();
  }

  @Override
  public boolean hasNull() {
    return false;
  }

  @Override
  public PartitionFunction getPartitionFunction() {
    return _partitionFunction;
  }

  @Override
  public int getNumPartitions() {
    return _numPartitions;
  }

  @Override
  public Set<Integer> getPartitions() {
    return _partitions;
  }
}
