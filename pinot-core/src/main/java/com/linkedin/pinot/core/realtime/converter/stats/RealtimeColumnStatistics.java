/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.core.realtime.converter.stats;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.data.partition.PartitionFunction;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.operator.blocks.RealtimeSingleValueBlock;
import com.linkedin.pinot.core.realtime.impl.datasource.RealtimeColumnDataSource;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryReader;
import com.linkedin.pinot.core.segment.creator.ColumnStatistics;
import java.util.List;
import org.apache.commons.lang.math.IntRange;


/**
 * Column statistics for a column coming from an in-memory realtime segment.
 */
public class RealtimeColumnStatistics implements ColumnStatistics {
  private final RealtimeColumnDataSource _dataSource;
  private final int[] _sortedDocIdIterationOrder;
  private final MutableDictionaryReader _dictionaryReader;
  private final Block _block;

  public RealtimeColumnStatistics(RealtimeColumnDataSource dataSource, int[] sortedDocIdIterationOrder) {
    _dataSource = dataSource;
    _sortedDocIdIterationOrder = sortedDocIdIterationOrder;
    _dictionaryReader = dataSource.getDictionary();
    _block = dataSource.getNextBlock();
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
  public int getLengthOfLargestElement() {
    // Length of longest string
    int maximumStringLength = 0;

    // If this column is a string column, iterate over the dictionary to find the maximum length
    if (_dataSource.getDataSourceMetadata().getDataType() == FieldSpec.DataType.STRING) {
      final int length = _dictionaryReader.length();
      for (int i = 0; i < length; i++) {
        maximumStringLength = Math.max(_dictionaryReader.getStringValue(i).length(), maximumStringLength);
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
    SingleColumnSingleValueReader singleValueReader = ((RealtimeSingleValueBlock) _block).getReader();

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
      while(valIterator.hasNext()) {
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
    return null;
  }

  @Override
  public List<IntRange> getPartitionRanges() {
    return null;
  }
}
