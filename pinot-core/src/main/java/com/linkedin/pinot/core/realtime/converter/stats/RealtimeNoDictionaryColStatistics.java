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
package com.linkedin.pinot.core.realtime.converter.stats;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.data.partition.PartitionFunction;
import com.linkedin.pinot.core.segment.creator.ColumnStatistics;
import com.linkedin.pinot.core.segment.index.data.source.ColumnDataSource;
import java.util.List;
import org.apache.commons.lang.math.IntRange;

import static com.linkedin.pinot.core.common.Constants.*;

public class RealtimeNoDictionaryColStatistics implements ColumnStatistics {

  final BlockValSet _blockValSet;
  final int _numDocIds;
  final String _operatorName;

  public RealtimeNoDictionaryColStatistics(ColumnDataSource dataSource) {
    _operatorName = dataSource.getOperatorName();
    Block block = dataSource.nextBlock();
    _numDocIds = block.getMetadata().getEndDocId() + 1;
    _blockValSet = block.getBlockValueSet();
  }

  @Override
  public Object getMinValue() {
    throw new RuntimeException("Cannot get min value for no dictionary column " + _operatorName);
  }

  @Override
  public Object getMaxValue() {
    throw new RuntimeException("Cannot get max value for no dictionary column " + _operatorName);
  }

  @Override
  public Object getUniqueValuesSet() {
    return null;
  }

  @Override
  public int getCardinality() {
    return UNKNOWN_CARDINALITY;
  }

  @Override
  public int getLengthOfShortestElement() {
    return lengthOfDataType(); // Only fixed length data types supported.
  }

  @Override
  public int getLengthOfLargestElement() {
    return lengthOfDataType(); // Only fixed length data types supported.
  }

  @Override
  public boolean isSorted() {
    return false;
  }

  @Override
  public int getTotalNumberOfEntries() {
    return _numDocIds;
  }

  @Override
  public int getMaxNumberOfMultiValues() {
    return 1;
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
  public int getNumPartitions() {
    return 0;
  }

  @Override
  public List<IntRange> getPartitionRanges() {
    return null;
  }

  @Override
  public int getPartitionRangeWidth() {
    return 0;
  }

  private int lengthOfDataType() {
    switch (_blockValSet.getValueType()) {
      case INT:
        return Integer.BYTES;
      case LONG:
        return Long.BYTES;
      case FLOAT:
        return Float.BYTES;
      case DOUBLE:
        return Double.BYTES;
      default:
        throw new UnsupportedOperationException();
    }
  }
}
