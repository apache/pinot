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

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.data.partition.PartitionFunction;
import com.linkedin.pinot.core.segment.creator.ColumnStatistics;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.data.source.ColumnDataSource;
import java.util.List;
import org.apache.commons.lang.math.IntRange;


public class RealtimeNoDictionaryColStatistics implements ColumnStatistics {

  final BlockValSet _blockValSet;
  final int _numDocIds;
  Object _minValue;
  Object _maxValue;

  public RealtimeNoDictionaryColStatistics(ColumnDataSource dataSource) {
    Block block = dataSource.nextBlock();
    _numDocIds = block.getMetadata().getEndDocId() + 1;
    _blockValSet = block.getBlockValueSet();
  }

  private void gatherStatistics() {
    final int rows[] = new int[_numDocIds];
    for (int i = 0; i < _numDocIds; i++) {
      rows[i] = i;
    }
    switch (_blockValSet.getValueType()) {
      case INT:
        computeIntMinMax(rows);
        break;
      case LONG:
        computeLongMinMax(rows);
        break;
      case FLOAT:
        computeFloatMinMax(rows);
        break;
      case DOUBLE:
        computeDoubleMinMax(rows);
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private void computeIntMinMax(int[] rows) {
    int values[] = new int[_numDocIds];
    _blockValSet.getIntValues(rows, 0, _numDocIds, values, 0);
    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;
    for (int i = 0; i < _numDocIds; i++) {
      if (values[i] < min) {
        min = values[i];
      }
      if (values[i] > max) {
        max = values[i];
      }
    }
    _minValue = Integer.valueOf(min);
    _maxValue = Integer.valueOf(max);
  }

  private void computeLongMinMax(int[] rows) {
    long values[] = new long[_numDocIds];
    _blockValSet.getLongValues(rows, 0, _numDocIds, values, 0);
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (int i = 0; i < _numDocIds; i++) {
      if (values[i] < min) {
        min = values[i];
      }
      if (values[i] > max) {
        max = values[i];
      }
    }
    _minValue = Long.valueOf(min);
    _maxValue = Long.valueOf(max);
  }

  private void computeFloatMinMax(int[] rows) {
    float values[] = new float[_numDocIds];
    _blockValSet.getFloatValues(rows, 0, _numDocIds, values, 0);
    float min = Float.MAX_VALUE;
    float max = Float.MIN_VALUE;
    for (int i = 0; i < _numDocIds; i++) {
      if (values[i] < min) {
        min = values[i];
      }
      if (values[i] > max) {
        max = values[i];
      }
    }
    _minValue = Float.valueOf(min);
    _maxValue = Float.valueOf(max);
  }

  private void computeDoubleMinMax(int[] rows) {
    double values[] = new double[_numDocIds];
    _blockValSet.getDoubleValues(rows, 0, _numDocIds, values, 0);
    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;
    for (int i = 0; i < _numDocIds; i++) {
      if (values[i] < min) {
        min = values[i];
      }
      if (values[i] > max) {
        max = values[i];
      }
    }
    _minValue = Double.valueOf(min);
    _maxValue = Double.valueOf(max);
  }

  @Override
  public Object getMinValue() {
    if (_minValue == null) {
      gatherStatistics();
    }
    return _minValue;
  }

  @Override
  public Object getMaxValue() {
    if (_maxValue == null) {
      gatherStatistics();
    }
    return _maxValue;
  }

  @Override
  public Object getUniqueValuesSet() {
    return null;
  }

  @Override
  public int getCardinality() {
    return Constants.UNKNOWN_CARDINALITY;
  }

  @Override
  public int getLengthOfLargestElement() {
    switch (_blockValSet.getValueType()) {
      case INT:
        return V1Constants.Numbers.INTEGER_SIZE;
      case LONG:
        return V1Constants.Numbers.LONG_SIZE;
      case FLOAT:
        return V1Constants.Numbers.FLOAT_SIZE;
      case DOUBLE:
        return V1Constants.Numbers.DOUBLE_SIZE;
    }
    throw new UnsupportedOperationException();
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
}
