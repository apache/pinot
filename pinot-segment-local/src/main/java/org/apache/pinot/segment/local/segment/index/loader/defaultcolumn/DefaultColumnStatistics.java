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
package org.apache.pinot.segment.local.segment.index.loader.defaultcolumn;

import java.util.Set;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.partition.PartitionFunction;


public class DefaultColumnStatistics implements ColumnStatistics {

  private final Object _minValue;
  private final Object _maxValue;
  private final Object _uniqueValuesSet;
  private final int _cardinality = 1;
  private final int _lengthOfShortestElement = -1;
  private final int _lengthOfLargestElement = -1;
  private final boolean _isSorted;
  private final int _totalNumberOfEntries;
  private final int _maxNumberOfMultiValues;
  private final boolean _hasNull = false;

  public DefaultColumnStatistics(Object minValue, Object maxValue, Object uniqueValuesSet, boolean isSorted,
      int totalNumberOfEntries, int maxNumberOfMultiValues) {
    _minValue = minValue;
    _maxValue = maxValue;
    _uniqueValuesSet = uniqueValuesSet;
    _isSorted = isSorted;
    _totalNumberOfEntries = totalNumberOfEntries;
    _maxNumberOfMultiValues = maxNumberOfMultiValues;
  }

  @Override
  public Object getMinValue() {
    return _minValue;
  }

  @Override
  public Object getMaxValue() {
    return _maxValue;
  }

  @Override
  public Object getUniqueValuesSet() {
    return _uniqueValuesSet;
  }

  @Override
  public int getCardinality() {
    return _cardinality;
  }

  @Override
  public int getLengthOfShortestElement() {
    return _lengthOfShortestElement;
  }

  @Override
  public int getLengthOfLargestElement() {
    return _lengthOfLargestElement;
  }

  @Override
  public boolean isSorted() {
    return _isSorted;
  }

  @Override
  public int getTotalNumberOfEntries() {
    return _totalNumberOfEntries;
  }

  @Override
  public int getMaxNumberOfMultiValues() {
    return _maxNumberOfMultiValues;
  }

  @Override
  public boolean hasNull() {
    return _hasNull;
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
  public Set<Integer> getPartitions() {
    return null;
  }
}
