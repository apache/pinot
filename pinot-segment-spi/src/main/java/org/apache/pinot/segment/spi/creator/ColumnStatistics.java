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
package org.apache.pinot.segment.spi.creator;

import java.io.Serializable;
import java.util.Set;
import org.apache.pinot.segment.spi.partition.PartitionFunction;


/**
 * An interface to read the column statistics from statistics collectors.
 */
public interface ColumnStatistics extends Serializable {
  /**
   * @return Minimum value of the column
   */
  Object getMinValue();

  /**
   * @return Maximum value of the column
   */
  Object getMaxValue();

  /**
   *
   * @return An array of elements that has the unique values for this column, sorted order.
   */
  Object getUniqueValuesSet();

  /**
   *
   * @return The number of unique values of this column.
   */
  int getCardinality();

  /**
   *
   * @return For variable length objects, returns the length of the shortest value. For others, returns -1.
   */
  int getLengthOfShortestElement();

  /**
   *
   * @return For variable length objects, returns the length of the longest value. For others, returns -1.
   */
  int getLengthOfLargestElement();

  default boolean isFixedLength() {
    return getLengthOfShortestElement() == getLengthOfLargestElement();
  }

  /**
   * Whether or not the data in this column is in ascending order.
   * @return true if the data is in ascending order.
   */
  boolean isSorted();

  /**
   * @return total number of entries
   */
  int getTotalNumberOfEntries();

  /**
   * @return For multi-valued columns, returns the max number of values in a single occurrence of the column,
   * otherwise 0.
   */
  int getMaxNumberOfMultiValues();

  /**
   * @return Returns if any of the values have nulls in the segments.
   */
  boolean hasNull();

  PartitionFunction getPartitionFunction();

  int getNumPartitions();

  Set<Integer> getPartitions();
}
