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
package org.apache.pinot.core.segment.processing.partitioner;

import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Partitioner for GenericRow
 */
public interface Partitioner {

  /**
   * Computes a partition value for the given row
   */
  String getPartition(GenericRow genericRow);

  /**
   * Returns the column names used for partitioning,
   * This enables efficient columnar processing by reading only the required columns.
   * @return
   *  1. array of column names, or
   *  2. null if the partitioner doesn't support columnar processing, or
   *  3. an empty array if no columns are needed to derive the partition.
   */
  @Nullable
  String[] getPartitionColumns();

  /**
   * Computes a partition value from column values.
   * Should only be called if getPartitionColumns() returns a non-null array.
   *
   * @param columnValues Array of column values in same order as columns returned by getPartitionColumns()
   * @return The partition string
   */
  String getPartitionFromColumns(Object[] columnValues);
}
