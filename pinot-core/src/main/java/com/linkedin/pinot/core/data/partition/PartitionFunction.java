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
package com.linkedin.pinot.core.data.partition;

/**
 * Interface for partition function.
 */
public interface PartitionFunction {

  /**
   * Method to compute and return partition id for the given value.
   *
   * @param value Value for which to determine the partition id.
   * @return partition id for the value.
   */
  int getPartition(Object value);

  /**
   * Returns the total number of possible partitions.
   * @return Number of possible partitions.
   */
  int getNumPartitions();
}
