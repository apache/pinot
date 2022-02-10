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
package org.apache.pinot.spark.jobs.preprocess;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.spark.Partitioner;


public class SparkDataPreprocessingPartitioner extends Partitioner {
  private final String _partitionColumn;
  private final int _numPartitions;
  private final PartitionFunction _partitionFunction;
  private final String _partitionColumnDefaultNullValue;
  private final AtomicInteger _counter = new AtomicInteger(0);

  public SparkDataPreprocessingPartitioner(String partitionColumn, int numPartitions,
      PartitionFunction partitionFunction, String partitionColumnDefaultNullValue) {
    _partitionColumn = partitionColumn;
    _numPartitions = numPartitions;
    _partitionFunction = partitionFunction;
    _partitionColumnDefaultNullValue = partitionColumnDefaultNullValue;
  }

  @Override
  public int numPartitions() {
    return _numPartitions;
  }

  @Override
  public int getPartition(Object key) {
    SparkDataPreprocessingJobKey jobKey = (SparkDataPreprocessingJobKey) key;
    return (int) jobKey.getPartitionId();
  }

  public int generatePartitionId(Object key) {
    if (_partitionColumn == null) {
      // Need to distribute evenly for data with the default partition key value.
      // We may want to partition and sort on a non-primary key.
      return Math.abs(_counter.getAndIncrement()) % _numPartitions;
    }
    Object keyToPartition = _partitionColumnDefaultNullValue;
    if (key != null) {
      keyToPartition = key;
    }
    return _partitionFunction.getPartition(keyToPartition);
  }
}
