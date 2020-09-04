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

import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Partitioner which creates a partition value between [0-numPartitions) in a round-robin manner
 */
public class RoundRobinPartitioner implements Partitioner {
  private final int _numPartitions;
  private transient int _nextPartition = 0;

  public RoundRobinPartitioner(int numPartitions) {
    _numPartitions = numPartitions;
  }

  @Override
  public String getPartition(GenericRow genericRow) {
    int currentPartition = _nextPartition;
    _nextPartition = (_nextPartition + 1) % _numPartitions;
    return String.valueOf(currentPartition);
  }
}
