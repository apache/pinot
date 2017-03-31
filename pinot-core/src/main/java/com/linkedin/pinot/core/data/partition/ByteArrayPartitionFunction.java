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
package com.linkedin.pinot.core.data.partition;

import com.google.common.base.Preconditions;
import kafka.producer.ByteArrayPartitioner;


/**
 * Implementation of {@link Byte array partitioner}
 * {@link org.apache.kafka.producer.ByteArrayPartitioner}.
 *
 */
public class ByteArrayPartitionFunction implements PartitionFunction {
  private static final String NAME = "ByteArray";
  private final int _divisor;
  public ByteArrayPartitioner _byteArrayPartitioner;

  /**
   * Constructor for the class.
   * @param args Arguments for the partition function.
   */
  public ByteArrayPartitionFunction(String[] args) {
    Preconditions.checkArgument(args.length == 1);
    _divisor = Integer.parseInt(args[0]);
    Preconditions.checkState(_divisor > 0, "Divisor for ByteArrayPartitionFunction cannot be <= zero.");
    _byteArrayPartitioner = new ByteArrayPartitioner(null);
  }

  @Override
  public int getPartition(Object valueIn) {
    return _byteArrayPartitioner.partition(valueIn.toString().getBytes(), _divisor);
  }

  @Override
  public String toString() {
    return NAME + PartitionFunctionFactory.PARTITION_FUNCTION_DELIMITER + _divisor;
  }
}

