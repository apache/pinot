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
import org.apache.kafka.common.utils.Utils;


/**
 * Implementation of {@link PartitionFunction} that mimics Kafka's
 * {@link org.apache.kafka.clients.producer.internals.DefaultPartitioner}.
 * However, this is not general purpose. It assumes that the partition key
 * was a string as used by the Tracking Producer. This is only a reference
 * implementation and users should create their own for their specific partitioner.
 *
 */
public class MurmurPartitionFunction implements PartitionFunction {
  private static final String NAME = "Murmur";
  private final int _divisor;

  /**
   * Constructor for the class.
   * @param args Arguments for the partition function.
   */
  public MurmurPartitionFunction(String[] args) {
    Preconditions.checkArgument(args.length == 1);
    _divisor = Integer.parseInt(args[0]);
    Preconditions.checkState(_divisor > 0, "Divisor for MurmurPartitionFunction cannot be <= zero.");
  }

  @Override
  public int getPartition(Object valueIn) {
    String value = (valueIn instanceof String) ? (String) valueIn : valueIn.toString();
    return (Utils.murmur2(((String) value).getBytes()) & 0x7fffffff) % _divisor;
  }

  @Override
  public String toString() {
    return NAME + PartitionFunctionFactory.PARTITION_FUNCTION_DELIMITER + _divisor;
  }
}
