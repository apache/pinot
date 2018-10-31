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
import java.nio.charset.Charset;


/**
 * Implementation of {@link PartitionFunction} that mimics Kafka's
 * {@link org.apache.kafka.clients.producer.internals.DefaultPartitioner}.
 *
 * **** ALERT ****
 * However, this is not general purpose. It assumes that the partition key
 * was a string as used by the Tracking Producer. This is only a reference
 * implementation and users should create their own for their specific partitioner.
 *
 */
public class MurmurPartitionFunction implements PartitionFunction {
  private static final String NAME = "Murmur";
  private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
  private final int _numPartitions;

  /**
   * Constructor for the class.
   * @param numPartitions Number of partitions.
   */
  public MurmurPartitionFunction(int numPartitions) {
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0, specified", numPartitions);
    _numPartitions = numPartitions;
  }

  @Override
  public int getPartition(Object valueIn) {
    String value = (valueIn instanceof String) ? (String) valueIn : valueIn.toString();
    return (Utils.murmur2((value).getBytes(UTF8_CHARSET)) & 0x7fffffff) % _numPartitions;
  }

  @Override
  public int getNumPartitions() {
    return _numPartitions;
  }

  @Override
  public String toString() {
    return NAME;
  }
}
