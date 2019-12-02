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
package org.apache.pinot.core.data.partition;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.common.utils.StringUtil;


/**
 * Implementation of {@link PartitionFunction} which partitions based on 32 bit murmur hash
 */
public class MurmurPartitionFunction implements PartitionFunction {
  private static final String NAME = "Murmur";
  private final int _numPartitions;

  /**
   * Constructor for the class.
   * @param numPartitions Number of partitions.
   */
  public MurmurPartitionFunction(int numPartitions) {
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0");
    _numPartitions = numPartitions;
  }

  @Override
  public int getPartition(Object valueIn) {
    String value = (valueIn instanceof String) ? (String) valueIn : valueIn.toString();
    return (murmur2(StringUtil.encodeUtf8(value)) & 0x7fffffff) % _numPartitions;
  }

  @Override
  public int getNumPartitions() {
    return _numPartitions;
  }

  @Override
  public String toString() {
    return NAME;
  }

  /**
   * NOTE: This code has been copied over from org.apache.kafka.common.utils.Utils::murmur2
   *
   * Generates 32 bit murmur2 hash from byte array
   * @param data byte array to hash
   * @return 32 bit hash of the given array
   */
  @VisibleForTesting
  int murmur2(final byte[] data) {
    return HashUtil.murmur2(data);
  }
}
