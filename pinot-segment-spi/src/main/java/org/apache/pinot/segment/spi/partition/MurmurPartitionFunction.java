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
package org.apache.pinot.segment.spi.partition;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.hash.MurmurHashFunctions;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Implementation of {@link PartitionFunction} which partitions based on 32 bit murmur hash
 */
public class MurmurPartitionFunction implements PartitionFunction {
  private static final String NAME = "Murmur";
  private static final String USE_RAW_BYTES_KEY = "useRawBytes";
  private final int _numPartitions;
  @Nullable
  private final Map<String, String> _functionConfig;
  private final boolean _useRawBytes;

  /**
   * Constructor for backward compatibility.
   * @param numPartitions Number of partitions.
   */
  public MurmurPartitionFunction(int numPartitions) {
    this(numPartitions, null);
  }

  /**
   * Constructor for the class.
   * @param numPartitions Number of partitions.
   * @param functionConfig to extract configurations for the partition function.
   */
  public MurmurPartitionFunction(int numPartitions, @Nullable Map<String, String> functionConfig) {
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0");
    _numPartitions = numPartitions;
    _functionConfig = functionConfig != null ? Collections.unmodifiableMap(functionConfig) : null;
    _useRawBytes = functionConfig != null && Boolean.parseBoolean(functionConfig.get(USE_RAW_BYTES_KEY));
  }

  @Override
  public int getPartition(String value) {
    byte[] bytes = _useRawBytes ? BytesUtils.toBytes(value) : value.getBytes(UTF_8);
    return (MurmurHashFunctions.murmurHash2(bytes) & Integer.MAX_VALUE) % _numPartitions;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getNumPartitions() {
    return _numPartitions;
  }

  @Nullable
  @Override
  public Map<String, String> getFunctionConfig() {
    return _functionConfig;
  }

  // Keep it for backward-compatibility, use getName() instead
  @Override
  public String toString() {
    return NAME;
  }
}
