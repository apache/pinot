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
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.utils.hash.MurmurHashFunctions;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Implementation of {@link PartitionFunction} which partitions based on 32 bit murmur3 hash
 */
public class Murmur3PartitionFunction implements PartitionFunction {
  private static final String NAME = "Murmur3";
  private static final String SEED_KEY = "seed";
  private static final String VARIANT_KEY = "variant";
  private final int _numPartitions;
  private final int _seed;
  private final boolean _useX64;

  /**
   * Constructor for the class.
   * @param numPartitions Number of partitions.
   * @param functionConfig to extract configurations for the partition function.
   */
  public Murmur3PartitionFunction(int numPartitions, Map<String, String> functionConfig) {
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0");
    _numPartitions = numPartitions;

    int seed = 0;
    boolean useX64 = false;
    if (functionConfig != null) {
      String seedString = functionConfig.get(SEED_KEY);
      if (StringUtils.isNotEmpty(seedString)) {
        seed = Integer.parseInt(seedString);
      }
      String variantString = functionConfig.get(VARIANT_KEY);
      if (StringUtils.isNotEmpty(variantString)) {
        if (variantString.equals("x64_32")) {
          useX64 = true;
        } else {
          Preconditions.checkArgument(variantString.equals("x86_32"),
              "Murmur3 variant must be either x86_32 or x64_32");
        }
      }
    }
    _seed = seed;
    _useX64 = useX64;
  }

  @Override
  public int getPartition(String value) {
    int hash = _useX64 ? MurmurHashFunctions.murmurHash3X64Bit32(value, _seed)
        : MurmurHashFunctions.murmurHash3X86Bit32(value.getBytes(UTF_8), _seed);
    return (hash & Integer.MAX_VALUE) % _numPartitions;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getNumPartitions() {
    return _numPartitions;
  }

  // Keep it for backward-compatibility, use getName() instead
  @Override
  public String toString() {
    return NAME;
  }
}
