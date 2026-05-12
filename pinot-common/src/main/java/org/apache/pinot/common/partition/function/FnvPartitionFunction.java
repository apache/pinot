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
package org.apache.pinot.common.partition.function;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionIdNormalizer;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.hash.FnvHashFunctions;

import static java.nio.charset.StandardCharsets.UTF_8;


/// Stateless and thread-safe [PartitionFunction] backed by configurable FNV variants. The
/// configured [PartitionIdNormalizer] (default [PartitionIdNormalizer#MASK]) is applied
/// to the raw FNV hash to derive the partition id.
public class FnvPartitionFunction implements PartitionFunction {
  private static final String NAME = "FNV";
  private static final String VARIANT_KEY = "variant";
  private static final String USE_RAW_BYTES_KEY = "useRawBytes";
  private static final FnvHashFunctions.Variant DEFAULT_VARIANT = FnvHashFunctions.Variant.FNV1A_32;
  private static final PartitionIdNormalizer DEFAULT_NORMALIZER = PartitionIdNormalizer.MASK;

  private final int _numPartitions;
  @Nullable
  private final Map<String, String> _functionConfig;
  private final FnvHashFunctions.Variant _variant;
  private final boolean _useRawBytes;
  private final PartitionIdNormalizer _normalizer;

  public FnvPartitionFunction(int numPartitions, @Nullable Map<String, String> functionConfig) {
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0");
    _numPartitions = numPartitions;
    _functionConfig = functionConfig != null ? Collections.unmodifiableMap(functionConfig) : null;

    FnvHashFunctions.Variant variant = DEFAULT_VARIANT;
    boolean useRawBytes = false;
    if (functionConfig != null) {
      String variantString = functionConfig.get(VARIANT_KEY);
      if (StringUtils.isNotBlank(variantString)) {
        variant = FnvHashFunctions.Variant.fromString(variantString);
      }
      useRawBytes = Boolean.parseBoolean(functionConfig.get(USE_RAW_BYTES_KEY));
    }
    _variant = variant;
    _useRawBytes = useRawBytes;
    _normalizer = PartitionFunctionConfigs.normalizer(functionConfig, DEFAULT_NORMALIZER);
  }

  @Override
  public int getPartition(String value) {
    byte[] bytes = _useRawBytes ? BytesUtils.toBytes(value) : value.getBytes(UTF_8);
    if (_variant.is64Bit()) {
      long hash = _variant == FnvHashFunctions.Variant.FNV1_64 ? FnvHashFunctions.fnv1Hash64(bytes)
          : FnvHashFunctions.fnv1aHash64(bytes);
      return _normalizer.getPartitionId(hash, _numPartitions);
    }

    int hash = _variant == FnvHashFunctions.Variant.FNV1_32 ? FnvHashFunctions.fnv1Hash32(bytes)
        : FnvHashFunctions.fnv1aHash32(bytes);
    return _normalizer.getPartitionId(hash, _numPartitions);
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

  @Override
  public PartitionIdNormalizer getPartitionIdNormalizer() {
    return _normalizer;
  }

  // Keep it for backward-compatibility, use getName() instead
  @Override
  public String toString() {
    return NAME;
  }
}
