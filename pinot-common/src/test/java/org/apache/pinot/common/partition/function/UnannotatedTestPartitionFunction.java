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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionIdNormalizer;


/// Test fixture: a [PartitionFunction] without `@PartitionFunctionType`. Exercises the registry's
/// fallback path where the canonical name is probed via [#getName()].
public class UnannotatedTestPartitionFunction implements PartitionFunction {
  public static final String NAME = "UnannotatedTestPartitionFunction-name";
  private final int _numPartitions;

  public UnannotatedTestPartitionFunction(int numPartitions, @Nullable Map<String, String> functionConfig) {
    _numPartitions = Math.max(numPartitions, 1);
  }

  @Override
  public int getPartition(String value) {
    return PartitionIdNormalizer.POSITIVE_MODULO.getPartitionId(value.hashCode(), _numPartitions);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getNumPartitions() {
    return _numPartitions;
  }

  @Override
  public PartitionIdNormalizer getPartitionIdNormalizer() {
    return PartitionIdNormalizer.POSITIVE_MODULO;
  }
}
