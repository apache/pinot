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
package org.apache.pinot.broker.routing.segmentpruner;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionIdNormalizer;


/// A function that can never place a value, so every call returns [PartitionFunction#UNKNOWN_PARTITION].
///
/// Stands in for a real function facing a value outside its column's domain -- a decimal whose scale it
/// cannot honour, a literal that does not parse as the column's type. Those cases are hard to provoke
/// through a real function because the query layer normalizes literals first, so the sentinel is what
/// gets tested rather than the parsing that produces it.
///
/// Discovered by `PartitionFunctionFactory`'s classpath scan like any other implementation under
/// `org.apache.pinot`, which is what lets a test name it in segment metadata.
public class UnknownPartitionFunction implements PartitionFunction {
  public static final String NAME = "TestUnknownPartition";

  private final int _numPartitions;

  public UnknownPartitionFunction(int numPartitions, @Nullable Map<String, String> functionConfig) {
    _numPartitions = numPartitions;
  }

  @Override
  public int getPartition(String value) {
    return UNKNOWN_PARTITION;
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
    return null;
  }

  /// Never reached -- this function produces no ids to normalize -- but the contract requires one.
  @Override
  public PartitionIdNormalizer getPartitionIdNormalizer() {
    return PartitionIdNormalizer.NO_OP;
  }
}
