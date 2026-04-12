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
package org.apache.pinot.segment.spi.partition.pipeline;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.pinot.segment.spi.partition.PartitionFunction;


/**
 * {@link PartitionFunction} adapter for expression-mode partition pipelines.
 */
public class PartitionPipelineFunction implements PartitionFunction {
  public static final String NAME = "FunctionExpr";

  private final PartitionPipeline _pipeline;
  private final int _numPartitions;

  public PartitionPipelineFunction(PartitionPipeline pipeline, int numPartitions) {
    Preconditions.checkNotNull(pipeline, "Partition pipeline must be configured");
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0");
    Preconditions.checkArgument(pipeline.getOutputType().isIntegral(),
        "Partition pipeline must produce INT or LONG output, got: %s", pipeline.getOutputType());
    _pipeline = pipeline;
    _numPartitions = numPartitions;
  }

  public PartitionPipeline getPartitionPipeline() {
    return _pipeline;
  }

  @Override
  public int getPartition(String value) {
    PartitionValue partitionValue = _pipeline.evaluate(value);
    PartitionIntNormalizer intNormalizer = _pipeline.getIntNormalizer();
    Preconditions.checkState(intNormalizer != null, "Integral-output partition pipeline must have an INT normalizer");
    if (partitionValue.getType() == PartitionValueType.INT) {
      return intNormalizer.getPartitionId(partitionValue.getIntValue(), _numPartitions);
    }
    Preconditions.checkState(partitionValue.getType() == PartitionValueType.LONG,
        "Expected INT or LONG partition value but got: %s", partitionValue.getType());
    return intNormalizer.getPartitionId(partitionValue.getLongValue(), _numPartitions);
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
  @JsonIgnore(false)
  @JsonProperty("functionExpr")
  public String getFunctionExpr() {
    return _pipeline.getCanonicalFunctionExpr();
  }

  @Override
  @JsonIgnore(false)
  @JsonProperty("partitionIdNormalizer")
  public String getPartitionIdNormalizer() {
    PartitionIntNormalizer intNormalizer = _pipeline.getIntNormalizer();
    return intNormalizer != null ? intNormalizer.name() : null;
  }

  @Override
  public String toString() {
    return NAME;
  }
}
