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

import com.google.common.base.Preconditions;


/**
 * One compiled pipeline step with a fixed input/output type contract.
 */
public final class PartitionStep {
  @FunctionalInterface
  interface Evaluator {
    PartitionValue apply(PartitionValue input);
  }

  private final String _name;
  private final PartitionValueType _inputType;
  private final PartitionValueType _outputType;
  private final Evaluator _evaluator;

  PartitionStep(String name, PartitionValueType inputType, PartitionValueType outputType, Evaluator evaluator) {
    _name = name;
    _inputType = inputType;
    _outputType = outputType;
    _evaluator = evaluator;
  }

  public String getName() {
    return _name;
  }

  public PartitionValueType getInputType() {
    return _inputType;
  }

  public PartitionValueType getOutputType() {
    return _outputType;
  }

  public PartitionValue evaluate(PartitionValue input) {
    Preconditions.checkArgument(input.getType() == _inputType,
        "Step '%s' expects %s input but got %s", _name, _inputType, input.getType());
    PartitionValue output = _evaluator.apply(input);
    Preconditions.checkState(output.getType() == _outputType,
        "Step '%s' produced %s output but expected %s", _name, output.getType(), _outputType);
    return output;
  }
}
