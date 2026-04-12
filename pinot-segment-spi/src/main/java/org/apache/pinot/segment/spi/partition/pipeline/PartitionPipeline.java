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
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.function.ExecutableFunctionEvaluator;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Immutable compiled pipeline for one raw partition column.
 */
public final class PartitionPipeline extends ExecutableFunctionEvaluator {
  private final String _rawColumn;
  private final PartitionValueType _inputType;
  private final PartitionValueType _outputType;
  private final String _canonicalFunctionExpr;
  @Nullable
  private final PartitionIntNormalizer _intNormalizer;
  private final List<PartitionStep> _steps;

  public PartitionPipeline(String rawColumn, PartitionValueType inputType, PartitionValueType outputType,
      String canonicalFunctionExpr, @Nullable PartitionIntNormalizer intNormalizer, List<PartitionStep> steps,
      ExecutableNode rootNode) {
    super(rootNode, Collections.singletonList(rawColumn), canonicalFunctionExpr);
    Preconditions.checkNotNull(rawColumn, "Raw column must be configured");
    Preconditions.checkNotNull(inputType, "Input type must be configured");
    Preconditions.checkNotNull(outputType, "Output type must be configured");
    Preconditions.checkNotNull(canonicalFunctionExpr, "Canonical function expression must be configured");
    Preconditions.checkNotNull(steps, "Pipeline steps must be configured");
    Preconditions.checkNotNull(rootNode, "Runtime evaluator root node must be configured");
    Preconditions.checkArgument(!outputType.isIntegral() || intNormalizer != null,
        "Integral-output pipelines must configure an INT normalizer");
    _rawColumn = rawColumn;
    _inputType = inputType;
    _outputType = outputType;
    _canonicalFunctionExpr = canonicalFunctionExpr;
    _intNormalizer = intNormalizer;
    _steps = Collections.unmodifiableList(steps);
  }

  public String getRawColumn() {
    return _rawColumn;
  }

  @Override
  public List<String> getArguments() {
    return Collections.singletonList(_rawColumn);
  }

  public PartitionValueType getInputType() {
    return _inputType;
  }

  public PartitionValueType getOutputType() {
    return _outputType;
  }

  public String getCanonicalFunctionExpr() {
    return _canonicalFunctionExpr;
  }

  @Nullable
  public PartitionIntNormalizer getIntNormalizer() {
    return _intNormalizer;
  }

  public List<PartitionStep> getSteps() {
    return _steps;
  }

  @Override
  public Object evaluate(GenericRow genericRow) {
    Object inputValue = genericRow.getValue(_rawColumn);
    return inputValue != null ? super.evaluate(genericRow) : null;
  }

  @Override
  public Object evaluate(Object[] values) {
    Preconditions.checkArgument(values.length == 1,
        "Partition pipeline for column '%s' expects exactly 1 positional argument, got: %s", _rawColumn,
        values.length);
    return values[0] != null ? super.evaluate(values) : null;
  }

  public PartitionValue evaluate(String rawValue) {
    Preconditions.checkState(_inputType == PartitionValueType.STRING,
        "evaluate(String) is only supported for STRING-input pipelines");
    return PartitionValue.fromObject(super.evaluate(new Object[]{rawValue}));
  }

  public PartitionValue evaluate(PartitionValue input) {
    Preconditions.checkArgument(input.getType() == _inputType,
        "Pipeline for column '%s' expects %s input but got %s", _rawColumn, _inputType, input.getType());
    return PartitionValue.fromObject(super.evaluate(new Object[]{input.toObject()}));
  }
}
