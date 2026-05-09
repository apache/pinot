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
import org.apache.pinot.segment.spi.partition.PartitionIdNormalizer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.function.FunctionEvaluator;
import org.apache.pinot.spi.utils.BytesUtils;


/// Immutable compiled pipeline for one raw partition column, backed by a [FunctionEvaluator].
///
/// The underlying [FunctionEvaluator] is responsible for correct type coercion. For partition expressions,
/// the evaluator uses UTF-8 encoding when converting `String` values to `byte[]` parameters, ensuring
/// that hash functions (`md5`, `murmur2`, `fnv1a_32`, etc.) operate on raw string bytes rather
/// than a hex-decoded representation.
public final class PartitionPipeline implements FunctionEvaluator {
  private final String _rawColumn;
  private final boolean _isBytesInput;
  private final String _canonicalFunctionExpr;
  @Nullable
  private final PartitionIdNormalizer _intNormalizer;
  private final FunctionEvaluator _evaluator;
  private final List<String> _arguments;

  PartitionPipeline(String rawColumn, boolean isBytesInput, String canonicalFunctionExpr,
      @Nullable PartitionIdNormalizer intNormalizer, FunctionEvaluator evaluator) {
    Preconditions.checkNotNull(rawColumn, "Raw column must be configured");
    Preconditions.checkNotNull(canonicalFunctionExpr, "Canonical function expression must be configured");
    Preconditions.checkNotNull(evaluator, "Function evaluator must be configured");
    _rawColumn = rawColumn;
    _isBytesInput = isBytesInput;
    _canonicalFunctionExpr = canonicalFunctionExpr;
    _intNormalizer = intNormalizer;
    _evaluator = evaluator;
    _arguments = Collections.singletonList(rawColumn);
  }

  public String getRawColumn() {
    return _rawColumn;
  }

  public boolean isBytesInput() {
    return _isBytesInput;
  }

  public String getCanonicalFunctionExpr() {
    return _canonicalFunctionExpr;
  }

  @Nullable
  public PartitionIdNormalizer getIntNormalizer() {
    return _intNormalizer;
  }

  @Override
  public List<String> getArguments() {
    return _arguments;
  }

  @Override
  public Object evaluate(GenericRow genericRow) {
    Object inputValue = genericRow.getValue(_rawColumn);
    if (inputValue == null) {
      return null;
    }
    if (_isBytesInput) {
      // For BYTES-input pipelines pass raw byte[] directly; String values are hex-encoded representations.
      if (inputValue instanceof byte[]) {
        return _evaluator.evaluate(new Object[]{inputValue});
      }
      // Hex-encoded string representation of bytes (e.g. from broker routing) — decode before passing.
      return _evaluator.evaluate(new Object[]{BytesUtils.toBytes(
          FieldSpec.getStringValue(inputValue))});
    }
    // For STRING-input pipelines pass the string value as-is. The underlying PartitionFunctionEvaluator converts
    // String to byte[] using UTF-8 encoding when a function parameter requires byte[].
    return _evaluator.evaluate(new Object[]{FieldSpec.getStringValue(inputValue)});
  }

  @Override
  public Object evaluate(Object[] values) {
    Preconditions.checkArgument(values.length == 1,
        "Partition pipeline for column '%s' expects exactly 1 positional argument, got: %s", _rawColumn,
        values.length);
    Object inputValue = values[0];
    if (inputValue == null) {
      return null;
    }
    if (_isBytesInput) {
      if (inputValue instanceof byte[]) {
        return _evaluator.evaluate(new Object[]{inputValue});
      }
      return _evaluator.evaluate(new Object[]{BytesUtils.toBytes(
          FieldSpec.getStringValue(inputValue))});
    }
    return _evaluator.evaluate(new Object[]{FieldSpec.getStringValue(inputValue)});
  }
}
