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
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.function.FunctionEvaluator;


/// Immutable compiled pipeline for one raw partition column, backed by a [FunctionEvaluator].
///
/// The pipeline always treats the input as a string. For BYTES-typed partition columns the value is hex-encoded at
/// every call site (ingestion, broker pruner) so the partition expression sees a consistent string representation —
/// the user's expression operates on the hex string, not the raw bytes.
public final class PartitionPipeline implements FunctionEvaluator {
  private final String _rawColumn;
  private final String _canonicalFunctionExpr;
  private final FunctionEvaluator _evaluator;
  private final List<String> _arguments;

  PartitionPipeline(String rawColumn, String canonicalFunctionExpr, FunctionEvaluator evaluator) {
    Preconditions.checkNotNull(rawColumn, "Raw column must be configured");
    Preconditions.checkNotNull(canonicalFunctionExpr, "Canonical function expression must be configured");
    Preconditions.checkNotNull(evaluator, "Function evaluator must be configured");
    _rawColumn = rawColumn;
    _canonicalFunctionExpr = canonicalFunctionExpr;
    _evaluator = evaluator;
    _arguments = Collections.singletonList(rawColumn);
  }

  public String getRawColumn() {
    return _rawColumn;
  }

  public String getCanonicalFunctionExpr() {
    return _canonicalFunctionExpr;
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
    return _evaluator.evaluate(new Object[]{FieldSpec.getStringValue(inputValue)});
  }
}
