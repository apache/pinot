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
package org.apache.pinot.query.planner.validation;

import com.google.common.base.Preconditions;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;


/// Validates the logical types used as join keys.
///
/// <p>This stateless validator is shared by planning and execution so that mixed-version plans are rejected with the
/// same semantics and error messages regardless of where validation first occurs.
public final class RawVariantJoinKeyValidator {
  private static final String JOIN_KEY_ERROR =
      "Raw VARIANT values do not support JOIN keys; extract a typed path with variantGet first";
  private static final String ASOF_MATCH_KEY_ERROR =
      "Raw VARIANT values do not support ASOF JOIN match keys; extract a typed path with variantGet first";

  private RawVariantJoinKeyValidator() {
  }

  /// Validates equality/hash keys and, for ASOF joins, ordering keys.
  ///
  /// <p>The right schema can be absent for legacy SEMI and ANTI join plans whose output schema contains only left
  /// columns. In that case, the caller can validate only the left keys.
  public static void validate(JoinNode joinNode, DataSchema leftSchema, @Nullable DataSchema rightSchema) {
    validateEqualityAndHashing(joinNode.getLeftKeys(), leftSchema);
    if (rightSchema == null) {
      return;
    }
    validateEqualityAndHashing(joinNode.getRightKeys(), rightSchema);
    if (joinNode.getJoinStrategy() == JoinNode.JoinStrategy.ASOF) {
      validateAsofMatchKeys(joinNode, leftSchema, rightSchema);
    }
  }

  private static void validateEqualityAndHashing(List<Integer> keys, DataSchema schema) {
    for (int key : keys) {
      ColumnDataType dataType = schema.getColumnDataType(key);
      Preconditions.checkArgument(dataType != ColumnDataType.VARIANT, JOIN_KEY_ERROR);
    }
  }

  private static void validateAsofMatchKeys(JoinNode joinNode, DataSchema leftSchema, DataSchema rightSchema) {
    RexExpression.FunctionCall matchCondition = (RexExpression.FunctionCall) joinNode.getMatchCondition();
    List<RexExpression> matchKeys = matchCondition.getFunctionOperands();
    int leftMatchKey = ((RexExpression.InputRef) matchKeys.get(0)).getIndex();
    int rightMatchKey = ((RexExpression.InputRef) matchKeys.get(1)).getIndex() - leftSchema.size();
    Preconditions.checkArgument(leftSchema.getColumnDataType(leftMatchKey) != ColumnDataType.VARIANT
        && rightSchema.getColumnDataType(rightMatchKey) != ColumnDataType.VARIANT, ASOF_MATCH_KEY_ERROR);
  }
}
