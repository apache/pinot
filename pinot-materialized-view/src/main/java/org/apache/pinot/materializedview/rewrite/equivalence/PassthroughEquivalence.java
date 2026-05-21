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
package org.apache.pinot.materializedview.rewrite.equivalence;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.utils.request.RequestUtils;


/// Equivalence for aggregation functions whose MV expression is identical to
/// the user expression (after stripping aliases), but whose re-aggregation
/// function may differ from the original.
///
/// This covers two categories:
///
///   - **Distributive** — the re-aggregation function is the same as the
///       user function: `SUM(col) -> SUM(materialized_view_sum_col)`,
///       `MIN(col) -> MIN(materialized_view_min_col)`,
///       `MAX(col) -> MAX(materialized_view_max_col)`
///   - **Algebraic (simple transformation)** — the re-aggregation function
///       differs: `COUNT(col) -> SUM(materialized_view_count_col)`,
///       `COUNT(*) -> SUM(materialized_view_count_star_col)`
///
///
/// The [#matches] method requires the MV to store the same aggregation
/// function as the user query (e.g. MV has `SUM(revenue)` for a user
/// query with `SUM(revenue)`).
///
/// Trailing literal parameters injected by broker overrides (e.g. HLL
/// `log2m`) are preserved in the rewritten expression.
///
/// This class is stateless and thread-safe.
public class PassthroughEquivalence implements AggregationEquivalence {

  private final String _userFunctionName;
  private final String _reAggFunctionName;

  /// @param userFunctionName the user-side aggregation function name (uppercase)
  /// @param reAggFunctionName the function to apply during re-aggregation on
  ///                          the MV column (uppercase)
  public PassthroughEquivalence(String userFunctionName, String reAggFunctionName) {
    _userFunctionName = userFunctionName;
    _reAggFunctionName = reAggFunctionName;
  }

  @Override
  public boolean isSplitSafe() {
    // Split-safe only when re-aggregation uses the same function as the user query
    // (distributive: SUM->SUM, MIN->MIN, MAX->MAX). COUNT->SUM is NOT split-safe
    // because the base side returns COUNT intermediates but the MV side returns
    // SUM intermediates, causing a schema mismatch in the broker reducer.
    return _userFunctionName.equalsIgnoreCase(_reAggFunctionName);
  }

  @Override
  public boolean matches(String userFunctionName, String materializedViewFunctionName) {
    return _userFunctionName.equalsIgnoreCase(userFunctionName)
        && _userFunctionName.equalsIgnoreCase(materializedViewFunctionName);
  }

  @Override
  public boolean supportsMaterializedViewFunction(String materializedViewFunctionName) {
    return _userFunctionName.equalsIgnoreCase(materializedViewFunctionName);
  }

  @Nullable
  @Override
  public Expression rewrite(Expression userAggExpression, String materializedViewColumnName) {
    List<Expression> trailingLiterals = extractTrailingLiterals(userAggExpression);
    List<Expression> operands = new ArrayList<>(1 + trailingLiterals.size());
    operands.add(RequestUtils.getIdentifierExpression(materializedViewColumnName));
    operands.addAll(trailingLiterals);
    return RequestUtils.getFunctionExpression(_reAggFunctionName.toLowerCase(),
        operands.toArray(new Expression[0]));
  }
}
