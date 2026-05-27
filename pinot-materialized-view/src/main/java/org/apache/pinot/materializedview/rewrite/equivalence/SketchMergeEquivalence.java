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


/// Equivalence for sketch-based aggregation functions where the user query
/// requests a computed result (e.g. cardinality) but the MV must store the
/// raw sketch bytes for merging.
///
/// The MV stores a "raw" variant of the sketch function (e.g.
/// `DISTINCTCOUNTRAWHLL(col)`) which serializes the sketch object.
/// At re-aggregation time, the result function (e.g. `DISTINCTCOUNTHLL`)
/// is applied on the MV column; it automatically deserializes and merges the
/// sketch bytes, then returns the computed result.
///
/// Supported sketch families:
///
///   - `DISTINCTCOUNTHLL` / `DISTINCTCOUNTRAWHLL`
///   - `DISTINCTCOUNTHLLPLUS` / `DISTINCTCOUNTRAWHLLPLUS`
///   - `DISTINCTCOUNTTHETASKETCH` / `DISTINCTCOUNTRAWTHETASKETCH`
///
///
/// Trailing literal parameters injected by broker overrides (e.g. HLL
/// `log2m`) are preserved in the rewritten expression.
///
/// This class is stateless and thread-safe.
public class SketchMergeEquivalence implements AggregationEquivalence {

  private final String _userFunctionName;
  private final String _materializedViewFunctionName;
  private final String _reAggFunctionName;

  /// @param userFunctionName  the user-side aggregation function name
  ///                          (uppercase, e.g. "DISTINCTCOUNTHLL")
  /// @param materializedViewFunctionName    the function the MV must use to store the raw
  ///                          sketch (uppercase, e.g. "DISTINCTCOUNTRAWHLL")
  /// @param reAggFunctionName the function to apply on the MV column during
  ///                          re-aggregation (uppercase, e.g. "DISTINCTCOUNTHLL")
  public SketchMergeEquivalence(String userFunctionName, String materializedViewFunctionName,
      String reAggFunctionName) {
    _userFunctionName = userFunctionName;
    _materializedViewFunctionName = materializedViewFunctionName;
    _reAggFunctionName = reAggFunctionName;
  }

  @Override
  public boolean matches(String userFunctionName, String materializedViewFunctionName) {
    return _userFunctionName.equalsIgnoreCase(userFunctionName)
        && _materializedViewFunctionName.equalsIgnoreCase(materializedViewFunctionName);
  }

  @Override
  public boolean supportsMaterializedViewFunction(String materializedViewFunctionName) {
    return _materializedViewFunctionName.equalsIgnoreCase(materializedViewFunctionName);
  }

  @Override
  public boolean isSplitSafe() {
    return true;
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
