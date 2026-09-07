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
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.utils.CommonConstants;


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
///   - `DISTINCTCOUNTCPCSKETCH` / `DISTINCTCOUNTRAWCPCSKETCH`
///   - `DISTINCTCOUNTTUPLESKETCH`, `SUMVALUESINTEGERSUMTUPLESKETCH`,
///     `AVGVALUEINTEGERSUMTUPLESKETCH` / `DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH`
///
/// Each raw variant is also registered user-side, so a query wanting the merged sketch itself can
/// be served from an MV storing it.
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

  @Override
  public boolean operandsCompatible(@Nullable List<Expression> userOperands,
      @Nullable List<Expression> materializedViewOperands) {
    Family family = family();
    if (family == null) {
      return true;
    }
    if (family == Family.THETA
        && (!isThetaSimpleUnion(userOperands) || !isThetaSimpleUnion(materializedViewOperands))) {
      return false;
    }
    return effectiveNominalEntries(family, userOperands) <= effectiveNominalEntries(family, materializedViewOperands);
  }

  /// A theta sketch query with filter predicates and a post-aggregation (set) expression cannot be
  /// served by a collapsed MV sketch. This mirrors the aggregation function's own threshold, which
  /// treats fewer than 4 arguments (column, params, filter(s), post-aggregation) as a simple union.
  private static boolean isThetaSimpleUnion(@Nullable List<Expression> operands) {
    return operands == null || operands.size() < THETA_POST_AGGREGATION_MIN_ARGUMENTS;
  }

  @Nullable
  private Family family() {
    if (_userFunctionName.contains("CPC")) {
      return Family.CPC;
    }
    if (_userFunctionName.contains("THETA")) {
      return Family.THETA;
    }
    if (_userFunctionName.contains("TUPLE")) {
      return Family.TUPLE;
    }
    return null;
  }

  private static long effectiveNominalEntries(Family family, @Nullable List<Expression> operands) {
    Literal param = literalAt(operands, 1);
    switch (family) {
      case CPC:
        if (param == null) {
          return 1L << CommonConstants.Helix.DEFAULT_CPC_SKETCH_LGK;
        }
        if (param.isSetStringValue()) {
          return nominalEntriesFromString(param.getStringValue(), 1L << CommonConstants.Helix.DEFAULT_CPC_SKETCH_LGK);
        }
        return 1L << numeric(param, CommonConstants.Helix.DEFAULT_CPC_SKETCH_LGK);
      case THETA:
        if (param == null) {
          return DEFAULT_THETA_NOMINAL_ENTRIES;
        }
        return param.isSetStringValue()
            ? nominalEntriesFromString(param.getStringValue(), DEFAULT_THETA_NOMINAL_ENTRIES)
            : numeric(param, DEFAULT_THETA_NOMINAL_ENTRIES);
      case TUPLE:
      default:
        long tupleDefault = 1L << CommonConstants.Helix.DEFAULT_TUPLE_SKETCH_LGK;
        if (param == null) {
          return tupleDefault;
        }
        return param.isSetStringValue()
            ? nominalEntriesFromString(param.getStringValue(), tupleDefault)
            : numeric(param, (int) tupleDefault);
    }
  }

  @Nullable
  private static Literal literalAt(@Nullable List<Expression> operands, int index) {
    if (operands == null || operands.size() <= index) {
      return null;
    }
    Expression op = operands.get(index);
    return op.getType() == ExpressionType.LITERAL ? op.getLiteral() : null;
  }

  private static long nominalEntriesFromString(String params, long defaultValue) {
    for (String pair : params.split(";")) {
      String[] keyValue = pair.split("=", 2);
      if (keyValue.length == 2 && keyValue[0].trim().equalsIgnoreCase(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES)) {
        try {
          return Long.parseLong(keyValue[1].trim());
        } catch (NumberFormatException e) {
          return defaultValue;
        }
      }
    }
    return defaultValue;
  }

  private static int numeric(Literal literal, int defaultValue) {
    if (literal.isSetIntValue()) {
      return literal.getIntValue();
    }
    if (literal.isSetLongValue()) {
      return (int) literal.getLongValue();
    }
    if (literal.isSetDoubleValue()) {
      return (int) literal.getDoubleValue();
    }
    return defaultValue;
  }

  private enum Family {
    CPC, THETA, TUPLE
  }

  /// The theta aggregation-function default (`ThetaUtil.DEFAULT_NOMINAL_ENTRIES`), which is what an
  /// MV stores. This is intentionally NOT `CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES`
  /// (16384), which is the star-tree ingestion default.
  private static final int DEFAULT_THETA_NOMINAL_ENTRIES = 1 << 12;

  private static final int THETA_POST_AGGREGATION_MIN_ARGUMENTS = 4;
}
