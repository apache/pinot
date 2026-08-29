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
package org.apache.pinot.query.runtime.operator.match;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.utils.TypeUtils;
import org.apache.pinot.spi.exception.QueryErrorCode;


/// A leaf of a MEASURES or DEFINE expression whose value depends on the match rather than on a single row: a row
/// pattern navigation, `CLASSIFIER()`, `MATCH_NUMBER()`, or a single variable aggregate.
///
/// [MatchExpression] replaces every such leaf with a slot in a synthetic row, so that everything above the
/// leaves - comparisons, boolean connectives, arithmetic, scalar functions - is evaluated by Pinot's ordinary
/// [TransformOperand] machinery instead of a second expression interpreter.
///
/// Implementations are stateless with respect to the match: all match state is passed in through the
/// [MatchTape], so one instance is reused across every match of every partition.
public interface MatchTerm {

  /// The type this term reports to the enclosing expression. Values returned by [#evaluate] are in the
  /// corresponding [stored][ColumnDataType#getStoredType()] representation, exactly like the values of a real
  /// input row.
  ColumnDataType getResultType();

  @Nullable
  Object evaluate(MatchTape tape);

  /// A row pattern navigation: an optional logical step (`FIRST` / `LAST`) that designates a row of the
  /// match, followed by an optional physical step (`PREV` / `NEXT`) that moves a fixed number of rows
  /// relative to it, and finally a column read.
  ///
  /// Both steps are needed because SQL:2016 nests them, e.g. `PREV(LAST(A.price), 2)` designates the last row
  /// mapped to `A` and then moves two rows back. The logical step is bounded by the match; the physical step is
  /// bounded only by the partition, so it may legally read a row outside the match. Either step falling off its bound
  /// yields `null`, as the standard requires.
  final class Navigation implements MatchTerm {
    private final int _symbolOrdinal;
    private final boolean _fromEnd;
    private final int _logicalOffset;
    private final int _physicalDelta;
    private final int _columnIndex;
    private final ColumnDataType _resultType;
    private final ColumnDataType _storedType;
    private final boolean _requiresConversion;

    /// @param symbolOrdinal pattern variable to navigate, or
    ///        [org.apache.pinot.query.planner.logical.RexExpression.PatternFieldRef#UNIVERSAL_SYMBOL_ORDINAL]
    ///        for an unqualified column reference, which navigates the rows of the whole match
    /// @param fromEnd `true` for `LAST`, `false` for `FIRST`
    /// @param logicalOffset how many rows back from the end (or forward from the start) of the designated variable
    /// @param physicalDelta rows to move in the partition afterwards; negative for `PREV`, positive for
    ///        `NEXT`, zero when there is no physical step
    /// @param columnIndex column to read, as an index into the input row of the MATCH_RECOGNIZE node
    /// @param sourceType type of that input column; values already use its stored representation
    public Navigation(int symbolOrdinal, boolean fromEnd, int logicalOffset, int physicalDelta, int columnIndex,
        ColumnDataType sourceType, ColumnDataType resultType) {
      _symbolOrdinal = symbolOrdinal;
      _fromEnd = fromEnd;
      _logicalOffset = logicalOffset;
      _physicalDelta = physicalDelta;
      _columnIndex = columnIndex;
      _resultType = resultType;
      _storedType = resultType.getStoredType();
      _requiresConversion = sourceType.getStoredType() != _storedType;
    }

    @Override
    public ColumnDataType getResultType() {
      return _resultType;
    }

    @Nullable
    @Override
    public Object evaluate(MatchTape tape) {
      int rowIndex = _fromEnd ? tape.lastRow(_symbolOrdinal, _logicalOffset)
          : tape.firstRow(_symbolOrdinal, _logicalOffset);
      if (rowIndex == MatchTape.NO_ROW) {
        return null;
      }
      long physicalRowIndex = (long) rowIndex + _physicalDelta;
      List<Object[]> rows = tape.getPartitionRows();
      if (physicalRowIndex < 0 || physicalRowIndex >= rows.size()) {
        return null;
      }
      Object value = rows.get((int) physicalRowIndex)[_columnIndex];
      // The declared type of the navigation may be wider than the column's (e.g. BIG_DECIMAL for a DOUBLE column),
      // and the enclosing operand compares against that declared type.
      return value != null && _requiresConversion ? TypeUtils.convert(value, _storedType) : value;
    }
  }

  /// `CLASSIFIER()`: the name of the pattern variable the designated row is mapped to. With ONE ROW PER MATCH
  /// the designated row is the last row of the match, which is also the current row while a DEFINE predicate is being
  /// evaluated.
  final class Classifier implements MatchTerm {
    public static final Classifier INSTANCE = new Classifier();

    private Classifier() {
    }

    @Override
    public ColumnDataType getResultType() {
      return ColumnDataType.STRING;
    }

    @Nullable
    @Override
    public Object evaluate(MatchTape tape) {
      return tape.classifierAt(tape.getEndPos() - 1);
    }
  }

  /// `MATCH_NUMBER()`: the sequential number of the match within its partition, starting at 1.
  final class MatchNumber implements MatchTerm {
    public static final MatchNumber INSTANCE = new MatchNumber();

    private MatchNumber() {
    }

    @Override
    public ColumnDataType getResultType() {
      return ColumnDataType.LONG;
    }

    @Override
    public Object evaluate(MatchTape tape) {
      return tape.getMatchNumber();
    }
  }

  /// A single variable aggregate in MEASURES, e.g. `SUM(A.price)` or `COUNT(*)`: the aggregate of an
  /// expression evaluated over every row of the match that is mapped to one pattern variable.
  ///
  /// The argument is evaluated by an ordinary [TransformOperand] against the raw input row, so any scalar
  /// expression works, e.g. `SUM(A.price * A.quantity)`. Nulls are skipped, as SQL requires; an aggregate over
  /// zero rows is `null` except for `COUNT`, which is `0`.
  final class Aggregate implements MatchTerm {
    private final Kind _kind;
    private final int _symbolOrdinal;
    @Nullable
    private final TransformOperand _argument;
    private final ColumnDataType _resultType;
    private final ColumnDataType _storedType;
    private final boolean _requiresResultConversion;

    /// @param argument the aggregated expression evaluated against an input row, or `null` for `COUNT(*)`,
    ///        which counts rows rather than values
    public Aggregate(Kind kind, int symbolOrdinal, @Nullable TransformOperand argument, ColumnDataType resultType) {
      _kind = kind;
      _symbolOrdinal = symbolOrdinal;
      _argument = argument;
      _resultType = resultType;
      _storedType = resultType.getStoredType();
      if (argument != null && argument.getResultType().isArray()) {
        throw QueryErrorCode.QUERY_EXECUTION.asException(
            "Multi-value operand type '" + argument.getResultType() + "' is not supported for " + kind
                + " in a MATCH_RECOGNIZE MEASURES clause. Reduce the array to a scalar before aggregating it.");
      }
      if ((kind == Kind.SUM || kind == Kind.AVG) && !_storedType.isNumber()) {
        throw QueryErrorCode.QUERY_EXECUTION.asException(
            "MATCH_RECOGNIZE " + kind + " requires a numeric result type, got: " + resultType + ".");
      }
      _requiresResultConversion = argument != null && argument.getResultType().getStoredType() != _storedType;
    }

    @Override
    public ColumnDataType getResultType() {
      return _resultType;
    }

    @Nullable
    @Override
    public Object evaluate(MatchTape tape) {
      if (_kind == Kind.COUNT && _argument == null
          && _symbolOrdinal == RexExpression.PatternFieldRef.UNIVERSAL_SYMBOL_ORDINAL) {
        return (long) tape.getLength();
      }
      IntArrayList rows = tape.rowsOf(_symbolOrdinal);
      if (_kind == Kind.COUNT && _argument == null) {
        return (long) rows.size();
      }
      List<Object[]> partitionRows = tape.getPartitionRows();
      switch (_kind) {
        case COUNT:
          return countNonNull(rows, partitionRows);
        case MIN:
        case MAX:
          return evaluateExtremum(rows, partitionRows);
        case SUM:
        case AVG:
          return evaluateSumOrAverage(rows, partitionRows);
        default:
          throw new IllegalStateException("Unexpected MATCH_RECOGNIZE aggregate: " + _kind);
      }
    }

    private long countNonNull(IntArrayList rows, List<Object[]> partitionRows) {
      long count = 0;
      for (int i = 0; i < rows.size(); i++) {
        Object value = _argument.apply(partitionRows.get(rows.getInt(i)));
        if (value != null) {
          count++;
        }
      }
      return count;
    }

    @Nullable
    private Object evaluateExtremum(IntArrayList rows, List<Object[]> partitionRows) {
      Object extremum = null;
      for (int i = 0; i < rows.size(); i++) {
        Object value = _argument.apply(partitionRows.get(rows.getInt(i)));
        if (value == null) {
          continue;
        }
        if (extremum == null || (_kind == Kind.MIN ? compare(value, extremum) < 0 : compare(value, extremum) > 0)) {
          extremum = value;
        }
      }
      if (extremum == null || !_requiresResultConversion) {
        return extremum;
      }
      return TypeUtils.convert(extremum, _storedType);
    }

    @Nullable
    private Object evaluateSumOrAverage(IntArrayList rows, List<Object[]> partitionRows) {
      switch (_storedType) {
        case INT:
        case LONG:
          return evaluateIntegralSumOrAverage(rows, partitionRows);
        case FLOAT:
        case DOUBLE:
          return evaluateFloatingPointSumOrAverage(rows, partitionRows);
        case BIG_DECIMAL:
          return evaluateDecimalSumOrAverage(rows, partitionRows);
        default:
          throw QueryErrorCode.QUERY_EXECUTION.asException(
              "MATCH_RECOGNIZE " + _kind + " requires a numeric result type, got: " + _resultType + ".");
      }
    }

    @Nullable
    private Object evaluateIntegralSumOrAverage(IntArrayList rows, List<Object[]> partitionRows) {
      long sum = 0;
      long count = 0;
      for (int i = 0; i < rows.size(); i++) {
        Object value = _argument.apply(partitionRows.get(rows.getInt(i)));
        if (value != null) {
          sum += numericValue(value).longValue();
          count++;
        }
      }
      if (count == 0) {
        return null;
      }
      long result = _kind == Kind.AVG ? sum / count : sum;
      if (_storedType == ColumnDataType.INT) {
        return (int) result;
      }
      return result;
    }

    @Nullable
    private Object evaluateFloatingPointSumOrAverage(IntArrayList rows, List<Object[]> partitionRows) {
      double sum = 0;
      long count = 0;
      for (int i = 0; i < rows.size(); i++) {
        Object value = _argument.apply(partitionRows.get(rows.getInt(i)));
        if (value != null) {
          sum += numericValue(value).doubleValue();
          count++;
        }
      }
      if (count == 0) {
        return null;
      }
      double result = _kind == Kind.AVG ? sum / count : sum;
      if (_storedType == ColumnDataType.FLOAT) {
        return (float) result;
      }
      return result;
    }

    @Nullable
    private Object evaluateDecimalSumOrAverage(IntArrayList rows, List<Object[]> partitionRows) {
      BigDecimal sum = BigDecimal.ZERO;
      long count = 0;
      for (int i = 0; i < rows.size(); i++) {
        Object value = _argument.apply(partitionRows.get(rows.getInt(i)));
        if (value != null) {
          sum = sum.add(toBigDecimal(numericValue(value)));
          count++;
        }
      }
      if (count == 0) {
        return null;
      }
      return _kind == Kind.AVG ? sum.divide(BigDecimal.valueOf(count), MathContext.DECIMAL128) : sum;
    }

    private Number numericValue(Object value) {
      if (value instanceof Number) {
        return (Number) value;
      }
      throw QueryErrorCode.QUERY_EXECUTION.asException(
          "MATCH_RECOGNIZE " + _kind + " requires scalar numeric values, got: " + value.getClass().getName() + ".");
    }

    private static BigDecimal toBigDecimal(Number value) {
      if (value instanceof BigDecimal) {
        return (BigDecimal) value;
      }
      if (value instanceof BigInteger) {
        return new BigDecimal((BigInteger) value);
      }
      if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
        return BigDecimal.valueOf(value.longValue());
      }
      return BigDecimal.valueOf(value.doubleValue());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int compare(Object left, Object right) {
      return ((Comparable) left).compareTo(right);
    }

    /// The aggregate functions supported inside MEASURES. Anything else is rejected at operator construction time
    /// rather than silently dropped.
    public enum Kind {
      COUNT, SUM, MIN, MAX, AVG;

      /// Resolves `functionName` to a supported aggregate, or `null` if it is not an aggregate at all.
      @Nullable
      public static Kind of(String functionName) {
        for (Kind kind : values()) {
          if (kind.name().equals(functionName)) {
            return kind;
          }
        }
        return null;
      }

      /// Throws with an actionable message for an aggregate that exists in Pinot but is not supported inside
      /// MEASURES yet.
      public static Kind resolveOrThrow(String functionName) {
        Kind kind = of(functionName);
        if (kind == null) {
          throw QueryErrorCode.QUERY_EXECUTION.asException(
              "Aggregate function '" + functionName + "' is not supported in a MATCH_RECOGNIZE MEASURES clause. "
                  + "Supported aggregates are " + Arrays.toString(values()) + ".");
        }
        return kind;
      }
    }
  }
}
