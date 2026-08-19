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
import java.math.MathContext;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.utils.TypeUtils;
import org.apache.pinot.spi.exception.QueryErrorCode;


/**
 * A leaf of a MEASURES or DEFINE expression whose value depends on the match rather than on a single row: a row
 * pattern navigation, {@code CLASSIFIER()}, {@code MATCH_NUMBER()}, or a single variable aggregate.
 *
 * <p>{@link MatchExpression} replaces every such leaf with a slot in a synthetic row, so that everything above the
 * leaves - comparisons, boolean connectives, arithmetic, scalar functions - is evaluated by Pinot's ordinary
 * {@link TransformOperand} machinery instead of a second expression interpreter.
 *
 * <p>Implementations are stateless with respect to the match: all match state is passed in through the
 * {@link MatchTape}, so one instance is reused across every match of every partition.
 */
public interface MatchTerm {

  /**
   * The type this term reports to the enclosing expression. Values returned by {@link #evaluate} are in the
   * corresponding {@link ColumnDataType#getStoredType() stored} representation, exactly like the values of a real
   * input row.
   */
  ColumnDataType getResultType();

  @Nullable
  Object evaluate(MatchTape tape);

  /**
   * A row pattern navigation: an optional logical step ({@code FIRST} / {@code LAST}) that designates a row of the
   * match, followed by an optional physical step ({@code PREV} / {@code NEXT}) that moves a fixed number of rows
   * relative to it, and finally a column read.
   *
   * <p>Both steps are needed because SQL:2016 nests them, e.g. {@code PREV(LAST(A.price), 2)} designates the last row
   * mapped to {@code A} and then moves two rows back. The logical step is bounded by the match; the physical step is
   * bounded only by the partition, so it may legally read a row outside the match. Either step falling off its bound
   * yields {@code null}, as the standard requires.
   */
  final class Navigation implements MatchTerm {
    private final int _symbolOrdinal;
    private final boolean _fromEnd;
    private final int _logicalOffset;
    private final int _physicalDelta;
    private final int _columnIndex;
    private final ColumnDataType _resultType;
    private final ColumnDataType _storedType;

    /**
     * @param symbolOrdinal pattern variable to navigate, or
     *        {@link org.apache.pinot.query.planner.logical.RexExpression.PatternFieldRef#UNIVERSAL_SYMBOL_ORDINAL}
     *        for an unqualified column reference, which navigates the rows of the whole match
     * @param fromEnd {@code true} for {@code LAST}, {@code false} for {@code FIRST}
     * @param logicalOffset how many rows back from the end (or forward from the start) of the designated variable
     * @param physicalDelta rows to move in the partition afterwards; negative for {@code PREV}, positive for
     *        {@code NEXT}, zero when there is no physical step
     * @param columnIndex column to read, as an index into the input row of the MATCH_RECOGNIZE node
     */
    public Navigation(int symbolOrdinal, boolean fromEnd, int logicalOffset, int physicalDelta, int columnIndex,
        ColumnDataType resultType) {
      _symbolOrdinal = symbolOrdinal;
      _fromEnd = fromEnd;
      _logicalOffset = logicalOffset;
      _physicalDelta = physicalDelta;
      _columnIndex = columnIndex;
      _resultType = resultType;
      _storedType = resultType.getStoredType();
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
      rowIndex += _physicalDelta;
      List<Object[]> rows = tape.getPartitionRows();
      if (rowIndex < 0 || rowIndex >= rows.size()) {
        return null;
      }
      Object value = rows.get(rowIndex)[_columnIndex];
      // The declared type of the navigation may be wider than the column's (e.g. BIG_DECIMAL for a DOUBLE column),
      // and the enclosing operand compares against that declared type.
      return value != null ? TypeUtils.convert(value, _storedType) : null;
    }
  }

  /**
   * {@code CLASSIFIER()}: the name of the pattern variable the designated row is mapped to. With ONE ROW PER MATCH
   * the designated row is the last row of the match, which is also the current row while a DEFINE predicate is being
   * evaluated.
   */
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

  /**
   * {@code MATCH_NUMBER()}: the sequential number of the match within its partition, starting at 1.
   */
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

  /**
   * A single variable aggregate in MEASURES, e.g. {@code SUM(A.price)} or {@code COUNT(*)}: the aggregate of an
   * expression evaluated over every row of the match that is mapped to one pattern variable.
   *
   * <p>The argument is evaluated by an ordinary {@link TransformOperand} against the raw input row, so any scalar
   * expression works, e.g. {@code SUM(A.price * A.quantity)}. Nulls are skipped, as SQL requires; an aggregate over
   * zero rows is {@code null} except for {@code COUNT}, which is {@code 0}.
   */
  final class Aggregate implements MatchTerm {
    private final Kind _kind;
    private final int _symbolOrdinal;
    @Nullable
    private final TransformOperand _argument;
    private final ColumnDataType _resultType;
    private final ColumnDataType _storedType;

    /**
     * @param argument the aggregated expression evaluated against an input row, or {@code null} for {@code COUNT(*)},
     *        which counts rows rather than values
     */
    public Aggregate(Kind kind, int symbolOrdinal, @Nullable TransformOperand argument, ColumnDataType resultType) {
      _kind = kind;
      _symbolOrdinal = symbolOrdinal;
      _argument = argument;
      _resultType = resultType;
      _storedType = resultType.getStoredType();
    }

    @Override
    public ColumnDataType getResultType() {
      return _resultType;
    }

    @Nullable
    @Override
    public Object evaluate(MatchTape tape) {
      IntArrayList rows = tape.rowsOf(_symbolOrdinal);
      List<Object[]> partitionRows = tape.getPartitionRows();
      if (_kind == Kind.COUNT && _argument == null) {
        return (long) rows.size();
      }
      long count = 0;
      Object accumulator = null;
      for (int i = 0; i < rows.size(); i++) {
        Object value = _argument.apply(partitionRows.get(rows.getInt(i)));
        if (value == null) {
          continue;
        }
        count++;
        // COUNT only needs the tally; accumulate() has no COUNT case and would throw.
        if (_kind != Kind.COUNT) {
          accumulator = accumulate(accumulator, value);
        }
      }
      if (_kind == Kind.COUNT) {
        return count;
      }
      if (count == 0) {
        return null;
      }
      Object result = _kind == Kind.AVG ? divide(accumulator, count) : accumulator;
      return TypeUtils.convert(result, _storedType);
    }

    private Object accumulate(@Nullable Object accumulator, Object value) {
      switch (_kind) {
        case MIN:
          return accumulator == null || compare(value, accumulator) < 0 ? value : accumulator;
        case MAX:
          return accumulator == null || compare(value, accumulator) > 0 ? value : accumulator;
        case SUM:
        case AVG:
          return add(accumulator, value);
        default:
          throw new IllegalStateException("Unexpected MATCH_RECOGNIZE aggregate: " + _kind);
      }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int compare(Object left, Object right) {
      return ((Comparable) left).compareTo(right);
    }

    /**
     * Accumulates without losing precision: exactly for {@code BIG_DECIMAL} and for the integral types, and in
     * {@code double} otherwise, which is what the declared result type of the aggregate already implies.
     */
    private Object add(@Nullable Object accumulator, Object value) {
      switch (_storedType) {
        case BIG_DECIMAL:
          BigDecimal decimal = value instanceof BigDecimal ? (BigDecimal) value
              : BigDecimal.valueOf(((Number) value).doubleValue());
          return accumulator == null ? decimal : ((BigDecimal) accumulator).add(decimal);
        case INT:
        case LONG:
          long longValue = ((Number) value).longValue();
          return accumulator == null ? longValue : (Long) accumulator + longValue;
        default:
          double doubleValue = ((Number) value).doubleValue();
          return accumulator == null ? doubleValue : (Double) accumulator + doubleValue;
      }
    }

    private static Object divide(Object sum, long count) {
      if (sum instanceof BigDecimal) {
        return ((BigDecimal) sum).divide(BigDecimal.valueOf(count), MathContext.DECIMAL128);
      }
      return ((Number) sum).doubleValue() / count;
    }

    /**
     * The aggregate functions supported inside MEASURES. Anything else is rejected at operator construction time
     * rather than silently dropped.
     */
    public enum Kind {
      COUNT, SUM, MIN, MAX, AVG;

      /**
       * Resolves {@code functionName} to a supported aggregate, or {@code null} if it is not an aggregate at all.
       */
      @Nullable
      public static Kind of(String functionName) {
        for (Kind kind : values()) {
          if (kind.name().equals(functionName)) {
            return kind;
          }
        }
        return null;
      }

      /**
       * Throws with an actionable message for an aggregate that exists in Pinot but is not supported inside
       * MEASURES yet.
       */
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
