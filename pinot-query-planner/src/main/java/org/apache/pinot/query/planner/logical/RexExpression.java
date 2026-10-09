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
package org.apache.pinot.query.planner.logical;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


/// `RexExpression` is the serializable format of the [org.apache.calcite.rex.RexNode].
public interface RexExpression {

  class InputRef implements RexExpression {
    private final int _index;

    public InputRef(int index) {
      _index = index;
    }

    public int getIndex() {
      return _index;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof InputRef)) {
        return false;
      }
      InputRef inputRef = (InputRef) o;
      return _index == inputRef._index;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_index);
    }
  }

  /// Reference to a column of the row bound to a specific MATCH_RECOGNIZE pattern variable, as it appears inside
  /// MEASURES and DEFINE expressions. It is the serializable form of Calcite's `RexPatternFieldRef`, and is
  /// only meaningful inside a `MatchNode`, whose pattern symbol table the ordinal indexes into.
  ///
  /// This deliberately does **not** extend [InputRef]. `RexPatternFieldRef` extends
  /// `RexInputRef`, so any code that only looks at the index silently turns
  /// `DEFINE UP AS UP.price > PREV(UP.price)` into a read of the current row's column: results that are wrong
  /// but still type-correct, and therefore invisible. Consumers that do not understand this class must fail loudly.
  class PatternFieldRef implements RexExpression {
    /// Ordinal used while converting from Calcite, before the pattern symbol table has been built. It must never
    /// reach the wire: `RexExpressionToProtoExpression` rejects it.
    public static final int UNRESOLVED_SYMBOL_ORDINAL = -1;
    /// Ordinal of the SQL:2016 *universal* row pattern variable, i.e. a column reference in MEASURES or DEFINE
    /// that is not qualified by a pattern variable, such as `price` in
    /// `DEFINE UP AS price > PREV(price)`. It denotes the rows of the match regardless of which variable they
    /// are mapped to, so it is **not** an index into the symbol table and must never be confused with one:
    /// `LAST(price)` is the last row of the whole match, whereas `LAST(X.price)` is the last row mapped
    /// to `X`.
    ///
    /// Calcite has no dedicated representation for it: it reuses the row source alias (the table alias) as the
    /// [alpha][#getAlpha()], which is why the planner maps any alpha that is not a pattern variable onto this
    /// ordinal. It is a legal wire value, unlike [#UNRESOLVED_SYMBOL_ORDINAL].
    public static final int UNIVERSAL_SYMBOL_ORDINAL = -2;

    private final int _index;
    private final int _symbolOrdinal;
    private final String _alpha;

    public PatternFieldRef(int index, int symbolOrdinal, String alpha) {
      _index = index;
      _symbolOrdinal = symbolOrdinal;
      _alpha = alpha;
    }

    /// Column index into the input row of the enclosing `MatchNode`.
    public int getIndex() {
      return _index;
    }

    /// Ordinal of the pattern variable, i.e. the index into the enclosing `MatchNode`'s symbol table, or
    /// [#UNIVERSAL_SYMBOL_ORDINAL] for an unqualified reference spanning every row of the match. The universal
    /// sentinel is the only legal negative wire value; [#UNRESOLVED_SYMBOL_ORDINAL] is planner-local only.
    /// This ordinal is the authoritative identification of the variable.
    public int getSymbolOrdinal() {
      return _symbolOrdinal;
    }

    /// Pattern variable name as written in the query, for explain plans and error messages only. Consumers must
    /// resolve the variable through [#getSymbolOrdinal()] and never string-match on this value.
    public String getAlpha() {
      return _alpha;
    }

    /// Returns a copy of this reference bound to the given pattern symbol ordinal.
    public PatternFieldRef withSymbolOrdinal(int symbolOrdinal) {
      return new PatternFieldRef(_index, symbolOrdinal, _alpha);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PatternFieldRef)) {
        return false;
      }
      PatternFieldRef that = (PatternFieldRef) o;
      return _index == that._index && _symbolOrdinal == that._symbolOrdinal && Objects.equals(_alpha, that._alpha);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_index, _symbolOrdinal, _alpha);
    }

    @Override
    public String toString() {
      return _alpha + "." + _index;
    }
  }

  class Literal implements RexExpression {
    public static final Literal TRUE = new Literal(ColumnDataType.BOOLEAN, 1);
    public static final Literal FALSE = new Literal(ColumnDataType.BOOLEAN, 0);

    private final ColumnDataType _dataType;
    private final Object _value;

    /// NOTE: Value is the internal stored value for the data type. E.g. BOOLEAN -> int, TIMESTAMP -> long.
    public Literal(ColumnDataType dataType, @Nullable Object value) {
      _dataType = dataType;
      _value = value;
    }

    public ColumnDataType getDataType() {
      return _dataType;
    }

    @Nullable
    public Object getValue() {
      return _value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Literal)) {
        return false;
      }
      Literal literal = (Literal) o;
      return _dataType == literal._dataType && Objects.deepEquals(_value, literal._value);
    }

    @Override
    public int hashCode() {
      return Arrays.deepHashCode(new Object[]{_dataType, _value});
    }
  }

  class FunctionCall implements RexExpression {
    // the return data type of the function.
    private final ColumnDataType _dataType;
    // the name of the SQL function. For standard SqlKind it should match the SqlKind ENUM name.
    private final String _functionName;
    // the list of RexExpressions that represents the operands to the function.
    private final List<RexExpression> _functionOperands;
    // whether the function is a distinct function.
    private final boolean _isDistinct;
    // whether the function should ignore nulls (relevant to certain window functions like LAST_VALUE).
    private final boolean _ignoreNulls;

    public FunctionCall(ColumnDataType dataType, String functionName, List<RexExpression> functionOperands) {
      this(dataType, functionName, functionOperands, false, false);
    }

    public FunctionCall(ColumnDataType dataType, String functionName, List<RexExpression> functionOperands,
        boolean isDistinct, boolean ignoreNulls) {
      _dataType = dataType;
      _functionName = functionName;
      _functionOperands = functionOperands;
      _isDistinct = isDistinct;
      _ignoreNulls = ignoreNulls;
    }

    public ColumnDataType getDataType() {
      return _dataType;
    }

    public String getFunctionName() {
      return _functionName;
    }

    public List<RexExpression> getFunctionOperands() {
      return _functionOperands;
    }

    public boolean isDistinct() {
      return _isDistinct;
    }

    public boolean isIgnoreNulls() {
      return _ignoreNulls;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FunctionCall)) {
        return false;
      }
      FunctionCall that = (FunctionCall) o;
      return _isDistinct == that._isDistinct && _ignoreNulls == that._ignoreNulls && _dataType == that._dataType
          && Objects.equals(_functionName, that._functionName)
          && Objects.equals(_functionOperands, that._functionOperands);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_dataType, _functionName, _functionOperands, _isDistinct, _ignoreNulls);
    }
  }
}
