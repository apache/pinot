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
package org.apache.pinot.query.planner.plannode;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;


/// MatchNode models SQL:2016 MATCH_RECOGNIZE (row pattern recognition): it partitions its input, orders each
/// partition, finds the runs of rows that match a row pattern, and emits the MEASURES of every match.
///
/// The pattern is carried as a self-contained [RowPattern] tree over a symbol table
/// ([#getPatternSymbols()]), rather than as Calcite's `RexCall`-over-string-literals encoding. Every
/// reference to a pattern variable - [RowPattern.Symbol], [RexExpression.PatternFieldRef] and
/// [#getAfterMatchSkipToSymbolOrdinal()] - is an ordinal into that table, so the operator never string-matches
/// variable names.
///
/// DataSchema on this node reflects the output schema: the PARTITION BY columns followed by the MEASURES columns
/// for [RowsPerMatchMode#ONE_ROW_PER_MATCH].
///
/// Not supported in this node and rejected at planning time: `SUBSET`, `PERMUTE`, `{- -}`
/// exclusions, `WITHIN`, `OMIT EMPTY MATCHES`, `WITH UNMATCHED ROWS`, aggregates inside
/// `DEFINE`, and [RowsPerMatchMode#ALL_ROWS_PER_MATCH].
public class MatchNode extends BasePlanNode {
  /// [#getAfterMatchSkipToSymbolOrdinal()] value for the skip modes that have no target variable.
  public static final int NO_SKIP_TO_SYMBOL = -1;

  private final List<PatternSymbol> _patternSymbols;
  private final RowPattern _pattern;
  private final List<Measure> _measures;
  private final List<Integer> _partitionKeys;
  private final List<RelFieldCollation> _collations;
  private final AfterMatchSkipMode _afterMatchSkipMode;
  private final int _afterMatchSkipToSymbolOrdinal;
  private final RowsPerMatchMode _rowsPerMatchMode;

  public MatchNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs,
      List<PatternSymbol> patternSymbols, RowPattern pattern, List<Measure> measures, List<Integer> partitionKeys,
      List<RelFieldCollation> collations, AfterMatchSkipMode afterMatchSkipMode, int afterMatchSkipToSymbolOrdinal,
      RowsPerMatchMode rowsPerMatchMode) {
    super(stageId, dataSchema, nodeHint, inputs);
    _patternSymbols = List.copyOf(patternSymbols);
    _pattern = pattern;
    _measures = List.copyOf(measures);
    _partitionKeys = List.copyOf(partitionKeys);
    _collations = List.copyOf(collations);
    _afterMatchSkipMode = afterMatchSkipMode;
    _afterMatchSkipToSymbolOrdinal = afterMatchSkipToSymbolOrdinal;
    _rowsPerMatchMode = rowsPerMatchMode;
  }

  /// The pattern variable symbol table. A symbol's ordinal is its index in this list.
  public List<PatternSymbol> getPatternSymbols() {
    return _patternSymbols;
  }

  /// Root of the row pattern tree, i.e. the `PATTERN` clause.
  public RowPattern getPattern() {
    return _pattern;
  }

  /// The `MEASURES` items, in output order.
  public List<Measure> getMeasures() {
    return _measures;
  }

  /// `PARTITION BY`: column indexes into the input row. Empty means a single partition over the whole input.
  public List<Integer> getPartitionKeys() {
    return _partitionKeys;
  }

  /// `ORDER BY`. Mandatory for MATCH_RECOGNIZE, so this is never empty.
  public List<RelFieldCollation> getCollations() {
    return _collations;
  }

  public AfterMatchSkipMode getAfterMatchSkipMode() {
    return _afterMatchSkipMode;
  }

  /// For [AfterMatchSkipMode#TO_FIRST] and [AfterMatchSkipMode#TO_LAST], the ordinal of the target
  /// pattern variable; [#NO_SKIP_TO_SYMBOL] for the other skip modes.
  public int getAfterMatchSkipToSymbolOrdinal() {
    return _afterMatchSkipToSymbolOrdinal;
  }

  public RowsPerMatchMode getRowsPerMatchMode() {
    return _rowsPerMatchMode;
  }

  /// Renders the pattern using the symbol table, e.g. `(A B* | C){2,3}`, for explain plans and error messages.
  public String getPatternString() {
    StringBuilder builder = new StringBuilder();
    _pattern.appendTo(builder, _patternSymbols);
    return builder.toString();
  }

  @Override
  public String explain() {
    return "MATCH_RECOGNIZE";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitMatch(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new MatchNode(_stageId, _dataSchema, _nodeHint, inputs, _patternSymbols, _pattern, _measures,
        _partitionKeys, _collations, _afterMatchSkipMode, _afterMatchSkipToSymbolOrdinal, _rowsPerMatchMode);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MatchNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    MatchNode that = (MatchNode) o;
    return _afterMatchSkipToSymbolOrdinal == that._afterMatchSkipToSymbolOrdinal
        && _afterMatchSkipMode == that._afterMatchSkipMode && _rowsPerMatchMode == that._rowsPerMatchMode
        && Objects.equals(_patternSymbols, that._patternSymbols) && Objects.equals(_pattern, that._pattern)
        && Objects.equals(_measures, that._measures) && Objects.equals(_partitionKeys, that._partitionKeys)
        && Objects.equals(_collations, that._collations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _patternSymbols, _pattern, _measures, _partitionKeys, _collations,
        _afterMatchSkipMode, _afterMatchSkipToSymbolOrdinal, _rowsPerMatchMode);
  }

  /// One `MEASURES <expression> AS <name>` item.
  public static final class Measure {
    private final String _name;
    private final RexExpression _expression;

    public Measure(String name, RexExpression expression) {
      _name = name;
      _expression = expression;
    }

    /// Output column name of this measure.
    public String getName() {
      return _name;
    }

    public RexExpression getExpression() {
      return _expression;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Measure)) {
        return false;
      }
      Measure that = (Measure) o;
      return Objects.equals(_name, that._name) && Objects.equals(_expression, that._expression);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_name, _expression);
    }

    @Override
    public String toString() {
      return _expression + " AS " + _name;
    }
  }

  /// SQL:2016 `AFTER MATCH SKIP` clause: where pattern matching resumes after a successful match.
  ///
  /// [#PAST_LAST_ROW] is the SQL:2016 default (and the default in Trino, Snowflake and Oracle), which
  /// produces non-overlapping matches. Calcite defaults an omitted clause to [#TO_NEXT_ROW] instead, which
  /// produces overlapping matches, so the default must be fixed at the `SqlNode` level before conversion.
  ///
  /// The constant names are part of the wire protocol via `Plan.AfterMatchSkipMode` and must remain stable
  /// across mixed-version brokers and servers.
  public enum AfterMatchSkipMode {
    /// Resume at the row after the last row of the match.
    PAST_LAST_ROW,
    /// Resume at the row after the first row of the match, so matches may overlap.
    TO_NEXT_ROW,
    /// Resume at the first row mapped to the target pattern variable.
    TO_FIRST,
    /// Resume at the last row mapped to the target pattern variable.
    TO_LAST
  }

  /// SQL:2016 `ONE ROW PER MATCH` / `ALL ROWS PER MATCH`.
  ///
  /// [#ALL_ROWS_PER_MATCH] is declared so its wire value is pinned and so the planner can reject it by
  /// name, but it is not supported yet.
  ///
  /// The constant names are part of the wire protocol via `Plan.RowsPerMatchMode` and must remain stable
  /// across mixed-version brokers and servers.
  public enum RowsPerMatchMode {
    ONE_ROW_PER_MATCH, ALL_ROWS_PER_MATCH
  }
}
