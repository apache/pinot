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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.PatternSymbol;


/// The classifier tape of the match currently being explored: which pattern variable each row of the candidate match
/// is mapped to, plus everything the MEASURES and DEFINE expressions need in order to navigate it.
///
/// ## Append only, O(1) backtrack
///
/// The tape grows by one entry every time the matcher consumes a row ([#push]) and shrinks by one every time it
/// backtracks over that row ([#pop]). Both are O(1): [#pop] only decrements lengths, it never rebuilds an
/// index. The per symbol position lists are maintained the same way, which makes [#lastRow] and
/// [#firstRow] O(1) as well, so logical navigation does not degrade into a scan of the match.
///
/// ## Rows are addressed by partition index
///
/// All row indexes handed out by this class are indexes into the partition, not into the match, because physical
/// navigation (`PREV` / `NEXT`) may leave the match while staying inside the partition. The invariant
/// `getEndPos() == getStartPos() + getLength()` always holds: the rows of a match are contiguous.
///
/// ## Running semantics
///
/// While the matcher is exploring, the tape holds only the rows matched so far, so a DEFINE predicate evaluated
/// through it automatically sees SQL:2016 *running* semantics. The candidate row is pushed *before* its own
/// predicate is evaluated, which is what makes `PREV(A.col, 0)` inside `DEFINE A` resolve to the current
/// row. Once the match is complete the tape holds all of its rows, so the same lookups give *final* semantics for
/// MEASURES.
///
/// Not thread safe: one instance belongs to one [PartitionMatcher].
public class MatchTape {
  /// Returned by [#lastRow] and [#firstRow] when no such row exists.
  public static final int NO_ROW = -1;

  private final List<PatternSymbol> _patternSymbols;
  /// Partition row indexes mapped to each symbol, in ascending order; index is the symbol ordinal.
  private final IntArrayList[] _rowsBySymbol;

  private List<Object[]> _partitionRows = List.of();
  private int _startPos;
  private int[] _labels = new int[16];
  private int _length;
  private long _matchNumber;

  public MatchTape(List<PatternSymbol> patternSymbols) {
    _patternSymbols = patternSymbols;
    _rowsBySymbol = new IntArrayList[patternSymbols.size()];
    for (int i = 0; i < _rowsBySymbol.length; i++) {
      _rowsBySymbol[i] = new IntArrayList();
    }
  }

  /// Starts a new match attempt at `startPos` of `partitionRows`. `matchNumber` is the value
  /// `MATCH_NUMBER()` reports, which SQL:2016 assigns before the match is known to succeed.
  public void reset(List<Object[]> partitionRows, int startPos, long matchNumber) {
    _partitionRows = partitionRows;
    _startPos = startPos;
    _length = 0;
    _matchNumber = matchNumber;
    for (IntArrayList rows : _rowsBySymbol) {
      rows.clear();
    }
  }

  /// Maps the row at [#getEndPos()] to `symbolOrdinal` and extends the tape by one row.
  public void push(int symbolOrdinal) {
    if (_length == _labels.length) {
      int[] grown = new int[_labels.length * 2];
      System.arraycopy(_labels, 0, grown, 0, _length);
      _labels = grown;
    }
    _labels[_length] = symbolOrdinal;
    _rowsBySymbol[symbolOrdinal].add(_startPos + _length);
    _length++;
  }

  /// Removes the last row from the tape. O(1); this is what makes backtracking cheap.
  public void pop() {
    _length--;
    IntArrayList rows = _rowsBySymbol[_labels[_length]];
    rows.removeInt(rows.size() - 1);
  }

  public List<Object[]> getPartitionRows() {
    return _partitionRows;
  }

  /// Partition index of the first row of the match.
  public int getStartPos() {
    return _startPos;
  }

  /// Partition index one past the last row of the match, i.e. where the next row would be consumed.
  public int getEndPos() {
    return _startPos + _length;
  }

  /// Number of rows currently on the tape.
  public int getLength() {
    return _length;
  }

  public long getMatchNumber() {
    return _matchNumber;
  }

  /// Symbol ordinal the row at partition index `rowIndex` is mapped to, or [#NO_ROW] if that row is not
  /// part of the match.
  public int labelAt(int rowIndex) {
    int offset = rowIndex - _startPos;
    return offset >= 0 && offset < _length ? _labels[offset] : NO_ROW;
  }

  /// Name of the pattern variable the row at partition index `rowIndex` is mapped to, i.e. the value of
  /// `CLASSIFIER()` at that row, or `null` if that row is not part of the match.
  @Nullable
  public String classifierAt(int rowIndex) {
    int label = labelAt(rowIndex);
    return label == NO_ROW ? null : _patternSymbols.get(label).getName();
  }

  /// Partition index of the `(offset + 1)`-th row from the **end** of the rows mapped to
  /// `symbolOrdinal`, i.e. the row `LAST(symbol.col, offset)` designates. Pass
  /// [RexExpression.PatternFieldRef#UNIVERSAL_SYMBOL_ORDINAL] to navigate the rows of the whole match instead of
  /// a single variable. Returns [#NO_ROW] if there are not that many such rows.
  public int lastRow(int symbolOrdinal, int offset) {
    if (symbolOrdinal == RexExpression.PatternFieldRef.UNIVERSAL_SYMBOL_ORDINAL) {
      int index = _length - 1 - offset;
      return index >= 0 ? _startPos + index : NO_ROW;
    }
    IntArrayList rows = _rowsBySymbol[symbolOrdinal];
    int index = rows.size() - 1 - offset;
    return index >= 0 ? rows.getInt(index) : NO_ROW;
  }

  /// Partition index of the `(offset + 1)`-th row from the **start** of the rows mapped to
  /// `symbolOrdinal`, i.e. the row `FIRST(symbol.col, offset)` designates. Behaves like [#lastRow]
  /// otherwise.
  public int firstRow(int symbolOrdinal, int offset) {
    if (symbolOrdinal == RexExpression.PatternFieldRef.UNIVERSAL_SYMBOL_ORDINAL) {
      return offset < _length ? _startPos + offset : NO_ROW;
    }
    IntArrayList rows = _rowsBySymbol[symbolOrdinal];
    return offset < rows.size() ? rows.getInt(offset) : NO_ROW;
  }

  /// Number of rows mapped to `symbolOrdinal`, or the length of the whole match for
  /// [RexExpression.PatternFieldRef#UNIVERSAL_SYMBOL_ORDINAL].
  public int rowCount(int symbolOrdinal) {
    if (symbolOrdinal == RexExpression.PatternFieldRef.UNIVERSAL_SYMBOL_ORDINAL) {
      return _length;
    }
    return _rowsBySymbol[symbolOrdinal].size();
  }

  /// Partition index of the row at `logicalIndex` among the rows mapped to `symbolOrdinal`, or in the whole match
  /// for [RexExpression.PatternFieldRef#UNIVERSAL_SYMBOL_ORDINAL]. Universal rows are contiguous, so this lookup
  /// traverses them directly without allocating or filling a per-aggregate position list.
  public int rowAt(int symbolOrdinal, int logicalIndex) {
    if (symbolOrdinal == RexExpression.PatternFieldRef.UNIVERSAL_SYMBOL_ORDINAL) {
      return _startPos + logicalIndex;
    }
    return _rowsBySymbol[symbolOrdinal].getInt(logicalIndex);
  }
}
