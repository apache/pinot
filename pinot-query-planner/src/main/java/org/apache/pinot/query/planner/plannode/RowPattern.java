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


/// A node of the MATCH_RECOGNIZE row pattern tree, i.e. the parsed form of the `PATTERN` clause.
///
/// This is a self-contained representation: pattern variables are referenced by their ordinal in the enclosing
/// [MatchNode]'s symbol table, never by name, so an operator never has to string-match variable names. It is
/// also independent of Calcite's encoding of the pattern as a `RexCall` tree over string literals.
///
/// Instances are immutable.
public interface RowPattern {

  Kind getKind();

  /// Renders this pattern into `builder` using `symbols` to resolve variable ordinals to names, e.g.
  /// `(A B* | C){2,3}`. Used for explain plans and error messages.
  void appendTo(StringBuilder builder, List<PatternSymbol> symbols);

  /// The kind of pattern node.
  ///
  /// The SQL:2016 `{- -}` exclusion and `PERMUTE(...)` constructs are deliberately absent: they are
  /// rejected at planning time. Their wire values are already pinned in `Plan.RowPatternKind` so that adding
  /// them later is a purely additive change.
  enum Kind {
    SYMBOL, CONCAT, ALTERNATE, QUANTIFY, ANCHOR_START, ANCHOR_END
  }

  /// A single pattern variable, identified by its ordinal in the enclosing [MatchNode]'s symbol table.
  final class Symbol implements RowPattern {
    private final int _symbolOrdinal;

    public Symbol(int symbolOrdinal) {
      _symbolOrdinal = symbolOrdinal;
    }

    public int getSymbolOrdinal() {
      return _symbolOrdinal;
    }

    @Override
    public Kind getKind() {
      return Kind.SYMBOL;
    }

    @Override
    public void appendTo(StringBuilder builder, List<PatternSymbol> symbols) {
      builder.append(
          _symbolOrdinal >= 0 && _symbolOrdinal < symbols.size() ? symbols.get(_symbolOrdinal).getName()
              : "?" + _symbolOrdinal);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Symbol)) {
        return false;
      }
      return _symbolOrdinal == ((Symbol) o)._symbolOrdinal;
    }

    @Override
    public int hashCode() {
      return Objects.hash(Kind.SYMBOL, _symbolOrdinal);
    }
  }

  /// Juxtaposition of two or more sub-patterns that must match consecutively, e.g. `A B C`.
  final class Concat implements RowPattern {
    private final List<RowPattern> _children;

    public Concat(List<RowPattern> children) {
      _children = List.copyOf(children);
    }

    public List<RowPattern> getChildren() {
      return _children;
    }

    @Override
    public Kind getKind() {
      return Kind.CONCAT;
    }

    @Override
    public void appendTo(StringBuilder builder, List<PatternSymbol> symbols) {
      for (int i = 0; i < _children.size(); i++) {
        if (i > 0) {
          builder.append(' ');
        }
        _children.get(i).appendTo(builder, symbols);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Concat)) {
        return false;
      }
      return _children.equals(((Concat) o)._children);
    }

    @Override
    public int hashCode() {
      return Objects.hash(Kind.CONCAT, _children);
    }
  }

  /// Alternation of two or more sub-patterns, e.g. `A | B`. Alternatives are ordered: per SQL:2016 the
  /// leftmost alternative that yields a match wins.
  final class Alternate implements RowPattern {
    private final List<RowPattern> _children;

    public Alternate(List<RowPattern> children) {
      _children = List.copyOf(children);
    }

    public List<RowPattern> getChildren() {
      return _children;
    }

    @Override
    public Kind getKind() {
      return Kind.ALTERNATE;
    }

    @Override
    public void appendTo(StringBuilder builder, List<PatternSymbol> symbols) {
      builder.append('(');
      for (int i = 0; i < _children.size(); i++) {
        if (i > 0) {
          builder.append(" | ");
        }
        _children.get(i).appendTo(builder, symbols);
      }
      builder.append(')');
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Alternate)) {
        return false;
      }
      return _children.equals(((Alternate) o)._children);
    }

    @Override
    public int hashCode() {
      return Objects.hash(Kind.ALTERNATE, _children);
    }
  }

  /// A repetition applied to a single sub-pattern. `*`, `+` and `?` are normalized into this form:
  /// `*` is `{0,UNBOUNDED}`, `+` is `{1,UNBOUNDED}` and `?` is `{0,1}`.
  final class Quantify implements RowPattern {
    /// Sentinel for `maxRepeat` meaning "no upper bound", i.e. `*`, `+` and `{n,}`.
    public static final int UNBOUNDED = -1;

    private final RowPattern _child;
    private final int _minRepeat;
    private final int _maxRepeat;
    private final boolean _greedy;

    public Quantify(RowPattern child, int minRepeat, int maxRepeat, boolean greedy) {
      _child = child;
      _minRepeat = minRepeat;
      _maxRepeat = maxRepeat;
      _greedy = greedy;
    }

    public RowPattern getChild() {
      return _child;
    }

    public int getMinRepeat() {
      return _minRepeat;
    }

    /// Maximum number of repetitions, or [#UNBOUNDED].
    public int getMaxRepeat() {
      return _maxRepeat;
    }

    /// Whether the quantifier is greedy. The reluctant forms (`*?`, `+?`, `??`, `{n,m}?`)
    /// prefer the shortest match.
    public boolean isGreedy() {
      return _greedy;
    }

    @Override
    public Kind getKind() {
      return Kind.QUANTIFY;
    }

    @Override
    public void appendTo(StringBuilder builder, List<PatternSymbol> symbols) {
      // Concat binds looser than a quantifier, so it has to be parenthesized. Alternate parenthesizes itself.
      boolean parenthesize = _child.getKind() == Kind.CONCAT;
      if (parenthesize) {
        builder.append('(');
      }
      _child.appendTo(builder, symbols);
      if (parenthesize) {
        builder.append(')');
      }
      appendQuantifier(builder);
      if (!_greedy) {
        builder.append('?');
      }
    }

    private void appendQuantifier(StringBuilder builder) {
      if (_minRepeat == 0 && _maxRepeat == UNBOUNDED) {
        builder.append('*');
      } else if (_minRepeat == 1 && _maxRepeat == UNBOUNDED) {
        builder.append('+');
      } else if (_minRepeat == 0 && _maxRepeat == 1) {
        builder.append('?');
      } else if (_maxRepeat == UNBOUNDED) {
        builder.append('{').append(_minRepeat).append(",}");
      } else if (_minRepeat == _maxRepeat) {
        builder.append('{').append(_minRepeat).append('}');
      } else {
        builder.append('{').append(_minRepeat).append(',').append(_maxRepeat).append('}');
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Quantify)) {
        return false;
      }
      Quantify that = (Quantify) o;
      return _minRepeat == that._minRepeat && _maxRepeat == that._maxRepeat && _greedy == that._greedy
          && _child.equals(that._child);
    }

    @Override
    public int hashCode() {
      return Objects.hash(Kind.QUANTIFY, _child, _minRepeat, _maxRepeat, _greedy);
    }
  }

  /// The `^` anchor: the match must start at the first row of the partition.
  final class AnchorStart implements RowPattern {
    public static final AnchorStart INSTANCE = new AnchorStart();

    private AnchorStart() {
    }

    @Override
    public Kind getKind() {
      return Kind.ANCHOR_START;
    }

    @Override
    public void appendTo(StringBuilder builder, List<PatternSymbol> symbols) {
      builder.append('^');
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof AnchorStart;
    }

    @Override
    public int hashCode() {
      return Kind.ANCHOR_START.hashCode();
    }
  }

  /// The `$` anchor: the match must end at the last row of the partition.
  final class AnchorEnd implements RowPattern {
    public static final AnchorEnd INSTANCE = new AnchorEnd();

    private AnchorEnd() {
    }

    @Override
    public Kind getKind() {
      return Kind.ANCHOR_END;
    }

    @Override
    public void appendTo(StringBuilder builder, List<PatternSymbol> symbols) {
      builder.append('$');
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof AnchorEnd;
    }

    @Override
    public int hashCode() {
      return Kind.ANCHOR_END.hashCode();
    }
  }
}
