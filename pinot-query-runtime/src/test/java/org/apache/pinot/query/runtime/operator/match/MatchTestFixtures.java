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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.PatternSymbol;
import org.apache.pinot.query.planner.plannode.RowPattern;


/// Shared fixtures for the MATCH_RECOGNIZE runtime tests.
///
/// Rows use a deliberately trivial data model: column `label` carries the name of the pattern variable the
/// row is supposed to match, so a test can describe its input as the string `"AABB"` and read the expected
/// classification straight off it. Column `value` carries an increasing number for tests that need arithmetic or
/// navigation, and `pid` is the partition key.
public final class MatchTestFixtures {
  private MatchTestFixtures() {
  }

  public static final int PID_INDEX = 0;
  public static final int LABEL_INDEX = 1;
  public static final int VALUE_INDEX = 2;

  public static final DataSchema INPUT_SCHEMA = new DataSchema(new String[]{"pid", "label", "value"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT});

  /// A pattern variable whose DEFINE is `label = <name>`, i.e. it matches exactly the rows whose `label`
  /// column holds its own name.
  public static PatternSymbol labelSymbol(String name, int ordinal) {
    RexExpression reference = new RexExpression.PatternFieldRef(LABEL_INDEX, ordinal, name);
    RexExpression literal = new RexExpression.Literal(ColumnDataType.STRING, name);
    return new PatternSymbol(name,
        new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, "EQUALS", List.of(reference, literal)));
  }

  /// A pattern variable with no DEFINE clause, which matches every row per SQL:2016.
  public static PatternSymbol anySymbol(String name) {
    return new PatternSymbol(name, null);
  }

  /// Symbol table where each name is a [#labelSymbol], in the order given.
  public static List<PatternSymbol> labelSymbols(String... names) {
    List<PatternSymbol> symbols = new ArrayList<>(names.length);
    for (int i = 0; i < names.length; i++) {
      symbols.add(labelSymbol(names[i], i));
    }
    return symbols;
  }

  public static RowPattern symbol(int ordinal) {
    return new RowPattern.Symbol(ordinal);
  }

  public static RowPattern concat(RowPattern... children) {
    return new RowPattern.Concat(Arrays.asList(children));
  }

  public static RowPattern alternate(RowPattern... children) {
    return new RowPattern.Alternate(Arrays.asList(children));
  }

  public static RowPattern quantify(RowPattern child, int minRepeat, int maxRepeat, boolean greedy) {
    return new RowPattern.Quantify(child, minRepeat, maxRepeat, greedy);
  }

  /// Rows of a single partition, one per character of `labels`. The `value` column is the row index, so
  /// navigation tests can assert on a value that identifies the row.
  public static List<Object[]> rows(String labels) {
    return rows(0, labels);
  }

  public static List<Object[]> rows(int partitionId, String labels) {
    List<Object[]> rows = new ArrayList<>(labels.length());
    for (int i = 0; i < labels.length(); i++) {
      rows.add(new Object[]{partitionId, String.valueOf(labels.charAt(i)), i});
    }
    return rows;
  }
}
