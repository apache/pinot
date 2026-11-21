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
package org.apache.pinot.query.validate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlDelegatingConformance;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Util;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;


/// The `Validator` overwrites Calcite's Validator with Pinot specific logics.
public class Validator extends SqlValidatorImpl {

  /// Pinot conformance that delegates to BABEL but disables non-strict GROUP BY.
  ///
  /// Calcite 1.41.0 (CALCITE-7189) added MySQL-style non-strict GROUP BY support that wraps non-aggregated,
  /// non-grouped columns in `ANY_VALUE()`. However, the implementation has a bug where it doesn't handle window
  /// functions properly, causing an NPE during validation of queries like
  /// `SELECT MIN(col) OVER() FROM t GROUP BY col`. Disabling non-strict GROUP BY avoids this bug and is the
  /// correct behavior for Pinot, which requires all non-aggregated columns to appear in GROUP BY. The feature is still
  /// present (un-reverted) in Calcite 1.42.0, so this override must be retained.
  private static final SqlDelegatingConformance PINOT_CONFORMANCE =
      new SqlDelegatingConformance(SqlConformanceEnum.BABEL) {
        @Override
        public boolean isNonStrictGroupBy() {
          return false;
        }
      };

  private final boolean _preferSelectAliasInGroupBy;
  private final boolean _meterGroupByAliasShadowedByColumn;
  /// Whether a `GROUP BY` name of this query matches a `SELECT` alias and a column. See [#rewriteGroupByName].
  private boolean _groupByAliasShadowedByColumnFound;

  /// @param preferSelectAliasInGroupBy whether a simple name in `GROUP BY` resolves to a `SELECT` alias before a
  ///                                   column of the same name. See [#rewriteGroupByAliases].
  /// @param meterGroupByAliasShadowedByColumn whether to mark
  ///                                          [BrokerMeter#QUERIES_WITH_GROUP_BY_ALIAS_SHADOWED_BY_COLUMN]. See
  ///                                          [#validateGroupClause].
  public Validator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory,
      boolean preferSelectAliasInGroupBy, boolean meterGroupByAliasShadowedByColumn) {
    super(opTab, catalogReader, typeFactory,
        Config.DEFAULT.withConformance(PINOT_CONFORMANCE)
            .withIdentifierExpansion(true)
            .withTypeCoercionFactory(PinotTypeCoercion::new));
    _preferSelectAliasInGroupBy = preferSelectAliasInGroupBy;
    _meterGroupByAliasShadowedByColumn = meterGroupByAliasShadowedByColumn;
  }

  /// Finds each simple name in the `GROUP BY` list that matches exactly one `SELECT` alias and a column, unless the
  /// `SELECT` item is the column of the same name. When [#_preferSelectAliasInGroupBy] is set, replaces the name with
  /// the ordinal of that `SELECT` item.
  ///
  /// Calcite 1.42 (CALCITE-7052) resolves a `GROUP BY` name to a column first, and to a `SELECT` alias only when no
  /// column (or more than one) has that name. Calcite 1.40 resolved the alias first. Given
  /// `SELECT CASE WHEN c = 'a' THEN 'x' ELSE c END AS c, COUNT(*) FROM t GROUP BY c`, Calcite 1.40 groups by the
  /// `CASE` expression and Calcite 1.42 groups by the column `c`. An ordinal always refers to the `SELECT` item, so
  /// the rewrite restores the Calcite 1.40 result. It also makes `HAVING` resolve the name to the alias, as before.
  ///
  /// The column that Calcite 1.42 resolves the name to is one of these:
  /// - the column of a `USING` or `NATURAL` join
  /// - the column of exactly one table of the `FROM` clause
  /// - when no table of the `FROM` clause has the name, the column of exactly one table of an outer query
  ///
  /// Any other name is not replaced. Both Calcite versions resolve it to the alias. Calcite expands an alias, but not
  /// an ordinal, when the `SELECT` item uses another `SELECT` alias or a `USING` column, so an ordinal can make such
  /// a query fail.
  ///
  /// The rewrite also applies to the names inside `ROLLUP`, `CUBE`, `GROUPING SETS` and their `ROW` operands, because
  /// Calcite 1.40 resolved those names to the alias too, and Calcite 1.42 expands ordinals there as well.
  private void rewriteGroupByAliases(SqlSelect select) {
    SqlNodeList groupList = select.getGroup();
    if (groupList != null) {
      rewriteGroupByAliases(groupList, select, getCatalogReader().nameMatcher());
    }
  }

  /// Rewrites the `GROUP BY` items in `nodes` in place, and recurses into grouping-set operators.
  private void rewriteGroupByAliases(List<SqlNode> nodes, SqlSelect select, SqlNameMatcher nameMatcher) {
    for (int i = 0; i < nodes.size(); i++) {
      SqlNode node = nodes.get(i);
      if (node instanceof SqlNodeList) {
        rewriteGroupByAliases((SqlNodeList) node, select, nameMatcher);
      } else if (node instanceof SqlCall && isGroupingSetOperator(node.getKind())) {
        SqlCall call = (SqlCall) node;
        List<SqlNode> operands = new ArrayList<>(call.getOperandList());
        rewriteGroupByAliases(operands, select, nameMatcher);
        for (int j = 0; j < operands.size(); j++) {
          if (operands.get(j) != call.operand(j)) {
            call.setOperand(j, operands.get(j));
          }
        }
      } else if (node instanceof SqlIdentifier && ((SqlIdentifier) node).isSimple()) {
        SqlNode replacement = rewriteGroupByName((SqlIdentifier) node, select, nameMatcher);
        if (replacement != node) {
          nodes.set(i, replacement);
        }
      }
    }
  }

  private static boolean isGroupingSetOperator(SqlKind kind) {
    return kind == SqlKind.ROLLUP || kind == SqlKind.CUBE || kind == SqlKind.GROUPING_SETS || kind == SqlKind.ROW;
  }

  /// Returns the ordinal that replaces `identifier`, or `identifier` itself when it is not replaced. On the first name
  /// that matches an alias and a column, marks [BrokerMeter#QUERIES_WITH_GROUP_BY_ALIAS_SHADOWED_BY_COLUMN] if
  /// [#_meterGroupByAliasShadowedByColumn] is set.
  private SqlNode rewriteGroupByName(SqlIdentifier identifier, SqlSelect select, SqlNameMatcher nameMatcher) {
    String name = identifier.getSimple();
    List<SqlNode> selectList = select.getSelectList().getList();
    int matchIndex = -1;
    int matchCount = 0;
    for (int j = 0; j < selectList.size(); j++) {
      String alias = SqlValidatorUtil.alias(selectList.get(j));
      if (alias != null && nameMatcher.matches(alias, name)) {
        matchIndex = j;
        matchCount++;
      }
    }
    // With more than one match, Calcite 1.40 failed with an ambiguity error. Keep the Calcite 1.42 resolution.
    if (matchCount != 1) {
      return identifier;
    }
    SqlNode expr = SqlUtil.stripAs(selectList.get(matchIndex));
    if (expr instanceof SqlIdentifier && nameMatcher.matches(Util.last(((SqlIdentifier) expr).names), name)) {
      return identifier;
    }
    SelectScope selectScope = getRawSelectScope(select);
    if (selectScope == null || (!isCommonJoinColumn(select.getFrom(), name, nameMatcher)
        && selectScope.findQualifyingTableNames(name, select, nameMatcher).size() != 1)) {
      return identifier;
    }
    if (!_groupByAliasShadowedByColumnFound) {
      _groupByAliasShadowedByColumnFound = true;
      if (_meterGroupByAliasShadowedByColumn) {
        BrokerMetrics.get().addMeteredGlobalValue(BrokerMeter.QUERIES_WITH_GROUP_BY_ALIAS_SHADOWED_BY_COLUMN, 1);
      }
    }
    if (!_preferSelectAliasInGroupBy) {
      return identifier;
    }
    return SqlLiteral.createExactNumeric(String.valueOf(matchIndex + 1), identifier.getParserPosition());
  }

  /// Returns whether `name` is a column of a `USING` or `NATURAL` join in `from`. Follows the joins that Calcite
  /// looks at to expand such a column.
  private boolean isCommonJoinColumn(@Nullable SqlNode from, String name, SqlNameMatcher nameMatcher) {
    while (from instanceof SqlJoin) {
      SqlJoin join = (SqlJoin) from;
      List<String> commonColumns = usingNames(join);
      if (commonColumns == null) {
        return false;
      }
      if (nameMatcher.indexOf(commonColumns, name) >= 0) {
        return true;
      }
      from = join.getLeft();
    }
    return false;
  }

  /// Runs [#rewriteGroupByAliases] here because it needs the scopes to find the columns.
  ///
  /// This runs before the validation of the `SELECT` list, so a name that matches an alias and a column is also found
  /// when the query then fails with `Expression '...' is not being grouped`. See
  /// [#isGroupByNameResolvedToShadowedColumn].
  ///
  /// Only multi-stage queries mark the meter. Single-stage queries always resolve the name to the alias, so the
  /// Calcite change does not affect them.
  @Override
  protected void validateGroupClause(SqlSelect select) {
    rewriteGroupByAliases(select);
    super.validateGroupClause(select);
  }

  /// Returns whether the validation resolved a `GROUP BY` name to a column although a `SELECT` alias has the same
  /// name. That is, a name matches an alias and a column, and `preferSelectAliasInGroupBy` is not set. With the option
  /// set, the same query can validate.
  public boolean isGroupByNameResolvedToShadowedColumn() {
    return _groupByAliasShadowedByColumnFound && !_preferSelectAliasInGroupBy;
  }

  /// Expand the star in the select list.
  /// Pinot table schema has all columns along with virtual columns.
  /// We don't want to include virtual columns in the select \* query
  ///
  /// @param selectList        Select clause to be expanded
  /// @param select             Query
  /// @param includeSystemVars Whether to include system variables
  /// @return Expanded select list
  @Override
  public SqlNodeList expandStar(
      SqlNodeList selectList,
      SqlSelect select,
      boolean includeSystemVars) {
    SqlNodeList expandedSelectList = super.expandStar(selectList, select, includeSystemVars);
    RelRecordType validatedNodeType = (RelRecordType) getValidatedNodeType(select);

    List<String> selectedVirtualColumns = getSelectedVirtualColumns(select);
    // ExpandStar will add a field for each column in the table, but we don't want to include the virtual columns
    List<SqlNode> newSelectList = new ArrayList<>();
    List<RelDataTypeField> newFieldList = new ArrayList<>();
    for (int i = 0; i < expandedSelectList.size(); i++) {
      SqlNode node = expandedSelectList.get(i);
      if (node instanceof SqlIdentifier) {
        String columnName = getColumnName((SqlIdentifier) node);
        if (isVirtualColumn(columnName)) {
          // If the virtual column is selected, remove it from the list of selected virtual columns
          if (!selectedVirtualColumns.remove(columnName)) {
            continue;
          }
        }
      }
      newSelectList.add(node);
      newFieldList.add(validatedNodeType.getFieldList().get(i));
    }
    // Ensure the validation node type is updated
    setValidatedNodeType(select, new RelRecordType(newFieldList));
    return new SqlNodeList(newSelectList, selectList.getParserPosition());
  }

  /// Get the all the virtual columns explicitly selected from the query selection list.
  /// Use list in case there are duplicates.
  ///
  /// @param select query
  /// @return list of virtual columns selected
  private List<String> getSelectedVirtualColumns(SqlSelect select) {
    List<String> columnNamesFromSelectList = new ArrayList<>();
    for (SqlNode node : select.getSelectList().getList()) {
      if (node instanceof SqlIdentifier) {
        String columnName = getColumnName((SqlIdentifier) node);
        if (isVirtualColumn(columnName)) {
          columnNamesFromSelectList.add(columnName);
        }
      }
    }
    return columnNamesFromSelectList;
  }

  /// Check if the column is a virtual column.
  /// @param columnName column name
  /// @return true if the column is a virtual column
  public static boolean isVirtualColumn(String columnName) {
    return columnName.length() > 0 && columnName.charAt(0) == '$';
  }

  /// Extract the column name from the identifier. The identifier could be in the format of \[table\].\[columnName\] or
  /// just \[columnName\].
  /// @param identifier column identifier
  /// @return column name
  private static String getColumnName(SqlIdentifier identifier) {
    List<String> names = identifier.names;
    return names.get(names.size() - 1);
  }

  /// This method is only called by Calcite when adding rows to select as a result of doing `select *`.
  ///
  /// This is just a patch knowing the internals of Calcite, but there is no officially supported solution right now.
  /// See [this question in the dev email list](https://lists.apache.org/thread/c71fkv329001hjjq7bp4hm56q6yx6dvw)
  @Override
  protected void addToSelectList(List<SqlNode> list, Set<String> aliases,
      List<Map.Entry<String, RelDataType>> fieldList, SqlNode exp, SelectScope scope, boolean includeSystemVars) {
    if (exp.getKind() == SqlKind.IDENTIFIER) {
      SqlIdentifier sqlIdentifier = (SqlIdentifier) exp;
      if (sqlIdentifier.names.stream().anyMatch(s -> s.startsWith("$"))) {
        return;
      }
    }
    super.addToSelectList(list, aliases, fieldList, exp, scope, includeSystemVars);
  }

  @Override
  public void validateWith(SqlWith with, SqlValidatorScope scope) {
    Set<String> withNames = new HashSet<>();

    for (SqlNode sqlWithItem : with.withList) {
      for (String name : ((SqlWithItem) sqlWithItem).name.names) {
        if (withNames.contains(name)) {
          throw new RuntimeException("Duplicate alias in WITH: '" + name + "' at " + sqlWithItem.getParserPosition());
        }
        withNames.add(name);
      }
    }

    super.validateWith(with, scope);
  }
}
