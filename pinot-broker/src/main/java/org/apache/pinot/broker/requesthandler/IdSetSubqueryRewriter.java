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
package org.apache.pinot.broker.requesthandler;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Litmus;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


/// Runs the IdSet subqueries of a multi-stage query before the query is compiled, and puts the IdSets they return in
/// their place, so that the planner and the servers only see `IN_ID_SET` calls on literals:
/// - `IN_SUBQUERY(expr, 'subquery')` becomes `IN_ID_SET(expr, 'idSet')`, anywhere in the query.
/// - `IN_ID_SET(expr, (subquery))` becomes `IN_ID_SET(expr, 'idSet')` when the subquery validates on its own, i.e.
///   when it is not correlated, and does not name a WITH item of the query. Otherwise the subquery stays, and runs as
///   part of the query.
///
/// A subquery that appears more than once runs once. To explain a query, no subquery runs: the empty IdSet takes the
/// place of each result.
///
/// The rewrite changes the parsed query in place, so it is not thread-safe.
final class IdSetSubqueryRewriter {
  private static final String IN_SUBQUERY = "insubquery";
  private static final String IN_ID_SET = "inidset";

  static final String EMPTY_ID_SET;

  static {
    try {
      EMPTY_ID_SET = IdSets.emptyIdSet().toBase64String();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private final String _sql;
  private final boolean _explain;
  private final SubqueryRunner _runner;
  // The IdSets of the subqueries, by subquery, in the order the subqueries ran
  private final Map<String, String> _idSets = new LinkedHashMap<>();

  private IdSetSubqueryRewriter(String sql, boolean explain, SubqueryRunner runner) {
    _sql = sql;
    _explain = explain;
    _runner = runner;
  }

  /// Validates and runs the subqueries for [IdSetSubqueryRewriter].
  interface SubqueryRunner {
    /// Returns whether the subquery validates on its own, i.e. whether it does not refer to the query it is in.
    boolean validates(String subquery);

    /// Runs the subquery and returns the serialized IdSet it returns, or `null` if it returns no rows or NULL.
    @Nullable
    String run(String subquery);
  }

  /// Runs the IdSet subqueries of the query and replaces them with the IdSets they return, or with the empty IdSet to
  /// explain the query. Returns the subqueries that were replaced, each once, in the order they ran.
  ///
  /// @param sql the text of the query, which scalar subqueries are read from
  /// @param query the parsed query, without the EXPLAIN
  static List<String> rewrite(String sql, SqlNode query, boolean explain, SubqueryRunner runner) {
    IdSetSubqueryRewriter rewriter = new IdSetSubqueryRewriter(sql, explain, runner);
    rewriter.rewrite(query, Set.of());
    return new ArrayList<>(rewriter._idSets.keySet());
  }

  /// @param withNames the names of the WITH items in scope, in lower case
  private void rewrite(SqlNode node, Set<String> withNames) {
    IdSetCallFinder finder = new IdSetCallFinder(withNames);
    node.accept(finder);
    for (IdSetCall idSetCall : finder._calls) {
      SqlBasicCall call = idSetCall._call;
      if (IN_SUBQUERY.equals(getFunctionName(call))) {
        String subquery = getSubquery(call);
        call.setOperator(new SqlUnresolvedFunction(new SqlIdentifier("IN_ID_SET", call.getParserPosition()), null,
            null, null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION));
        setIdSet(call, getIdSet(subquery));
      } else {
        SqlNode subqueryNode = call.operand(1);
        String subquery = getText(subqueryNode);
        // A subquery that names a WITH item cannot run on its own: it would read the table that the item hides, if any
        boolean runsFirst = subquery != null && !hasAnyName(subqueryNode, idSetCall._withNames)
            && (_idSets.containsKey(subquery) || _runner.validates(subquery));
        if (runsFirst) {
          setIdSet(call, getIdSet(subquery));
        } else {
          // The planner runs the subquery as part of the query, but the IdSet subqueries in it still run first
          rewrite(subqueryNode, idSetCall._withNames);
        }
      }
    }
  }

  private String getIdSet(String subquery) {
    return _idSets.computeIfAbsent(subquery, key -> {
      String idSet = _explain ? null : _runner.run(key);
      return idSet != null ? idSet : EMPTY_ID_SET;
    });
  }

  private static void setIdSet(SqlBasicCall call, String idSet) {
    call.setOperand(1, SqlLiteral.createCharString(idSet, call.operand(1).getParserPosition()));
  }

  @Nullable
  private static String getFunctionName(SqlCall call) {
    return call.getOperator() instanceof SqlFunction
        ? FunctionRegistry.canonicalize(call.getOperator().getName())
        : null;
  }

  private static String getSubquery(SqlBasicCall call) {
    if (call.operandCount() == 2) {
      SqlNode subquery = call.operand(1);
      if (subquery.getKind() == SqlKind.LITERAL_CHAIN) {
        subquery = SqlLiteral.unchain(subquery);
      }
      if (subquery instanceof SqlCharStringLiteral) {
        return ((SqlCharStringLiteral) subquery).getValueAs(String.class);
      }
    }
    throw QueryErrorCode.QUERY_VALIDATION.asException(
        "IN_SUBQUERY takes an expression and a string literal with the subquery, got: " + call);
  }

  /// Returns the text of the subquery in the query, with its parentheses, or `null` if the text does not parse to the
  /// same subquery, e.g. when the parser read a different text than the given one.
  @Nullable
  private String getText(SqlNode subquery) {
    SqlParserPos pos = subquery.getParserPosition();
    try {
      String text = _sql.substring(SqlParserUtil.lineColToIndex(_sql, pos.getLineNum(), pos.getColumnNum()),
          SqlParserUtil.lineColToIndex(_sql, pos.getEndLineNum(), pos.getEndColumnNum()) + 1);
      SqlNode parsed = CalciteSqlParser.compileToSqlNodeAndOptions(text).getSqlNode();
      return parsed.equalsDeep(subquery, Litmus.IGNORE) ? text : null;
    } catch (RuntimeException e) {
      return null;
    }
  }

  /// Returns whether the node has an identifier with one of the given names, ignoring the case.
  private static boolean hasAnyName(SqlNode node, Set<String> names) {
    if (names.isEmpty()) {
      return false;
    }
    NameFinder nameFinder = new NameFinder(names);
    node.accept(nameFinder);
    return nameFinder._found;
  }

  /// An IdSet subquery, and the names of the WITH items in scope where it appears.
  private static class IdSetCall {
    final SqlBasicCall _call;
    final Set<String> _withNames;

    IdSetCall(SqlBasicCall call, Set<String> withNames) {
      _call = call;
      _withNames = withNames;
    }
  }

  /// Finds the IdSet subqueries of a query, except those in other IdSet subqueries.
  private static class IdSetCallFinder extends SqlBasicVisitor<Void> {
    final List<IdSetCall> _calls = new ArrayList<>();
    // The names of the WITH items in scope, in lower case. Each WITH gets a new set, so the calls can keep theirs.
    Set<String> _withNames;

    IdSetCallFinder(Set<String> withNames) {
      _withNames = withNames;
    }

    @Override
    public Void visit(SqlCall call) {
      // The ORDER BY of a WITH query parses outside the WITH, but the validator puts it in the scope of the WITH
      SqlNode with = call instanceof SqlOrderBy ? ((SqlOrderBy) call).query : call;
      if (with instanceof SqlWith) {
        Set<String> withNames = _withNames;
        _withNames = new HashSet<>(withNames);
        for (SqlNode withItem : ((SqlWith) with).withList) {
          _withNames.add(((SqlWithItem) withItem).name.getSimple().toLowerCase(Locale.ROOT));
        }
        super.visit(call);
        _withNames = withNames;
        return null;
      }
      if (call instanceof SqlBasicCall && call.operandCount() > 0) {
        String functionName = getFunctionName(call);
        if (IN_SUBQUERY.equals(functionName) || (IN_ID_SET.equals(functionName) && call.operandCount() == 2
            && call.operand(1).getKind().belongsTo(SqlKind.QUERY))) {
          _calls.add(new IdSetCall((SqlBasicCall) call, _withNames));
          // The tested expression can hold more of them
          call.operand(0).accept(this);
          return null;
        }
      }
      return super.visit(call);
    }
  }

  /// Finds whether a node has an identifier with one of the given names, ignoring the case.
  private static class NameFinder extends SqlBasicVisitor<Void> {
    final Set<String> _names;
    boolean _found;

    NameFinder(Set<String> names) {
      _names = names;
    }

    @Override
    public Void visit(SqlIdentifier id) {
      for (String name : id.names) {
        if (_names.contains(name.toLowerCase(Locale.ROOT))) {
          _found = true;
        }
      }
      return null;
    }
  }
}
