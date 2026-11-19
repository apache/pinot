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
package org.apache.pinot.sql.parsers.parser;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Helper class to extract table names from Calcite SqlNode tree.
public class TableNameExtractor {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableNameExtractor.class);

  /// Returns the name of all the tables used in a sql query.
  ///
  /// @param query The SQL query string to analyze
  /// @return name of all the tables used in a sql query, or null if parsing fails
  @Nullable
  public static String[] resolveTableName(String query) {
    try {
      SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(query);
      Set<String> tableNames = extractTableNamesFromPinotQuery(sqlNodeAndOptions.getSqlNode());
      if (tableNames != null) {
        return tableNames.toArray(new String[0]);
      }
      return null;
    } catch (Exception e) {
      throw new RuntimeException("Cannot parse table name from query: " + query, e);
    }
  }

  /// Visits referenced table identifiers in an already parsed query, preserving their name components and spelling.
  /// Only referenced CTE definitions are visited, using the scope in which each definition was declared.
  /// The visitor may receive the same table more than once. This syntactic walk does not validate or plan the query.
  public static void visitReferencedTableIdentifiers(SqlNode query, Consumer<SqlIdentifier> visitor) {
    new ReferencedTableVisitor(visitor).visit(query, null, false);
  }

  /// Extracts table names from a multi-stage query using Calcite SQL AST traversal.
  ///
  /// @param sqlNode The root SqlNode of the parsed query
  /// @return Set of table names found in the query
  private static Set<String> extractTableNamesFromPinotQuery(SqlNode sqlNode) {
    TableNameExtractor extractor = new TableNameExtractor();
    extractor.extractTableNames(sqlNode);
    return extractor.getTableNames();
  }

  private final Set<String> _tableNames = new HashSet<>();
  private final Set<String> _cteNames = new HashSet<>();
  private boolean _inFromClause = false;

  /// Returns the set of table names extracted from the SQL node tree.
  ///
  /// This method should be called after [#extractTableNames(SqlNode)] has been invoked
  /// to populate the set of table names.
  ///
  /// @return Set of table names found in the SQL node tree
  public Set<String> getTableNames() {
    return _tableNames;
  }

  public void extractTableNames(SqlNode node) {
    assert node != null;
    if (node instanceof SqlWith) {
      visitWith((SqlWith) node);
    } else if (node instanceof SqlOrderBy) {
      visitOrderBy((SqlOrderBy) node);
    } else if (node instanceof SqlWithItem) {
      visitWithItem((SqlWithItem) node);
    } else if (node instanceof SqlSelect) {
      visitSelect((SqlSelect) node);
    } else if (node instanceof SqlJoin) {
      visitJoin((SqlJoin) node);
    } else if (node instanceof SqlBasicCall) {
      visitBasicCall((SqlBasicCall) node);
    } else if (node instanceof SqlIdentifier) {
      visitIdentifier((SqlIdentifier) node);
    } else if (node instanceof SqlNodeList) {
      visitNodeList((SqlNodeList) node);
    }
  }

  private void visitWith(SqlWith with) {
    // Visit the WITH list (CTE definitions)
    if (with.withList != null) {
      visitNodeList(with.withList);
    }
    // Visit the main query body
    if (with.body != null) {
      extractTableNames(with.body);
    }
  }

  private void visitOrderBy(SqlOrderBy orderBy) {
    // Visit the main query - this is the most important part
    if (orderBy.query != null) {
      extractTableNames(orderBy.query);
    }
    // Visit ORDER BY expressions for potential subqueries
    if (orderBy.orderList != null) {
      // Don't set inFromClause=true for ORDER BY expressions
      // as they typically contain column references, not table names
      visitNodeList(orderBy.orderList);
    }
    // Visit OFFSET clause if it contains subqueries (rare but possible)
    if (orderBy.offset != null) {
      extractTableNames(orderBy.offset);
    }
    // Visit FETCH/LIMIT clause if it contains subqueries (rare but possible)
    if (orderBy.fetch != null) {
      extractTableNames(orderBy.fetch);
    }
  }

  private void visitWithItem(SqlWithItem withItem) {
    // Extract table names from the CTE query definition before adding the CTE alias to
    // filter. This handles cases where the CTE has the same name as the original table.
    // Otherwise the table won't be added to the result.
    if (withItem.query != null) {
      extractTableNames(withItem.query);
    }
    if (withItem.name != null) {
      String cteName = withItem.name.getSimple();
      _cteNames.add(cteName);
    }
  }

  private void visitSelect(SqlSelect select) {
    // Visit FROM clause - this is where we expect to find table names
    if (select.getFrom() != null) {
      _inFromClause = true;
      extractTableNames(select.getFrom());
      _inFromClause = false;
    }
    // Visit other clauses for subqueries
    if (select.getWhere() != null) {
      extractTableNames(select.getWhere());
    }
    if (select.getGroup() != null) {
      visitNodeList(select.getGroup());
    }
    if (select.getHaving() != null) {
      extractTableNames(select.getHaving());
    }
    // QUALIFY is rejected by the single-stage engine but legal for the multi-stage one, which is what this extractor
    // runs on, so a subquery hiding in it still has to be walked.
    if (select.getQualify() != null) {
      extractTableNames(select.getQualify());
    }
    if (select.getOrderList() != null) {
      visitNodeList(select.getOrderList());
    }
    if (select.getSelectList() != null) {
      visitNodeList(select.getSelectList());
    }
  }

  private void visitJoin(SqlJoin join) {
    // Visit both sides of the join - ensure they're processed as FROM clause items
    boolean wasInFromClause = _inFromClause;
    if (join.getLeft() != null) {
      _inFromClause = true;
      extractTableNames(join.getLeft());
    }
    if (join.getRight() != null) {
      _inFromClause = true;
      extractTableNames(join.getRight());
    }
    // Visit join condition but not as part of FROM clause context
    // This handles potential subqueries in join conditions while avoiding
    // incorrectly extracting column references as table names
    if (join.getCondition() != null) {
      _inFromClause = false;
      extractTableNames(join.getCondition());
    }
    // Restore original context
    _inFromClause = wasInFromClause;
  }

  private void visitBasicCall(SqlBasicCall call) {
    if (call.getKind() == SqlKind.AS) {
      // Handle table aliases like "tableA AS a"
      // For AS operations, the first operand is the actual table name
      if (!call.getOperandList().isEmpty() && call.getOperandList().get(0) != null) {
        extractTableNames(call.getOperandList().get(0));
      }
    } else if (call.getKind() == SqlKind.WITH) {
      // Handle CTE (Common Table Expression)
      visitWithClause(call);
    } else if (call.getKind() == SqlKind.VALUES) {
      // Handle VALUES clause - usually doesn't contain table references
      // Skip this to avoid false positives
    } else {
      // For other basic calls, visit all operands
      for (SqlNode operand : call.getOperandList()) {
        if (operand != null) {
          extractTableNames(operand);
        }
      }
    }
  }

  private void visitIdentifier(SqlIdentifier identifier) {
    // Only extract table names when we're in a FROM clause
    if (_inFromClause && !identifier.names.isEmpty()) {
      String tableName = identifier.names.get(identifier.names.size() - 1);
      // Filter out system identifiers and CTE names
      if (!tableName.startsWith("$") && !_cteNames.contains(tableName)) {
        _tableNames.add(tableName);
      }
    }
  }

  /// Visit a SqlNodeList by visiting each node in the list.
  private void visitNodeList(SqlNodeList nodeList) {
    if (nodeList != null) {
      for (SqlNode node : nodeList) {
        if (node != null) {
          extractTableNames(node);
        }
      }
    }
  }

  /// Handle WITH clause (CTE - Common Table Expression).
  private void visitWithClause(SqlNode node) {
    try {
      // WITH clause typically has operands: [with_list, query]
      if (node instanceof SqlBasicCall) {
        SqlBasicCall withCall = (SqlBasicCall) node;
        for (SqlNode operand : withCall.getOperandList()) {
          if (operand != null) {
            extractTableNames(operand);
          }
        }
      }
    } catch (Exception e) {
      // Fallback to generic operand handling
      visitNodeOperands(node);
    }
  }

  /// Generic method to visit node operands when specific handling is not available.
  private void visitNodeOperands(SqlNode node) {
    try {
      // Try to access operands through common interface
      if (node instanceof SqlBasicCall) {
        SqlBasicCall call = (SqlBasicCall) node;
        for (SqlNode operand : call.getOperandList()) {
          if (operand != null) {
            extractTableNames(operand);
          }
        }
      }
    } catch (Exception e) {
      throw new SqlCompilationException("Exception encountered while visiting node operands: " + node, e);
    }
  }

  /// A per-query visitor that keeps table identifiers intact and expands each referenced CTE once.
  private static final class ReferencedTableVisitor {
    private static final Set<SqlKind> RELATION_WRAPPERS = EnumSet.of(SqlKind.AS, SqlKind.EXTEND, SqlKind.TABLE_REF,
        SqlKind.TABLESAMPLE, SqlKind.LATERAL, SqlKind.SNAPSHOT, SqlKind.PIVOT, SqlKind.UNPIVOT,
        SqlKind.MATCH_RECOGNIZE);

    private final Consumer<SqlIdentifier> _visitor;
    private final Set<CteBinding> _expanded = new HashSet<>();

    ReferencedTableVisitor(Consumer<SqlIdentifier> visitor) {
      _visitor = visitor;
    }

    void visit(@Nullable SqlNode node, @Nullable CteBinding scope, boolean relation) {
      if (node instanceof SqlIdentifier) {
        if (relation) {
          visitRelation((SqlIdentifier) node, scope);
        }
      } else if (node instanceof SqlWith) {
        SqlWith with = (SqlWith) node;
        for (SqlNode definition : with.withList) {
          SqlWithItem item = (SqlWithItem) definition;
          scope = new CteBinding(item.name.getSimple(), item, scope);
        }
        visit(with.body, scope, false);
      } else if (node instanceof SqlSelect) {
        SqlSelect select = (SqlSelect) node;
        for (SqlNode operand : select.getOperandList()) {
          visit(operand, scope, operand == select.getFrom());
        }
      } else if (node instanceof SqlJoin) {
        SqlJoin join = (SqlJoin) node;
        visit(join.getLeft(), scope, true);
        visit(join.getRight(), scope, true);
        visit(join.getCondition(), scope, false);
      } else if (node instanceof SqlCall) {
        visitCall((SqlCall) node, scope, relation);
      } else if (node instanceof SqlNodeList) {
        for (SqlNode item : (SqlNodeList) node) {
          visit(item, scope, false);
        }
      }
    }

    private void visitRelation(SqlIdentifier identifier, @Nullable CteBinding scope) {
      CteBinding binding = scope != null && identifier.isSimple() ? scope.lookup(identifier.getSimple()) : null;
      if (binding == null) {
        _visitor.accept(identifier);
      } else if (_expanded.add(binding)) {
        SqlWithItem item = binding._item;
        if (item.recursive.booleanValue() && item.query.getKind() == SqlKind.UNION) {
          // Only the recursive term can see its own CTE name. The seed can still refer to a physical table.
          SqlCall union = (SqlCall) item.query;
          visit(union.operand(0), binding._parent, false);
          visit(union.operand(1), binding, false);
        } else {
          visit(item.query, binding._parent, false);
        }
      }
    }

    private void visitCall(SqlCall call, @Nullable CteBinding scope, boolean relation) {
      SqlKind kind = call.getKind();
      boolean wrapsRelation = kind == SqlKind.EXPLICIT_TABLE || relation && RELATION_WRAPPERS.contains(kind);
      List<SqlNode> operands = call.getOperandList();
      for (int i = 0; i < operands.size(); i++) {
        visit(operands.get(i), scope, wrapsRelation && i == 0);
      }
    }
  }

  /// A CTE binding shares its preceding and outer scopes.
  private static final class CteBinding {
    private final String _name;
    private final SqlWithItem _item;
    @Nullable
    private final CteBinding _parent;

    CteBinding(String name, SqlWithItem item, @Nullable CteBinding parent) {
      _name = name;
      _item = item;
      _parent = parent;
    }

    @Nullable
    CteBinding lookup(String alias) {
      for (CteBinding scope = this; scope != null; scope = scope._parent) {
        if (scope._name.equals(alias)) {
          return scope;
        }
      }
      return null;
    }
  }
}
