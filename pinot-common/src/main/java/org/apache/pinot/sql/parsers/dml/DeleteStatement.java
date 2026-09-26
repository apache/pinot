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
package org.apache.pinot.sql.parsers.dml;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;

import static com.google.common.base.Preconditions.checkArgument;


/// A SQL `DELETE FROM <table> WHERE <predicate>` statement.
///
/// Pinot parses `DELETE` but does not delete rows itself: the default SQL executor answers that it is not supported,
/// and a deployment that can delete rows, e.g. by purging the matching rows from the segments with a minion task,
/// executes the parsed statement by overriding `SqlQueryExecutor#executeDelete`. The generic [#execute()] and
/// [#generateAdhocTaskConfig()] do not apply to it.
///
/// Before handing the statement to the executor, the broker and the controller resolve its table with
/// [#resolveTableName] and authorize the caller to delete rows from that table.
///
/// Its options are the `SET` statements, the legacy `OPTION(...)` suffix and the request `queryOptions` of the
/// statement (`SET` takes precedence). The `database` option only qualifies the table name, see [#resolveTableName].
/// Every other option, including query options such as `timeoutMs`, reaches the executor as written through
/// [#getOptions()], so that an option the executor relies on (e.g. a dry run) cannot be reclassified as a query option
/// and silently dropped.
///
/// Instances are immutable and thread-safe.
public class DeleteStatement implements DataManipulationStatement {
  public static final String NOT_SUPPORTED_MESSAGE =
      "DELETE is not supported by this Pinot cluster, it requires a SQL executor that implements row deletion";

  /// Serializes the WHERE clause into SQL that the Pinot parser reads back into the same expression. Combined with
  /// `quoteAllIdentifiers = false`, identifiers are only quoted (with double quotes) when they were quoted.
  private static final SqlDialect PINOT_SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT
      .withIdentifierQuoteString("\"")
      .withLiteralQuoteString("'")
      .withLiteralEscapedQuoteString("''")
      .withUnquotedCasing(Casing.UNCHANGED)
      .withQuotedCasing(Casing.UNCHANGED)
      .withCaseSensitive(true));

  /// Quotes the unquoted identifiers named like a SQL function without arguments, e.g. `user`, `pi` or `current_date`:
  /// Calcite unparses them as upper-cased keywords, while Pinot reads them as columns, so they would name another
  /// column. Mirrors the check of `SqlUtil.unparseSqlIdentifierSyntax`.
  private static final SqlShuttle KEYWORD_IDENTIFIER_QUOTER = new SqlShuttle() {
    @Override
    public SqlNode visit(SqlIdentifier identifier) {
      if (identifier.isSimple() && !identifier.getParserPosition().isQuoted()) {
        SqlOperator operator =
            SqlValidatorUtil.lookupSqlFunctionByID(SqlStdOperatorTable.instance(), identifier, null);
        if (operator != null && (operator.getSyntax() == SqlSyntax.FUNCTION_ID
            || operator.getSyntax() == SqlSyntax.FUNCTION_ID_CONSTANT)) {
          return new SqlIdentifier(identifier.names, null, SqlParserPos.QUOTED_ZERO,
              List.of(SqlParserPos.QUOTED_ZERO));
        }
      }
      return identifier;
    }
  };

  /// Canonical names (lower case, without underscores) of the functions that read another table than the one the
  /// statement deletes from, which the caller is not authorized to read.
  private static final Set<String> CROSS_TABLE_FUNCTIONS = Set.of("lookup", "insubquery", "inpartitionedsubquery");

  private final String _tableName;
  private final String _predicate;
  @Nullable
  private final String _database;
  private final Map<String, String> _options;
  private final boolean _resolved;

  /// @param tableName table name, see [#getTableName()]
  /// @param predicate WHERE clause, see [#getPredicate()]
  /// @param database `database` option of the statement, which qualifies an unqualified table name when the table is
  ///                 resolved, see [#resolveTableName]
  /// @param options other options of the statement, see [#getOptions()]
  public DeleteStatement(String tableName, String predicate, @Nullable String database, Map<String, String> options) {
    this(tableName, predicate, database, options, false);
  }

  private DeleteStatement(String tableName, String predicate, @Nullable String database, Map<String, String> options,
      boolean resolved) {
    _tableName = tableName;
    _predicate = predicate;
    _database = database;
    _options = Collections.unmodifiableMap(new HashMap<>(options));
    _resolved = resolved;
  }

  /// Parses a `DELETE` statement.
  ///
  /// @throws IllegalArgumentException if the statement is not a supported `DELETE`: it must have a WHERE clause, no
  ///                                  table alias, a WHERE clause that Pinot can parse as an expression and that does
  ///                                  not read another table (e.g. with `lookUp` or `IN_SUBQUERY`), and set the
  ///                                  database with the `database` option only
  public static DeleteStatement parse(SqlNodeAndOptions sqlNodeAndOptions) {
    SqlNode sqlNode = sqlNodeAndOptions.getSqlNode();
    checkArgument(sqlNode instanceof SqlDelete, "Not a DELETE statement: %s", sqlNode.getKind());
    SqlDelete sqlDelete = (SqlDelete) sqlNode;
    String tableName = getTableName(sqlDelete.getTargetTable());
    checkArgument(sqlDelete.getAlias() == null,
        "DELETE does not support a table alias, reference the columns directly");
    SqlNode condition = sqlDelete.getCondition();
    checkArgument(condition != null,
        "DELETE requires a WHERE clause; delete the table segments to remove all of its rows");
    String predicate = toPinotSql(condition);

    String database = null;
    Map<String, String> options = new HashMap<>();
    for (Map.Entry<String, String> option : sqlNodeAndOptions.getOptions().entrySet()) {
      String key = option.getKey();
      if (key.equals(CommonConstants.DATABASE)) {
        database = option.getValue();
      } else if (key.equalsIgnoreCase(CommonConstants.DATABASE)) {
        // Queries ignore it, as they only read the `database` option: fail rather than delete from another table
        throw new IllegalArgumentException(
            "Unsupported option: " + key + ", set the database with the '" + CommonConstants.DATABASE + "' option");
      } else {
        options.put(key, option.getValue());
      }
    }
    return new DeleteStatement(tableName, predicate, database, options);
  }

  private static String getTableName(SqlNode targetTable) {
    // Table hints and EXTEND clauses parse into other node types
    checkArgument(targetTable instanceof SqlIdentifier, "DELETE only supports a plain table name, got: %s",
        targetTable);
    // A quoted name part may contain a dot, which splits it like the table name of a query
    String tableName = String.join(".", ((SqlIdentifier) targetTable).names);
    checkArgument(DatabaseUtils.splitTableName(tableName).length <= 2,
        "Invalid table name: %s, expected [database.]table", tableName);
    return tableName;
  }

  /// Serializes a WHERE clause back into SQL, and verifies that Pinot parses it into the same expression.
  private static String toPinotSql(SqlNode condition) {
    Expression expression;
    Expression serializedExpression;
    String predicate;
    try {
      expression = CalciteSqlParser.compileToExpression(condition);
      predicate = condition.accept(KEYWORD_IDENTIFIER_QUOTER)
          .toSqlString(config -> config.withDialect(PINOT_SQL_DIALECT)
              .withQuoteAllIdentifiers(false)
              .withIndentation(0))
          .getSql();
      serializedExpression = CalciteSqlParser.compileToExpression(predicate);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unsupported WHERE clause in DELETE: " + condition, e);
    }
    // Fail rather than hand over a predicate that selects other rows than the statement
    checkArgument(serializedExpression.equals(expression),
        "Unsupported WHERE clause in DELETE, it cannot be serialized back into the same expression: %s", condition);
    checkNoCrossTableFunction(expression);
    return predicate;
  }

  /// Rejects the functions that read another table: the caller is only authorized for the table it deletes from.
  private static void checkNoCrossTableFunction(Expression expression) {
    if (expression.getType() != ExpressionType.FUNCTION) {
      return;
    }
    Function function = expression.getFunctionCall();
    String functionName = function.getOperator().replace("_", "").toLowerCase(Locale.ROOT);
    checkArgument(!CROSS_TABLE_FUNCTIONS.contains(functionName),
        "Unsupported WHERE clause in DELETE, %s reads another table", function.getOperator());
    if (function.getOperands() != null) {
      for (Expression operand : function.getOperands()) {
        checkNoCrossTableFunction(operand);
      }
    }
  }

  /// Returns the statement with its table name resolved: qualified with the database of the request, and in the case
  /// the table is defined with (table names are case-insensitive by default). The broker and the controller authorize
  /// the caller for the resolved table name, and hand the resolved statement to the executor.
  ///
  /// The database of the request is the `database` request header, else the `database` option of the statement, as
  /// for a multi-stage query (see `DatabaseUtils#extractDatabaseFromQueryRequest`). They must match when both are set,
  /// and the database of a `database.table` name must match them.
  ///
  /// @param databaseHeader value of the `database` request header, if any
  /// @param tableCache tables of the cluster, to resolve the case of the table name. A table name it does not know
  ///                   keeps the case of the statement.
  /// @throws QueryException with [QueryErrorCode#QUERY_VALIDATION] if the `database` header, the `database` option
  ///                        and the database of a `database.table` name do not match (a
  ///                        `DatabaseConflictException`), or if the table is a logical table, which `DELETE` does
  ///                        not support
  public DeleteStatement resolveTableName(@Nullable String databaseHeader, TableCache tableCache)
      throws QueryException {
    String database = DatabaseUtils.extractDatabaseFromQueryRequest(_database, databaseHeader);
    String tableName;
    try {
      tableName = DatabaseUtils.translateTableName(_tableName, database, tableCache.isIgnoreCase());
    } catch (IllegalArgumentException e) {
      throw QueryErrorCode.QUERY_VALIDATION.asException("Invalid table name in DELETE: " + e.getMessage(), e);
    }
    String actualTableName = tableCache.getActualTableName(tableName);
    if (actualTableName == null && tableCache.getActualLogicalTableName(tableName) != null) {
      // Deleting from a logical table would delete from physical tables the caller is not authorized for
      throw QueryErrorCode.QUERY_VALIDATION.asException("DELETE does not support logical tables: " + tableName);
    }
    return new DeleteStatement(actualTableName != null ? actualTableName : tableName, _predicate, null, _options, true);
  }

  /// Whether the table name is resolved with [#resolveTableName]. The executor only executes a resolved statement.
  public boolean isResolved() {
    return _resolved;
  }

  /// Table name: as written in the statement (`table` or `database.table`, optionally with a type suffix), or, once
  /// resolved with [#resolveTableName], qualified with its database and in the case the table is defined with.
  ///
  /// The statement handed to the executor is resolved, and the caller is authorized to delete rows from this table.
  /// Executors delete from this exact table: resolving the name again, e.g. case-insensitively, could delete from
  /// another table than the one the caller is authorized for.
  public String getTableName() {
    return _tableName;
  }

  /// WHERE clause without the `WHERE` keyword, in Pinot SQL that [CalciteSqlParser#compileToExpression(String)]
  /// parses into the same expression as the statement. Identifiers are quoted where the statement quoted them, and
  /// where needed to keep their name.
  ///
  /// It is a standalone expression, e.g. `a = 1 OR b = 2`: wrap it in parentheses to combine it with other conditions.
  /// Pinot parses it as an expression but does not validate it as a filter of the table, e.g. that its columns exist
  /// or that it has no aggregation, so executors must validate it before deleting rows.
  public String getPredicate() {
    return _predicate;
  }

  /// Options of the statement other than the database, with the keys as written: the `SET` statements, the legacy
  /// `OPTION(...)` suffix and the request `queryOptions`, `SET` taking precedence over request options with the same
  /// key. Query options such as `timeoutMs` or `useMultistageEngine` are included: executors read the options they
  /// support and ignore the others.
  ///
  /// Keys are case-sensitive here, so the same option may appear with different cases, e.g. `dryRun` set with `SET`
  /// and `dryrun` in the request: executors that read options case-insensitively should reject such duplicates rather
  /// than pick one.
  public Map<String, String> getOptions() {
    return _options;
  }

  @Override
  public ExecutionType getExecutionType() {
    return ExecutionType.HTTP;
  }

  @Override
  public AdhocTaskConfig generateAdhocTaskConfig() {
    throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
  }

  @Override
  public List<Object[]> execute() {
    throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
  }

  /// The result depends on the executor that implements `DELETE`.
  @Override
  public DataSchema getResultSchema() {
    throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
  }
}
