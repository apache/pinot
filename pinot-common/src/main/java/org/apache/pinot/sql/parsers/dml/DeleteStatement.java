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
import java.util.Map;
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
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
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
/// Its options are the `SET` statements, the legacy `OPTION(...)` suffix and the request `queryOptions` of the
/// statement (`SET` takes precedence). The `database` option is lifted into [#getDatabase()]; every other option,
/// including query options such as `timeoutMs`, reaches the executor as written through [#getOptions()], so that an
/// option the executor relies on (e.g. a dry run) cannot be reclassified as a query option and silently dropped.
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

  private final String _tableName;
  private final String _predicate;
  @Nullable
  private final String _database;
  private final Map<String, String> _options;

  public DeleteStatement(String tableName, String predicate, @Nullable String database, Map<String, String> options) {
    _tableName = tableName;
    _predicate = predicate;
    _database = database;
    _options = Collections.unmodifiableMap(new HashMap<>(options));
  }

  /// Parses a `DELETE` statement.
  ///
  /// @throws IllegalArgumentException if the statement is not a supported `DELETE`: it must have a WHERE clause, no
  ///                                  table alias, a WHERE clause that Pinot can parse as an expression, and set the
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
    SqlIdentifier identifier = (SqlIdentifier) targetTable;
    checkArgument(identifier.names.size() <= 2, "Invalid table name: %s, expected [database.]table", identifier);
    return String.join(".", identifier.names);
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
    return predicate;
  }

  /// Table name as written in the statement: `table` or `database.table`, optionally with a type suffix.
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

  /// Database of an unqualified table name set with the `database` option, e.g. `SET database = '...'`, if any.
  ///
  /// Queries also read the database from the `database` request header (header names are case-insensitive), which
  /// takes precedence and must match the option when both are set (see
  /// `DatabaseUtils#extractDatabaseFromQueryRequest`), and a `database.table` name must match the resolved database
  /// (see `DatabaseUtils#translateTableName`): executors resolve it the same way with the headers they are given.
  @Nullable
  public String getDatabase() {
    return _database;
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
