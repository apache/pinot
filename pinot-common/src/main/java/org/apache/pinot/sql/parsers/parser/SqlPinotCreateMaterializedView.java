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

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;


/// Pinot-native `CREATE MATERIALIZED VIEW` DDL statement.
///
/// Syntax:
/// ```
///   CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]name (
///     col TYPE [NULL | NOT NULL] [DEFAULT literal] [DIMENSION | METRIC | DATETIME ...],
///     ...
///   )
///   [ REFRESH [INTERVAL] EVERY <period> ]
///   [ PROPERTIES ( 'key' = 'value', ... ) ]
///   AS <query>
/// ```
///
/// The `REFRESH ... EVERY <period>` clause is optional. When present, it is compiled to a
/// per-table Quartz cron expression stored under `task.MaterializedViewTask.schedule`.
/// When omitted, no `schedule` entry is written and the MV task runs under the
/// cluster-wide MV task cron.
///
/// Semantic validation and compilation to `Schema` + `TableConfig` happen in `pinot-sql-ddl`.
public class SqlPinotCreateMaterializedView extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("CREATE_MATERIALIZED_VIEW", SqlKind.OTHER_DDL);

  private final SqlIdentifier _name;
  private final boolean _ifNotExists;
  private final SqlNodeList _columns;
  @Nullable
  private final SqlPinotRefreshClause _refresh;
  private final SqlNodeList _properties;
  private final SqlNode _query;

  public SqlPinotCreateMaterializedView(SqlParserPos pos, SqlIdentifier name, boolean ifNotExists,
      SqlNodeList columns, @Nullable SqlPinotRefreshClause refresh, @Nullable SqlNodeList properties,
      SqlNode query) {
    super(pos);
    _name = name;
    _ifNotExists = ifNotExists;
    _columns = columns;
    _refresh = refresh;
    _properties = properties == null ? SqlNodeList.EMPTY : properties;
    _query = query;
  }

  public SqlIdentifier getName() {
    return _name;
  }

  public boolean isIfNotExists() {
    return _ifNotExists;
  }

  public SqlNodeList getColumns() {
    return _columns;
  }

  /// Returns the parsed `REFRESH` clause, or `null` if the DDL omitted it (use cluster cron).
  @Nullable
  public SqlPinotRefreshClause getRefresh() {
    return _refresh;
  }

  public SqlNodeList getProperties() {
    return _properties;
  }

  public SqlNode getQuery() {
    return _query;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Arrays.asList(_name, _columns, _refresh, _properties, _query);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE MATERIALIZED VIEW");
    if (_ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    _name.unparse(writer, leftPrec, rightPrec);
    // Column list is optional: when the DDL omitted it (column inference path), unparse
    // must NOT emit empty `()` because the round-trip parser then parses `()` as an
    // explicit-but-empty list which the grammar rejects (PinotColumnList requires at
    // least one declaration).
    if (!_columns.isEmpty()) {
      SqlWriter.Frame columnFrame = writer.startList("(", ")");
      for (SqlNode column : _columns) {
        writer.sep(",");
        column.unparse(writer, 0, 0);
      }
      writer.endList(columnFrame);
    }
    if (_refresh != null) {
      _refresh.unparse(writer, 0, 0);
    }
    if (!_properties.isEmpty()) {
      writer.keyword("PROPERTIES");
      SqlWriter.Frame propFrame = writer.startList("(", ")");
      for (SqlNode property : _properties) {
        writer.sep(",");
        property.unparse(writer, 0, 0);
      }
      writer.endList(propFrame);
    }
    writer.keyword("AS");
    _query.unparse(writer, 0, 0);
  }
}
