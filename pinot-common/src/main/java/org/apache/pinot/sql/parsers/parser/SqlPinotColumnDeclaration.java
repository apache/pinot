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
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;


/// Single column declaration inside the column list of a Pinot [SqlPinotCreateTable].
///
/// Grammar:
/// ```
///   col_name DATA_TYPE [NULL | NOT NULL] [DEFAULT literal]
///     [ DIMENSION | METRIC | DATETIME FORMAT 'fmt' GRANULARITY 'gran' ]
/// ```
///
/// The `role` field is one of "DIMENSION", "METRIC", "DATETIME", or `null`
/// (unspecified, defer inference to the compiler). When `role` is "DATETIME", the
/// `dateTimeFormat` and `dateTimeGranularity` string literals are required.
/// When `role` is "DIMENSION", the optional `ARRAY` suffix marks the column as
/// multi-value (Pinot MV dimension).
///
/// **DEFAULT semantics**: the `DEFAULT literal` clause maps to Pinot's
/// `FieldSpec.defaultNullValue`, NOT to standard SQL's "value substituted on insert when
/// column is omitted". Pinot's defaultNullValue is applied at ingestion time when the source
/// record contains a null/missing value for the column. Users coming from standard SQL should
/// not expect this clause to interact with INSERT statements.
///
/// This class is not thread-safe; instances should not be mutated after construction.
public class SqlPinotColumnDeclaration extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL);

  private final SqlIdentifier _columnName;
  private final SqlDataTypeSpec _dataType;
  private final boolean _nullable;
  private final SqlNode _defaultValue;
  private final String _role;
  private final SqlLiteral _dateTimeFormat;
  private final SqlLiteral _dateTimeGranularity;
  private final boolean _multiValue;

  public SqlPinotColumnDeclaration(SqlParserPos pos, SqlIdentifier columnName, SqlDataTypeSpec dataType,
      boolean nullable, @Nullable SqlNode defaultValue, @Nullable String role,
      @Nullable SqlLiteral dateTimeFormat, @Nullable SqlLiteral dateTimeGranularity) {
    this(pos, columnName, dataType, nullable, defaultValue, role, dateTimeFormat, dateTimeGranularity, false);
  }

  public SqlPinotColumnDeclaration(SqlParserPos pos, SqlIdentifier columnName, SqlDataTypeSpec dataType,
      boolean nullable, @Nullable SqlNode defaultValue, @Nullable String role,
      @Nullable SqlLiteral dateTimeFormat, @Nullable SqlLiteral dateTimeGranularity, boolean multiValue) {
    super(pos);
    _columnName = columnName;
    _dataType = dataType;
    _nullable = nullable;
    _defaultValue = defaultValue;
    _role = role;
    _dateTimeFormat = dateTimeFormat;
    _dateTimeGranularity = dateTimeGranularity;
    _multiValue = multiValue;
  }

  public SqlIdentifier getColumnName() {
    return _columnName;
  }

  public SqlDataTypeSpec getDataType() {
    return _dataType;
  }

  public boolean isNullable() {
    return _nullable;
  }

  @Nullable
  public SqlNode getDefaultValue() {
    return _defaultValue;
  }

  /// @return one of "DIMENSION", "METRIC", "DATETIME", or `null` when unspecified.
  @Nullable
  public String getRole() {
    return _role;
  }

  @Nullable
  public SqlLiteral getDateTimeFormat() {
    return _dateTimeFormat;
  }

  @Nullable
  public SqlLiteral getDateTimeGranularity() {
    return _dateTimeGranularity;
  }

  /// Returns true if this is a multi-value dimension (declared with `DIMENSION ARRAY`).
  public boolean isMultiValue() {
    return _multiValue;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Arrays.asList(_columnName, _dataType, _defaultValue, _dateTimeFormat, _dateTimeGranularity);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    _columnName.unparse(writer, 0, 0);
    _dataType.unparse(writer, 0, 0);
    if (!_nullable) {
      writer.keyword("NOT NULL");
    }
    if (_defaultValue != null) {
      writer.keyword("DEFAULT");
      _defaultValue.unparse(writer, 0, 0);
    }
    if (_role != null) {
      if ("DATETIME".equals(_role)) {
        writer.keyword("DATETIME");
        writer.keyword("FORMAT");
        _dateTimeFormat.unparse(writer, 0, 0);
        writer.keyword("GRANULARITY");
        _dateTimeGranularity.unparse(writer, 0, 0);
      } else {
        writer.keyword(_role);
        if (_multiValue) {
          writer.keyword("ARRAY");
        }
      }
    }
  }
}
