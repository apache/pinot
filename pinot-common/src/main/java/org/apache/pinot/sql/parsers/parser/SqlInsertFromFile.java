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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;


/**
 * Calcite extension for creating an INSERT sql node from a File object.
 *
 * <p>Syntax: INSERT INTO [db_name.]table_name FROM [ FILE | ARCHIVE ] 'file_uri' [, [ FILE | ARCHIVE ] 'file_uri' ]
 */
public class SqlInsertFromFile extends SqlCall {
  private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("UDF", SqlKind.OTHER_DDL);
  private SqlIdentifier _dbName;
  private SqlIdentifier _tableName;
  private SqlNodeList _fileList;

  public SqlInsertFromFile(SqlParserPos pos, SqlIdentifier dbName, SqlIdentifier tableName, SqlNodeList fileList) {
    super(pos);
    _dbName = dbName;
    _tableName = tableName;
    _fileList = fileList;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    UnparseUtils u = new UnparseUtils(writer, leftPrec, rightPrec);
    u.keyword("INSERT", "INTO");
    if (_dbName != null) {
      u.node(_dbName).keyword(".");
    }
    u.node(_tableName);
    if (_fileList != null) {
      u.keyword("FROM").nodeList(_fileList);
    }
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Arrays.asList(_dbName, _tableName, _fileList);
  }
}
