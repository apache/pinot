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

import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlNode;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.sql.parsers.parser.SqlInsertFromFile;


/// Parses the DML statements that Pinot executes: `INSERT INTO ... FROM FILE` and `DELETE`.
public class DataManipulationStatementParser {
  private DataManipulationStatementParser() {
  }

  /// Parses a DML statement.
  ///
  /// @throws QueryException with [QueryErrorCode#SQL_PARSING] if it is not a DML statement that Pinot executes, e.g. an
  ///                        `UPDATE`, or not a valid one, e.g. a `DELETE` without a WHERE clause
  public static DataManipulationStatement parse(SqlNodeAndOptions sqlNodeAndOptions) {
    SqlNode sqlNode = sqlNodeAndOptions.getSqlNode();
    try {
      if (sqlNode instanceof SqlInsertFromFile) {
        return InsertIntoFile.parse(sqlNodeAndOptions);
      }
      if (sqlNode instanceof SqlDelete) {
        return DeleteStatement.parse(sqlNodeAndOptions);
      }
    } catch (QueryException e) {
      throw e;
    } catch (Exception e) {
      throw QueryErrorCode.SQL_PARSING.asException(e.getMessage() != null ? e.getMessage() : e.toString(), e);
    }
    throw QueryErrorCode.SQL_PARSING.asException("Unsupported DML SqlKind - " + sqlNode.getKind());
  }
}
