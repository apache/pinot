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
package org.apache.pinot.common.utils;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class QueryLogSystemTableUtilsTest {

  private static SqlNode parse(String sql)
      throws SqlParseException {
    return CalciteSqlParser.compileToSqlNodeAndOptions(sql).getSqlNode();
  }

  @Test
  public void testRecognizesQueryLogTable()
      throws Exception {
    assertTrue(QueryLogSystemTableUtils.isQueryLogSystemTableQuery(parse("SELECT * FROM system.query_log")));
    assertTrue(QueryLogSystemTableUtils.isQueryLogSystemTableQuery(parse("SELECT * FROM sys.query_log")));
    assertTrue(QueryLogSystemTableUtils.isQueryLogSystemTableQuery(parse(
        "SELECT * FROM default.system.query_log ORDER BY timestampMs DESC")));
  }

  @Test
  public void testRejectsOtherTables()
      throws Exception {
    assertFalse(QueryLogSystemTableUtils.isQueryLogSystemTableQuery(parse("SELECT * FROM myTable")));
    assertFalse(QueryLogSystemTableUtils.isQueryLogSystemTableQuery(parse("SELECT * FROM sys.other_table")));
  }
}
