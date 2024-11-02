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
package org.apache.pinot.sql.parsers;

import org.testng.annotations.Test;

import static org.apache.pinot.sql.parsers.CalciteSqlParser.CALCITE_SQL_PARSER_IDENTIFIER_MAX_LENGTH;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.fail;

public class CalciteSqlParserTest {
  private static final String SINGLE_CHAR = "a";

  @Test
  public void testIdentifierLength() {
    String tableName = extendIdentifierToMaxLength("exampleTable");
    String columnName = extendIdentifierToMaxLength("exampleColumn");

    try {
      final String validQuery = "SELECT count(" + columnName + ") FROM " + tableName + " WHERE " + columnName
          + " IS NOT NULL";
      CalciteSqlParser.compileToPinotQuery(validQuery);
    } catch (Exception ignore) {
      // Should not reach this line
      fail();
    }

    final String invalidTableNameQuery = "SELECT count(" + columnName + ") FROM " + tableName + SINGLE_CHAR + " WHERE "
        + columnName + " IS NOT NULL";
    final String invalidColumnNameQuery = "SELECT count(" + columnName + SINGLE_CHAR + ") FROM " + tableName + " WHERE "
        + columnName + SINGLE_CHAR + " IS NOT NULL";
    assertThrows(SqlCompilationException.class, () -> CalciteSqlParser.compileToPinotQuery(invalidTableNameQuery));
    assertThrows(SqlCompilationException.class, () -> CalciteSqlParser.compileToPinotQuery(invalidColumnNameQuery));
  }

  private String extendIdentifierToMaxLength(String identifier) {
    return identifier + SINGLE_CHAR.repeat(CALCITE_SQL_PARSER_IDENTIFIER_MAX_LENGTH - identifier.length());
  }
}
