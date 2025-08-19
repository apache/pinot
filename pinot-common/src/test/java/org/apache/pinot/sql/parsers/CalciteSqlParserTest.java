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

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.sql.parsers.CalciteSqlParser.CALCITE_SQL_PARSER_IDENTIFIER_MAX_LENGTH;
import static org.testng.Assert.assertThrows;


public class CalciteSqlParserTest {
  private static final String SINGLE_CHAR = "a";
  private static final String QUERY_TEMPLATE = "SELECT %s FROM %s";

  @Test
  public void testIdentifierLength() {
    String tableName = extendIdentifierToMaxLength("exampleTable");
    String columnName = extendIdentifierToMaxLength("exampleColumn");

    String validQuery = createQuery(tableName, columnName);
    CalciteSqlParser.compileToPinotQuery(validQuery);

    String invalidTableNameQuery = createQuery(columnName, tableName + SINGLE_CHAR);
    assertThrows(SqlCompilationException.class, () -> CalciteSqlParser.compileToPinotQuery(invalidTableNameQuery));
    String invalidColumnNameQuery = createQuery(columnName + SINGLE_CHAR, tableName);
    assertThrows(SqlCompilationException.class, () -> CalciteSqlParser.compileToPinotQuery(invalidColumnNameQuery));
  }

  private String extendIdentifierToMaxLength(String identifier) {
    return identifier + SINGLE_CHAR.repeat(CALCITE_SQL_PARSER_IDENTIFIER_MAX_LENGTH - identifier.length());
  }

  private String createQuery(String columnName, String tableName) {
    return String.format(QUERY_TEMPLATE, columnName, tableName);
  }

  @Test(dataProvider = "nonReservedKeywords")
  public void testNonReservedKeywords(String keyword) {
    CalciteSqlParser.compileToPinotQuery(createQuery(keyword, "testTable"));
    CalciteSqlParser.compileToPinotQuery(createQuery(keyword.toUpperCase(), "testTable"));
  }

  @DataProvider
  public static Object[][] nonReservedKeywords() {
    return new Object[][]{
        new Object[]{"int"},
        new Object[]{"integer"},
        new Object[]{"long"},
        new Object[]{"bigint"},
        new Object[]{"float"},
        new Object[]{"double"},
        new Object[]{"big_decimal"},
        new Object[]{"decimal"},
        new Object[]{"boolean"},
        // TODO: Revisit if we should make "timestamp" non reserved
//        new Object[]{"timestamp"},
        new Object[]{"string"},
        new Object[]{"varchar"},
        new Object[]{"bytes"},
        new Object[]{"binary"},
        new Object[]{"varbinary"},
        new Object[]{"variant"},
        new Object[]{"uuid"}
    };
  }
}
