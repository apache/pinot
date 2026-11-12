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

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class ParserUtilsTest {

  @Test(dataProvider = "identifiers")
  public void testSanitizeIdentifier(String identifier, String expected) {
    String sanitized = ParserUtils.sanitizeIdentifier(identifier);
    assertEquals(sanitized, expected);
    CalciteSqlParser.compileToPinotQuery("SELECT " + sanitized + " FROM testTable");
  }

  @DataProvider
  public Object[][] identifiers() {
    return new Object[][] {
      {"myColumn", "myColumn"},
      {"ts", "ts"},
      {"123column", "123column"},
      {"café", "café"},
      {"城市", "城市"},
      {"*", "*"},
      {"schema.*", "schema.*"},
      {"schema.column", "schema.column"},
      {"schema.`group.group_city`", "schema.\"group.group_city\""},
      {"catalog.\"group.group_city\".myColumn", "catalog.\"group.group_city\".myColumn"},
      {"order-id", "\"order-id\""},
      {"column name", "\"column name\""},
      {"`order-id`", "\"order-id\""},
      {"\"123column\"", "\"123column\""},
      {"`order``id`", "\"order`id\""},
      {"\"order\"\"id\"", "\"order\"\"id\""},
      {"`order\"id`", "\"order\"\"id\""},
      {"order\"id", "\"order\"\"id\""},
      {"myTable; DROP TABLE other", "\"myTable; DROP TABLE other\""},
      {"column) FROM other", "\"column) FROM other\""},
      {"foo/*comment*/", "\"foo/*comment*/\""},
      {"foo--comment", "\"foo--comment\""},
      {"  schema . `column name`  ", "schema.\"column name\""}
    };
  }

  @Test
  public void testSanitizeIdentifierRejectsNull() {
    assertThrows(NullPointerException.class, () -> ParserUtils.sanitizeIdentifier(null));
  }

  @Test(dataProvider = "aggregationFunctions")
  public void testSanitizeAggregationFunction(String aggregationFunction, String expected) {
    assertEquals(ParserUtils.sanitizeAggregationFunction(aggregationFunction), expected);
  }

  @DataProvider
  public Object[][] aggregationFunctions() {
    return new Object[][] {
      {"count", "count"}, {" DISTINCTCOUNT ", "DISTINCTCOUNT"}, {"percentile95", "percentile95"}
    };
  }

  @Test
  public void testSanitizeAggregationFunctionRejectsNull() {
    assertThrows(NullPointerException.class, () -> ParserUtils.sanitizeAggregationFunction(null));
  }

  @Test(dataProvider = "invalidAggregationFunctions")
  public void testSanitizeAggregationFunctionRejectsInvalidInput(String aggregationFunction) {
    assertThrows(
        IllegalArgumentException.class,
        () -> ParserUtils.sanitizeAggregationFunction(aggregationFunction));
  }

  @DataProvider
  public Object[][] invalidAggregationFunctions() {
    return new Object[][] {
      {""}, {"lower"}, {"count) FROM other"}, {"count; DROP TABLE other"}
    };
  }

  @Test(dataProvider = "predicates")
  public void testSanitizePredicate(String predicate, String expected) {
    String sanitized = ParserUtils.sanitizePredicate(predicate);
    assertEquals(sanitized, expected);
    if (sanitized != null) {
      CalciteSqlParser.compileToPinotQuery("SELECT * FROM testTable WHERE " + sanitized);
    }
  }

  @DataProvider
  public Object[][] predicates() {
    return new Object[][] {
      {null, null},
      {"", null},
      {"   ", null},
      {" value = 1 ", "value = 1"},
      {"value = otherColumn", "value = otherColumn"},
      {"value >= lowerBound AND value < upperBound", "value >= lowerBound AND value < upperBound"},
      {"message = 'phase;done'", "message = 'phase;done'"},
      {"message = 'phase--done'", "message = 'phase--done'"},
      {"message = 'phase/*done*/'", "message = 'phase/*done*/'"},
      {"message = 'it''s;still--data'", "message = 'it''s;still--data'"}
    };
  }

  @Test(dataProvider = "invalidPredicates")
  public void testSanitizePredicateRejectsInvalidInput(String predicate) {
    assertThrows(IllegalArgumentException.class, () -> ParserUtils.sanitizePredicate(predicate));
  }

  @DataProvider
  public Object[][] invalidPredicates() {
    return new Object[][] {
      {"value = 1; DROP TABLE myTable"},
      {"value = 1 -- comment"},
      {"value = 1 /* comment */"},
      {"value = 1 */ comment"},
      {"message = 'safe;inside'; DROP TABLE myTable"},
      {"value = 1; message = 'safe'"},
      {"value = 1 LIMIT 1"},
      {"value = )"},
      {"message = 'unterminated"},
      {"message = 'unterminated;"}
    };
  }

  @Test(dataProvider = "invalidIdentifiers")
  public void testSanitizeIdentifierRejectsInvalidInput(String identifier) {
    assertThrows(IllegalArgumentException.class, () -> ParserUtils.sanitizeIdentifier(identifier));
  }

  @DataProvider
  public Object[][] invalidIdentifiers() {
    return new Object[][] {
      {""},
      {"   "},
      {"schema..column"},
      {".column"},
      {"column."},
      {"`unterminated"},
      {"\"unterminated"},
      {"``"},
      {"\"\""},
      {"`safe`; DROP TABLE other``"},
      {"\"safe\"; DROP TABLE other\"\""}
    };
  }

  @Test
  public void testRemoveExcessiveWhiteSpace() {

    testRemoveExcessiveWhiteSpace(
        "SELECT * FROM mytable " + " ".repeat(20000),
        "SELECT * FROM mytable"
    );

    testRemoveExcessiveWhiteSpace(
        "SELECT * FROM " + " ".repeat(20000) + " mytable",
        "SELECT * FROM " + " ".repeat(20000) + " mytable"
    );

    testRemoveExcessiveWhiteSpace(
        "SELECT * " + " ".repeat(20000) + "FROM mytable " + " ".repeat(20000),
        "SELECT * " + " ".repeat(20000) + "FROM mytable"
    );

    testRemoveExcessiveWhiteSpace(
        "SELECT * FROM mytable" + " ".repeat(20000) + " options(a=b)" + " ".repeat(20000),
        "SELECT * FROM mytable" + " ".repeat(20000) + " options(a=b)"
    );

    testRemoveExcessiveWhiteSpace(
        "SELECT * FROM mytable" + " ".repeat(20000) + " options(a=b) /* comment */" + " ".repeat(20000),
        "SELECT * FROM mytable" + " ".repeat(20000) + " options(a=b) /* comment */"
    );

    testRemoveExcessiveWhiteSpace(
        "SELECT * FROM mytable" + " ".repeat(20000) + " options(a=b)" + " ".repeat(20000) + " /* comment */",
        "SELECT * FROM mytable" + " ".repeat(20000) + " options(a=b)" + " ".repeat(20000) + " /* comment */"
    );
  }

  private void testRemoveExcessiveWhiteSpace(
      String sqlWithExcessiveWhitespace,
      String expectedSqlAfterSanitization
  ) {
    String sanitizedSql = ParserUtils.sanitizeSql(sqlWithExcessiveWhitespace);
    Assert.assertEquals(sanitizedSql, expectedSqlAfterSanitization);
  }
}
