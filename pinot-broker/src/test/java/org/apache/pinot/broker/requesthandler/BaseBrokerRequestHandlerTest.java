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
package org.apache.pinot.broker.requesthandler;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;


public class BaseBrokerRequestHandlerTest {

  @Test
  public void testUpdateColumnNames() {
    String query = "SELECT database.my_table.column_name_1st, column_name_2nd from database.my_table";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Map<String, String> columnNameMap =
        ImmutableMap.of("column_name_1st", "column_name_1st", "column_name_2nd", "column_name_2nd");
    BaseBrokerRequestHandler.updateColumnNames("database.my_table", pinotQuery, false, columnNameMap);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 2);
    for (Expression expression : pinotQuery.getSelectList()) {
      String columnName = expression.getIdentifier().getName();
      if (columnName.endsWith("column_name_1st")) {
        Assert.assertEquals(columnName, "column_name_1st");
      } else if (columnName.endsWith("column_name_2nd")) {
        Assert.assertEquals(columnName, "column_name_2nd");
      } else {
        Assert.fail("rewritten column name should be column_name_1st or column_name_1st, but is " + columnName);
      }
    }
  }

  @Test
  public void testGetActualColumnNameCaseSensitive() {
    Map<String, String> columnNameMap = new HashMap<>();
    columnNameMap.put("student_name", "student_name");
    String actualColumnName =
        BaseBrokerRequestHandler.getActualColumnName("mytable", "mytable.student_name", columnNameMap, null, false);
    Assert.assertEquals(actualColumnName, "student_name");
    boolean exceptionThrown = false;
    try {
      BaseBrokerRequestHandler.getActualColumnName("mytable", "mytable2.student_name", columnNameMap, null, false);
      Assert.fail("should throw exception if column is not known");
    } catch (BadQueryRequestException ex) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown, "should throw exception if column is not known");
    exceptionThrown = false;
    try {
      BaseBrokerRequestHandler.getActualColumnName("mytable", "MYTABLE.student_name", columnNameMap, null, false);
      Assert.fail("should throw exception if case sensitive and table name different");
    } catch (BadQueryRequestException ex) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown, "should throw exception if column is not known");
    columnNameMap.put("mytable_student_name", "mytable_student_name");
    String wrongColumnName2 =
        BaseBrokerRequestHandler.getActualColumnName("mytable", "mytable_student_name", columnNameMap, null, false);
    Assert.assertEquals(wrongColumnName2, "mytable_student_name");

    columnNameMap.put("mytable", "mytable");
    String wrongColumnName3 =
        BaseBrokerRequestHandler.getActualColumnName("mytable", "mytable", columnNameMap, null, false);
    Assert.assertEquals(wrongColumnName3, "mytable");
  }

  @Test
  public void testGetActualColumnNameCaseInSensitive() {
    Map<String, String> columnNameMap = new HashMap<>();
    columnNameMap.put("student_name", "student_name");
    String actualColumnName =
        BaseBrokerRequestHandler.getActualColumnName("mytable", "MYTABLE.student_name", columnNameMap, null, true);
    Assert.assertEquals(actualColumnName, "student_name");
    boolean exceptionThrown = false;
    try {
      BaseBrokerRequestHandler.getActualColumnName("student", "MYTABLE2.student_name", columnNameMap, null, true);
      Assert.fail("should throw exception if column is not known");
    } catch (BadQueryRequestException ex) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown, "should throw exception if column is not known");
    columnNameMap.put("mytable_student_name", "mytable_student_name");
    String wrongColumnName2 =
        BaseBrokerRequestHandler.getActualColumnName("mytable", "MYTABLE_student_name", columnNameMap, null, true);
    Assert.assertEquals(wrongColumnName2, "mytable_student_name");

    columnNameMap.put("mytable", "mytable");
    String wrongColumnName3 =
        BaseBrokerRequestHandler.getActualColumnName("MYTABLE", "mytable", columnNameMap, null, true);
    Assert.assertEquals(wrongColumnName3, "mytable");
  }

  @Test
  public void testGetActualTableNameBanningDots() {
    // not allowing dots
    PinotConfiguration configuration = new PinotConfiguration();
    configuration.setProperty(CommonConstants.Helix.ALLOW_TABLE_NAME_WITH_DATABASE, false);

    TableCache tableCache = Mockito.mock(TableCache.class);
    when(tableCache.isIgnoreCase()).thenReturn(true);
    when(tableCache.getActualTableName("mytable")).thenReturn("mytable");
    Assert.assertEquals(BaseBrokerRequestHandler.getActualTableName("mytable", tableCache), "mytable");
    when(tableCache.getActualTableName("db.mytable")).thenReturn(null);
    Assert.assertEquals(BaseBrokerRequestHandler.getActualTableName("db.mytable", tableCache), "mytable");

    when(tableCache.isIgnoreCase()).thenReturn(false);
    Assert.assertEquals(BaseBrokerRequestHandler.getActualTableName("db.mytable", tableCache), "mytable");
  }

  @Test
  public void testGetActualTableNameAllowingDots() {

    TableCache tableCache = Mockito.mock(TableCache.class);
    when(tableCache.isIgnoreCase()).thenReturn(true);
    // the tableCache should have only "db.mytable" in it since this is the only table
    when(tableCache.getActualTableName("mytable")).thenReturn(null);
    when(tableCache.getActualTableName("db.mytable")).thenReturn("db.mytable");
    when(tableCache.getActualTableName("other.mytable")).thenReturn(null);
    when(tableCache.getActualTableName("test_table")).thenReturn(null);

    Assert.assertEquals(BaseBrokerRequestHandler.getActualTableName("test_table", tableCache), "test_table");
    Assert.assertEquals(BaseBrokerRequestHandler.getActualTableName("mytable", tableCache), "mytable");

    Assert.assertEquals(BaseBrokerRequestHandler.getActualTableName("db.mytable", tableCache), "db.mytable");
    Assert.assertEquals(BaseBrokerRequestHandler.getActualTableName("other.mytable", tableCache), "other.mytable");

    when(tableCache.isIgnoreCase()).thenReturn(false);
    Assert.assertEquals(BaseBrokerRequestHandler.getActualTableName("db.mytable", tableCache), "db.mytable");
    Assert.assertEquals(BaseBrokerRequestHandler.getActualTableName("db.namespace.mytable", tableCache),
        "db.namespace.mytable");
  }
}
