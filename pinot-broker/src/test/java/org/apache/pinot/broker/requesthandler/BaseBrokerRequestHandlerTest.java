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
import org.apache.pinot.broker.routing.BrokerRoutingManager;
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
        BaseBrokerRequestHandler.getActualColumnName("student", "student.student_name", columnNameMap, null, false);
    Assert.assertEquals(actualColumnName, "student_name");
    boolean exceptionThrown = false;
    try {
      String unusedResult =
          BaseBrokerRequestHandler.getActualColumnName("student", "student2.student_name", columnNameMap, null, false);
      Assert.fail("should throw exception if column is not known");
    } catch (BadQueryRequestException ex) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown, "should throw exception if column is not known");
    columnNameMap.put("student_student_name", "student_student_name");
    String wrongColumnName2 = BaseBrokerRequestHandler.getActualColumnName("student",
        "student_student_name", columnNameMap, null, false);
    Assert.assertEquals(wrongColumnName2, "student_student_name");

    columnNameMap.put("student", "student");
    String wrongColumnName3 = BaseBrokerRequestHandler.getActualColumnName("student",
        "student", columnNameMap, null, false);
    Assert.assertEquals(wrongColumnName3, "student");
  }

  @Test
  public void testSplitByLastDot() {
    String[] res = BaseBrokerRequestHandler.splitByLastDot("db.table.column_name");
    Assert.assertEquals(res.length, 2);
    Assert.assertEquals(res[0], "db.table");
    Assert.assertEquals(res[1], "column_name");

    res = BaseBrokerRequestHandler.splitByLastDot("table.column_name");
    Assert.assertEquals(res.length, 2);
    Assert.assertEquals(res[0], "table");
    Assert.assertEquals(res[1], "column_name");

    res = BaseBrokerRequestHandler.splitByLastDot("");
    Assert.assertEquals(res.length, 1);
    Assert.assertEquals(res[0], "");

    res = BaseBrokerRequestHandler.splitByLastDot(".");
    Assert.assertEquals(res.length, 2);
    Assert.assertEquals(res[0], "");
    Assert.assertEquals(res[0], "");

    res = BaseBrokerRequestHandler.splitByLastDot(".column_name");
    Assert.assertEquals(res.length, 2);
    Assert.assertEquals(res[0], "");
    Assert.assertEquals(res[1], "column_name");
  }

  @Test
  public void testGetActualTableNameBanningDots() {
    // not allowing dots
    PinotConfiguration configuration = new PinotConfiguration();
    configuration.setProperty(CommonConstants.Helix.CONFIG_OF_ALLOW_TABLE_NAME_DOTS, false);

    TableCache tableCache = Mockito.mock(TableCache.class);
    BrokerRoutingManager routingManager = Mockito.mock(BrokerRoutingManager.class);
    when(tableCache.isCaseInsensitive()).thenReturn(true);
    when(tableCache.getActualTableName("mytable")).thenReturn("mytable");
    Assert.assertEquals(
        BaseBrokerRequestHandler.getActualTableName("mytable", tableCache, routingManager, configuration), "mytable");
    when(tableCache.getActualTableName("db.mytable")).thenReturn(null);
    Assert.assertEquals(
        BaseBrokerRequestHandler.getActualTableName("db.mytable", tableCache, routingManager, configuration),
        "mytable");

    when(tableCache.isCaseInsensitive()).thenReturn(false);
    when(routingManager.routingExists("mytable_OFFLINE")).thenReturn(true);
    when(routingManager.routingExists("mytable_REALTIME")).thenReturn(true);
    Assert.assertEquals(
        BaseBrokerRequestHandler.getActualTableName("db.mytable", tableCache, routingManager, configuration),
        "mytable");
  }

  @Test
  public void testGetActualTableNameAllowingDots() {
    // not allowing dots
    PinotConfiguration configuration = new PinotConfiguration();
    configuration.setProperty(CommonConstants.Helix.CONFIG_OF_ALLOW_TABLE_NAME_DOTS, true);

    TableCache tableCache = Mockito.mock(TableCache.class);
    BrokerRoutingManager routingManager = Mockito.mock(BrokerRoutingManager.class);
    when(tableCache.isCaseInsensitive()).thenReturn(true);
    // the tableCache should have only "db.mytable" in it since this is the only table
    when(tableCache.getActualTableName("mytable")).thenReturn(null);
    when(tableCache.getActualTableName("db.mytable")).thenReturn("db.mytable");
    when(tableCache.getActualTableName("other.mytable")).thenReturn(null);
    when(tableCache.getActualTableName("test_table")).thenReturn(null);

    Assert.assertEquals(
        BaseBrokerRequestHandler.getActualTableName("test_table", tableCache, routingManager, configuration),
        "test_table");
    Assert.assertEquals(
        BaseBrokerRequestHandler.getActualTableName("mytable", tableCache, routingManager, configuration), "mytable");

    Assert.assertEquals(
        BaseBrokerRequestHandler.getActualTableName("db.mytable", tableCache, routingManager, configuration),
        "db.mytable");
    Assert.assertEquals(
        BaseBrokerRequestHandler.getActualTableName("other.mytable", tableCache, routingManager, configuration),
        "other.mytable");

    when(tableCache.isCaseInsensitive()).thenReturn(false);
    when(routingManager.routingExists("db.namespace.mytable_OFFLINE")).thenReturn(true);
    when(routingManager.routingExists("db.namespace.mytable_REALTIME")).thenReturn(true);
    Assert.assertEquals(
        BaseBrokerRequestHandler.getActualTableName("db.mytable", tableCache, routingManager, configuration),
        "db.mytable");
    Assert.assertEquals(
        BaseBrokerRequestHandler.getActualTableName("db.namespace.mytable", tableCache, routingManager, configuration),
        "db.namespace.mytable");
  }

  @Test
  public void testSplitTableNameByConfig() {
    PinotConfiguration configuration = new PinotConfiguration();
    configuration.setProperty(CommonConstants.Helix.CONFIG_OF_ALLOW_TABLE_NAME_DOTS, false);
    String[] split = BaseBrokerRequestHandler.splitTableNameByConfig("db.table", configuration);
    Assert.assertEquals(split, new String[]{"db", "table"});

    configuration.setProperty(CommonConstants.Helix.CONFIG_OF_ALLOW_TABLE_NAME_DOTS, true);
    split = BaseBrokerRequestHandler.splitTableNameByConfig("db.schema.table", configuration);
    Assert.assertEquals(split, new String[]{"db.schema.table"});

    configuration.setProperty(CommonConstants.Helix.CONFIG_OF_ALLOW_TABLE_NAME_DOTS, false);
    split = BaseBrokerRequestHandler.splitTableNameByConfig("db.table", configuration);
    Assert.assertEquals(split, new String[]{"db", "table"});
  }
}
