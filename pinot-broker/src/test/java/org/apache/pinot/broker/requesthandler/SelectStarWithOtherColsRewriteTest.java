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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SelectStarWithOtherColsRewriteTest {

  private static final Map<String, String> COL_MAP;

  static {
    //build table schema
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("playerID", "playerID");
    builder.put("homeRuns", "homeRuns");
    builder.put("playerStint", "playerStint");
    builder.put("groundedIntoDoublePlays", "groundedIntoDoublePlays");
    builder.put("G_old", "G_old");
    builder.put("$segmentName", "$segmentName");
    builder.put("$docId", "$docId");
    builder.put("$hostName", "$hostName");
    COL_MAP = builder.build();
  }

  /**
   * When the query contains only '*', it should be expanded into columns.
   */
  @Test
  public void testShouldExpandWhenOnlyStarIsSelected() {
    String sql = "SELECT * FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    Map<String, Integer> countMap = new HashMap<>();
    for (Expression selection : newSelections) {
      String col = selection.getIdentifier().getName();
      countMap.put(col, countMap.getOrDefault(col, 0) + 1);
    }
    Assert.assertEquals(countMap.size(), 5, "More new selections than expected");
    Assert.assertTrue(countMap.keySet()
            .containsAll(COL_MAP.keySet().stream().filter(a -> !a.startsWith("$")).collect(Collectors.toList())),
        "New selections contain virtual columns");
    countMap.forEach(
        (key, value) -> Assert.assertEquals((int) value, 1, key + " has more than one occurrences in new selection"));
  }

  /**
   * Expansion should not contain any virtual columns
   */
  @Test
  public void testShouldNotReturnExtraDefaultColumns() {
    String sql = "SELECT $docId,*,$segmentName FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    int docIdCnt = 0;
    int segmentNameCnt = 0;
    for (Expression newSelection : newSelections) {
      String colName = newSelection.getIdentifier().getName();
      switch (colName) {
        case "$docId":
          docIdCnt++;
          break;
        case "$segmentName":
          segmentNameCnt++;
          break;
        case "$hostName":
          throw new RuntimeException("Extra default column returned");
        default:
      }
    }
    Assert.assertEquals(docIdCnt, 1);
    Assert.assertEquals(segmentNameCnt, 1);
  }

  /**
   * Columns should not be deduped
   */
  @Test
  public void testShouldNotDedupMultipleRequestedColumns() {
    String sql = "SELECT playerID,*,G_old FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    int playerIdCnt = 0;
    int goldCount = 0;
    for (Expression newSelection : newSelections) {
      String colName = newSelection.getIdentifier().getName();
      switch (colName) {
        case "playerID":
          playerIdCnt++;
          break;
        case "G_old":
          goldCount++;
          break;
        default:
      }
    }
    Assert.assertEquals(playerIdCnt, 2, "playerID does not occur once");
    Assert.assertEquals(goldCount, 2, "G_old occurs does not occur once");
  }

  /**
   * Selections should be returned in the requested order
   */
  @Test
  public void testSelectionOrder() {
    String sql = "SELECT playerID,*,G_old FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    Assert.assertEquals(newSelections.get(0).getIdentifier().getName(), "playerID");
    Assert.assertEquals(newSelections.get(newSelections.size() - 1).getIdentifier().getName(), "G_old");
    //the expanded list should be alphabetically sorted
    List<Expression> expandedSelections = newSelections.subList(1, newSelections.size() - 1);
    List<Expression> assumedUnsortedSelections = new ArrayList<>(expandedSelections);
    //sort alphabetically
    assumedUnsortedSelections.sort(null);
    Assert.assertEquals(expandedSelections, assumedUnsortedSelections, "Expanded selections not sorted alphabetically");
  }

  /**
   * When the same column is requested twice, once with an alias and once as part of *, then it should be returned twice
   */
  @Test
  public void testAliasing() {
    String sql = "SELECT playerID as pid,* FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    Assert.assertTrue(newSelections.get(0).isSetFunctionCall());
    Assert.assertEquals(newSelections.get(0).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(newSelections.get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "playerID");
    Assert.assertEquals(newSelections.get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "pid");
    boolean playerIdPresent = false;
    for (int i = 1; i < newSelections.size(); i++) {
      if (newSelections.get(i).getIdentifier().getName().equals("playerID")) {
        playerIdPresent = true;
        break;
      }
    }
    Assert.assertTrue(playerIdPresent, "playerID col is missing");
  }

  /**
   * When a function is applied to a column, then that col is returned along with the original column
   */
  @Test
  public void testFuncOnColumns1() {
    String sql = "SELECT sqrt(homeRuns),* FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    Assert.assertTrue(newSelections.get(0).isSetFunctionCall());
    Assert.assertEquals(newSelections.get(0).getFunctionCall().getOperator(), "sqrt");
    Assert.assertEquals(newSelections.get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "homeRuns");
    //homeRuns is returned as well
    int homeRunsCnt = 0;
    for (Expression selection : newSelections) {
      if (selection.isSetIdentifier() && selection.getIdentifier().getName().equals("homeRuns")) {
        homeRunsCnt++;
      }
    }
    Assert.assertEquals(homeRunsCnt, 1);
  }

  /**
   * When a function is applied to a column, then that col is returned along with the original column
   */
  @Test
  public void testFuncOnColumns2() {
    String sql = "SELECT add(homeRuns,groundedIntoDoublePlays),* FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    Assert.assertTrue(newSelections.get(0).isSetFunctionCall());
    Assert.assertEquals(newSelections.get(0).getFunctionCall().getOperator(), "add");
    Assert.assertEquals(newSelections.get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "homeRuns");
    Assert.assertEquals(newSelections.get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(),
        "groundedIntoDoublePlays");
    //homeRuns is returned as well
    int homeRunsCnt = 0;
    int groundedIntoDoublePlaysCnt = 0;
    for (Expression selection : newSelections) {
      if (selection.isSetIdentifier() && selection.getIdentifier().getName().equals("homeRuns")) {
        homeRunsCnt++;
      } else if (selection.isSetIdentifier() && selection.getIdentifier().getName().equals("groundedIntoDoublePlays")) {
        groundedIntoDoublePlaysCnt++;
      }
    }
    Assert.assertEquals(homeRunsCnt, 1);
    Assert.assertEquals(groundedIntoDoublePlaysCnt, 1);
  }

  /**
   * When 'n' no. of unqualified * are present, then each column is returned 'n' times.
   */
  @Test
  public void testMultipleUnqualifiedStars() {
    String sql = "SELECT *,* FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    Assert.assertEquals(newSelections.get(0).getIdentifier().getName(), "G_old");
    Assert.assertEquals(newSelections.get(1).getIdentifier().getName(), "groundedIntoDoublePlays");
    Assert.assertEquals(newSelections.get(2).getIdentifier().getName(), "homeRuns");
    Assert.assertEquals(newSelections.get(3).getIdentifier().getName(), "playerID");
    Assert.assertEquals(newSelections.get(4).getIdentifier().getName(), "playerStint");
    Assert.assertEquals(newSelections.get(5).getIdentifier().getName(), "G_old");
    Assert.assertEquals(newSelections.get(6).getIdentifier().getName(), "groundedIntoDoublePlays");
    Assert.assertEquals(newSelections.get(7).getIdentifier().getName(), "homeRuns");
    Assert.assertEquals(newSelections.get(8).getIdentifier().getName(), "playerID");
    Assert.assertEquals(newSelections.get(9).getIdentifier().getName(), "playerStint");
  }

  @Test
  public void testAll() {
    String sql =
        "SELECT abs(homeRuns),sqrt(groundedIntoDoublePlays),*,$segmentName,$hostName,playerStint as pstint,playerID  "
            + "FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    Assert.assertTrue(newSelections.get(0).isSetFunctionCall());
    Assert.assertEquals(newSelections.get(0).getFunctionCall().getOperator(), "abs");
    Assert.assertEquals(newSelections.get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "homeRuns");
    Assert.assertTrue(newSelections.get(1).isSetFunctionCall());
    Assert.assertEquals(newSelections.get(1).getFunctionCall().getOperator(), "sqrt");
    Assert.assertEquals(newSelections.get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "groundedIntoDoublePlays");
    Assert.assertEquals(newSelections.get(2).getIdentifier().getName(), "G_old");
    Assert.assertEquals(newSelections.get(3).getIdentifier().getName(), "groundedIntoDoublePlays");
    Assert.assertEquals(newSelections.get(4).getIdentifier().getName(), "homeRuns");
    Assert.assertEquals(newSelections.get(5).getIdentifier().getName(), "playerID");
    Assert.assertEquals(newSelections.get(6).getIdentifier().getName(), "playerStint");
    Assert.assertEquals(newSelections.get(7).getIdentifier().getName(), "$segmentName");
    Assert.assertEquals(newSelections.get(8).getIdentifier().getName(), "$hostName");
    Assert.assertEquals(newSelections.get(9).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(newSelections.get(9).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "playerStint");
    Assert.assertEquals(newSelections.get(9).getFunctionCall().getOperands().get(1).getIdentifier().getName(),
        "pstint");
    Assert.assertEquals(newSelections.get(10).getIdentifier().getName(), "playerID");
  }
}
