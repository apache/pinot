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
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SelectStarWithOtherColsRewriteTest {

  private static final Map<String, String> COL_MAP;

  static {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("homeRuns", "homeRuns");
    builder.put("playerStint", "playerStint");
    builder.put("groundedIntoDoublePlays", "groundedIntoDoublePlays");
    builder.put("playerID", "playerID");
    builder.put("$segmentName", "$segmentName");
    builder.put("$docId", "$docId");
    builder.put("$hostName", "$hostName");
    COL_MAP = builder.build();
  }

  @Test
  public void testHappyCase() {
    String sql = "SELECT $segmentName,*,$hostName FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    Assert.assertEquals(newSelections.get(0).getIdentifier().getName(), "$segmentName");
    Assert.assertEquals(newSelections.get(1).getIdentifier().getName(), "homeRuns");
    Assert.assertEquals(newSelections.get(2).getIdentifier().getName(), "playerStint");
    Assert.assertEquals(newSelections.get(3).getIdentifier().getName(), "groundedIntoDoublePlays");
    Assert.assertEquals(newSelections.get(4).getIdentifier().getName(), "playerID");
    Assert.assertEquals(newSelections.get(5).getIdentifier().getName(), "$hostName");
  }

  /**
   * When duplicate columns are requested, they should be deduped, that is, each column should be returned only once
   */
  @Test
  public void testDupCols() {
    String sql = "SELECT playerID,homeRuns,* FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    int playerIdCount = 0, homeRunsCount = 0;
    for(Expression expression : newSelections) {
      if (expression.getIdentifier().getName().equals("playerID")) {
        playerIdCount++;
      } else if(expression.getIdentifier().getName().equals("homeRuns")) {
        homeRunsCount++;
      }
    }
    Assert.assertEquals(playerIdCount, 1);
    Assert.assertEquals(homeRunsCount, 1);
  }

  /**
   * Selections should be returned in the requested order
   */
  @Test
  public void testSelectionOrder() {
//    SELECT playerID,intentionalWalks,* FROM baseballStats order by numberOfGames desc -> problemetic query, col:
//    runs does not have any values in any of the rows
    String sql = "SELECT playerID,*,homeRuns FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    Assert.assertEquals(newSelections.get(0).getIdentifier().getName(), "playerID");
    Assert.assertEquals(newSelections.get(1).getIdentifier().getName(), "playerStint");
    Assert.assertEquals(newSelections.get(2).getIdentifier().getName(), "groundedIntoDoublePlays");
    Assert.assertEquals(newSelections.get(3).getIdentifier().getName(), "homeRuns");
  }

  /**
   * Only requested default columns should be returned
   */
  @Test
  public void shouldNotReturnExtraDefaultVirtualCols() {
    String sql = "SELECT $segmentName,* FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    for (Expression expression : newSelections) {
      String colName = expression.getIdentifier().getName();
      if (CommonConstants.Segment.BuiltInVirtualColumn.DOCID.equals(colName)
          || CommonConstants.Segment.BuiltInVirtualColumn.HOSTNAME.equals(colName)) {
        throw new RuntimeException("Contains extra virtual col");
      }
    }
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
    Assert.assertEquals(newSelections.get(0).getFunctionCall().getOperator(), "AS");
    Assert.assertEquals(newSelections.get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "pid");
    Assert.assertEquals(newSelections.get(4).getIdentifier().getName(), "playerID");
  }

  /**
   * When the same column is requested twice, once with an alias and once as part of *, then it should be returned twice
   */
  @Test
  public void testAliasingWithDedup() {
    String sql = "SELECT playerID as pid,*, playerID FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    Assert.assertTrue(newSelections.get(0).isSetFunctionCall());
    Assert.assertEquals(newSelections.get(0).getFunctionCall().getOperator(), "AS");
    Assert.assertEquals(newSelections.get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "playerID");
    Assert.assertEquals(newSelections.get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "pid");
    Assert.assertEquals(newSelections.get(1).getIdentifier().getName(), "homeRuns");
    Assert.assertEquals(newSelections.get(2).getIdentifier().getName(), "playerStint");
    Assert.assertEquals(newSelections.get(3).getIdentifier().getName(), "groundedIntoDoublePlays");
    Assert.assertEquals(newSelections.get(4).getIdentifier().getName(), "playerID");
  }

  @Test
  public void testFunctionsOnCols() {
    String sql = "SELECT sqrt(homeRuns),* FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    int funcs = 0;
    for (Expression selection : newSelections) {
      if (selection.isSetFunctionCall()) {
        funcs++;
        Assert.assertEquals(selection.getFunctionCall().getOperator(), "SQRT");
        Assert.assertEquals(selection.getFunctionCall().getOperands().get(0).getIdentifier().getName(), "homeRuns");
      }
    }
    Assert.assertEquals(funcs, 1);
  }

  /**
   * When multiple unqualified * are present, all are expanded
   */
  @Test
  public void testMultipleUnqualifiedStars() {
    String sql = "SELECT *,* FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    Assert.assertEquals(newSelections.get(0).getIdentifier().getName(), "homeRuns");
    Assert.assertEquals(newSelections.get(1).getIdentifier().getName(), "playerStint");
    Assert.assertEquals(newSelections.get(2).getIdentifier().getName(), "groundedIntoDoublePlays");
    Assert.assertEquals(newSelections.get(3).getIdentifier().getName(), "playerID");
    Assert.assertEquals(newSelections.get(4).getIdentifier().getName(), "homeRuns");
    Assert.assertEquals(newSelections.get(5).getIdentifier().getName(), "playerStint");
    Assert.assertEquals(newSelections.get(6).getIdentifier().getName(), "groundedIntoDoublePlays");
    Assert.assertEquals(newSelections.get(7).getIdentifier().getName(), "playerID");
  }

  @Test
  public void testAll() {
    String sql = "SELECT abs(homeRuns),sqrt(groundedIntoDoublePlays),*,$segmentName,$hostName,playerStint as pstint,playerID  FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    Assert.assertTrue(newSelections.get(0).isSetFunctionCall());
    Assert.assertEquals(newSelections.get(0).getFunctionCall().getOperator(), "ABS");
    Assert.assertEquals(newSelections.get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "homeRuns");
    Assert.assertTrue(newSelections.get(1).isSetFunctionCall());
    Assert.assertEquals(newSelections.get(1).getFunctionCall().getOperator(), "SQRT");
    Assert.assertEquals(newSelections.get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "groundedIntoDoublePlays");
    Assert.assertEquals(newSelections.get(2).getIdentifier().getName(), "homeRuns");
    Assert.assertEquals(newSelections.get(3).getIdentifier().getName(), "playerStint");
    Assert.assertEquals(newSelections.get(4).getIdentifier().getName(), "groundedIntoDoublePlays");
    Assert.assertEquals(newSelections.get(5).getIdentifier().getName(), "$segmentName");
    Assert.assertEquals(newSelections.get(6).getIdentifier().getName(), "$hostName");
    Assert.assertEquals(newSelections.get(7).getFunctionCall().getOperator(), "AS");
    Assert.assertEquals(newSelections.get(7).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "playerStint");
    Assert.assertEquals(newSelections.get(7).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "pstint");
    Assert.assertEquals(newSelections.get(8).getIdentifier().getName(), "playerID");
  }
}
