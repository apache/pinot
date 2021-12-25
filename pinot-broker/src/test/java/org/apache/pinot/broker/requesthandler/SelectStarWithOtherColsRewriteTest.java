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
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
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

  @Test
  public void testDupCols() {
    String sql = "SELECT playerID,homeRuns,* FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    Assert.assertEquals(newSelections.get(0).getIdentifier().getName(), "playerID");
    Assert.assertEquals(newSelections.get(1).getIdentifier().getName(), "homeRuns");
    Assert.assertEquals(newSelections.get(2).getIdentifier().getName(), "homeRuns");
    Assert.assertEquals(newSelections.get(3).getIdentifier().getName(), "playerStint");
    Assert.assertEquals(newSelections.get(4).getIdentifier().getName(), "groundedIntoDoublePlays");
    Assert.assertEquals(newSelections.get(5).getIdentifier().getName(), "playerID");
  }

  @Test
  public void testOrder() {
    String sql = "SELECT playerID,*,homeRuns FROM baseballStats";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    BaseBrokerRequestHandler.updateColumnNames("baseballStats", pinotQuery, false, COL_MAP);
    List<Expression> newSelections = pinotQuery.getSelectList();
    Assert.assertEquals(newSelections.get(0).getIdentifier().getName(), "playerID");
    Assert.assertEquals(newSelections.get(1).getIdentifier().getName(), "homeRuns");
    Assert.assertEquals(newSelections.get(2).getIdentifier().getName(), "playerStint");
    Assert.assertEquals(newSelections.get(3).getIdentifier().getName(), "groundedIntoDoublePlays");
    Assert.assertEquals(newSelections.get(4).getIdentifier().getName(), "playerID");
    Assert.assertEquals(newSelections.get(5).getIdentifier().getName(), "homeRuns");
  }

}
