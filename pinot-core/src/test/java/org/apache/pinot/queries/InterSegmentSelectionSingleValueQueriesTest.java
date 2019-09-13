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
package org.apache.pinot.queries;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.testng.Assert;
import org.testng.annotations.Test;


public class InterSegmentSelectionSingleValueQueriesTest extends BaseSingleValueQueriesTest {
  @Test
  public void testSelectStar() {
    String query = "SELECT * FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    Map<String, String> expectedColumnTypes = new HashMap<>();
    expectedColumnTypes.put("column1", "INT");
    expectedColumnTypes.put("column3", "INT");
    expectedColumnTypes.put("column5", "STRING");
    expectedColumnTypes.put("column6", "INT");
    expectedColumnTypes.put("column7", "INT");
    expectedColumnTypes.put("column9", "INT");
    expectedColumnTypes.put("column11", "STRING");
    expectedColumnTypes.put("column12", "STRING");
    expectedColumnTypes.put("column17", "INT");
    expectedColumnTypes.put("column18", "INT");
    expectedColumnTypes.put("daysSinceEpoch", "INT");

    List<String> expectedColumns = new ArrayList<>();
    expectedColumns.add("column1");
    expectedColumns.add("column3");
    expectedColumns.add("column5");
    expectedColumns.add("column6");
    expectedColumns.add("column7");
    expectedColumns.add("column9");
    expectedColumns.add("column11");
    expectedColumns.add("column12");
    expectedColumns.add("column17");
    expectedColumns.add("column18");
    expectedColumns.add("daysSinceEpoch");
    verifyBrokerResponse(brokerResponse, 40L, 120000L, expectedColumns, expectedColumnTypes);
  }

  @Test
  public void testSelectColumns() {
    String query = "SELECT column1, column5 FROM testTable";
    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    Map<String, String> expectedColumnTypes = new HashMap<>();
    expectedColumnTypes.put("column1", "INT");
    expectedColumnTypes.put("column5", "STRING");
    List<String> expectedColumns = new ArrayList<>();
    expectedColumns.add("column1");
    expectedColumns.add("column5");
    verifyBrokerResponse(brokerResponse, 40L, 120000L, expectedColumns, expectedColumnTypes);
  }

  private void verifyBrokerResponse(BrokerResponseNative brokerResponse, long expectedNumDocsScanned,
      long expectedTotalDocNum, List<String> expectedColumns, Map<String, String> expectedColumnTypes) {
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), expectedNumDocsScanned);
    Assert.assertEquals(brokerResponse.getTotalDocs(), expectedTotalDocNum);
    Assert.assertEquals(brokerResponse.getSelectionResults().getColumns().size(), expectedColumns.size());
    for (String column : expectedColumns) {
      Assert.assertTrue(brokerResponse.getSelectionResults().getColumns().contains(column));
    }
    Assert.assertEquals(brokerResponse.getSelectionResults().getColumnTypes(), expectedColumnTypes);
  }
}
