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

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.GroupByOrderByResults;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class InterSegmentAggregationGroupByOrderByMultiValueQueriesTest extends BaseMultiValueQueriesTest {

  @Test
  public void testDummy() {
    String orderByQuery = "select sumMV(column7) from testTable group by column3";
    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(orderByQuery);
    GroupByOrderByResults expectedGroupByOrderBy = new GroupByOrderByResults(null, null, null);
  }

  @Test(dataProvider = "orderByDataProvider")
  public void testAggregationGroupByOrderByResults(String query, List<String> expectedColumns,
      List<String[]> expectedKeys, List<Serializable[]> expectedResults, long expectedNumEntriesScannedPostFilter) {
    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    GroupByOrderByResults expectedGroupByOrderBy =
        new GroupByOrderByResults(expectedColumns, expectedKeys, expectedResults);
    QueriesTestUtils.testInterSegmentAggregationGroupByOrderByResult(brokerResponse, 400000, 0L,
        expectedNumEntriesScannedPostFilter, 400000, expectedGroupByOrderBy);
  }

  /**
   * Provides various combinations of order by.
   * In order to calculate the expected results, the results from a group by were taken, and then ordered accordingly.
   */
  @DataProvider(name = "orderByDataProvider")
  public Object[][] orderByDataProvider() {

    List<Object[]> data = new ArrayList<>();
    String query;
    List<String> columns;
    List<String[]> groupByKeys;
    List<Serializable[]> results;
    long numEntriesScannedPostFilter;

    query = "SELECT SUMMV(column7) FROM testTable GROUP BY column3 ORDER BY column3";
    columns = Lists.newArrayList("column3");
    groupByKeys = Lists.newArrayList(new String[]{""}, new String[]{"L"}, new String[]{"P"}, new String[]{"PbQd"},
        new String[]{"w"});
    results = Lists.newArrayList(new Serializable[]{63917703269308.0}, new Serializable[]{33260235267900.0},
        new Serializable[]{212961658305696.0}, new Serializable[]{2001454759004.0},
        new Serializable[]{116831822080776.0});
    numEntriesScannedPostFilter = 800000;
    data.add(new Object[]{query, columns, groupByKeys, results, numEntriesScannedPostFilter});

    query = "SELECT SUMMV(column7) FROM testTable GROUP BY column5 ORDER BY column5 DESC TOP 4";
    columns = Lists.newArrayList("column5");
    groupByKeys = Lists.newArrayList(new String[]{"yQkJTLOQoOqqhkAClgC"}, new String[]{"mhoVvrJm"},
        new String[]{"kCMyNVGCASKYDdQbftOPaqVMWc"}, new String[]{"PbQd"});
    results = Lists.newArrayList(new Serializable[]{61100215182228.00000}, new Serializable[]{5806796153884.00000},
        new Serializable[]{51891832239248.00000}, new Serializable[]{36532997335388.00000});
    numEntriesScannedPostFilter = 800000;
    data.add(new Object[]{query, columns, groupByKeys, results, numEntriesScannedPostFilter});

    query = "SELECT SUMMV(column7) FROM testTable GROUP BY column5 ORDER BY SUMMV(column7) TOP 5";
    columns = Lists.newArrayList("summv(column7)");
    groupByKeys = Lists.newArrayList(new String[]{"NCoFku"}, new String[]{"mhoVvrJm"}, new String[]{"JXRmGakTYafZFPm"},
        new String[]{"PbQd"}, new String[]{"OKyOqU"});
    results = Lists.newArrayList(new Serializable[]{489626381288.00000}, new Serializable[]{5806796153884.00000},
        new Serializable[]{18408231081808.00000}, new Serializable[]{36532997335388.00000},
        new Serializable[]{51067166589176.00000});
    numEntriesScannedPostFilter = 800000;
    data.add(new Object[]{query, columns, groupByKeys, results, numEntriesScannedPostFilter});

    // TODO: handle non Number aggregations (AVG, DISTINCTCOUNTHLL etc) once implementation

    return data.toArray(new Object[data.size()][]);
  }
  }
