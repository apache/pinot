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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.CommonConstants.Broker.Request;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests order by queries
 */
public class InterSegmentOrderByMultiValueQueriesTest extends BaseMultiValueQueriesTest {

  @Test(dataProvider = "orderByDataProvider")
  public void testAggregationOrderedGroupByResults(String query, List<Object[]> expectedResults,
      long expectedNumEntriesScannedPostFilter) {
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(Request.QueryOptionKey.GROUP_BY_MODE, Request.SQL);
    queryOptions.put(Request.QueryOptionKey.RESPONSE_FORMAT, Request.SQL);
    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query, queryOptions);
    QueriesTestUtils.testInterSegmentGroupByOrderByResultSQL(brokerResponse, 400000L, 0,
        expectedNumEntriesScannedPostFilter, 400000L, expectedResults);
  }

  /**
   * Provides various combinations of order by.
   * In order to calculate the expected results, the results from a group by were taken, and then ordered accordingly.
   */
  @DataProvider(name = "orderByDataProvider")
  public Object[][] orderByDataProvider() {

    List<Object[]> data = new ArrayList<>();
    String query;
    List<Object[]> results;
    long numEntriesScannedPostFilter;

    query = "SELECT SUMMV(column7) FROM testTable GROUP BY column3 ORDER BY column3";
    results = Lists.newArrayList(new Object[]{"", 63917703269308.0}, new Object[]{"L", 33260235267900.0},
        new Object[]{"P", 212961658305696.0}, new Object[]{"PbQd", 2001454759004.0},
        new Object[]{"w", 116831822080776.0});
    numEntriesScannedPostFilter = 800000;
    data.add(new Object[]{query, results, numEntriesScannedPostFilter});

    query = "SELECT SUMMV(column7) FROM testTable GROUP BY column5 ORDER BY column5 DESC TOP 4";
    results = Lists.newArrayList(new Object[]{"yQkJTLOQoOqqhkAClgC", 61100215182228.00000},
        new Object[]{"mhoVvrJm", 5806796153884.00000},
        new Object[]{"kCMyNVGCASKYDdQbftOPaqVMWc", 51891832239248.00000},
        new Object[]{"PbQd", 36532997335388.00000});
    numEntriesScannedPostFilter = 800000;
    data.add(new Object[]{query, results, numEntriesScannedPostFilter});

    query = "SELECT SUMMV(column7) FROM testTable GROUP BY column5 ORDER BY SUMMV(column7) TOP 5";
    results = Lists.newArrayList(new Object[]{"NCoFku", 489626381288.00000},
        new Object[]{"mhoVvrJm", 5806796153884.00000},
        new Object[]{"JXRmGakTYafZFPm", 18408231081808.00000}, new Object[]{"PbQd", 36532997335388.00000},
        new Object[]{"OKyOqU", 51067166589176.00000});
    numEntriesScannedPostFilter = 800000;
    data.add(new Object[]{query, results, numEntriesScannedPostFilter});

    // object type aggregations
    query = "SELECT MINMAXRANGEMV(column7) FROM testTable GROUP BY column5 ORDER BY column5";
    results = Lists.newArrayList(new Object[]{"AKXcXcIqsqOJFsdwxZ", 2147483446.00000},
        new Object[]{"EOFxevm", 2147483446.00000}, new Object[]{"JXRmGakTYafZFPm", 2147483443.00000},
        new Object[]{"NCoFku", 2147483436.00000}, new Object[]{"OKyOqU", 2147483443.00000},
        new Object[]{"PbQd", 2147483443.00000},
        new Object[]{"kCMyNVGCASKYDdQbftOPaqVMWc", 2147483446.00000},
        new Object[]{"mhoVvrJm", 2147483438.00000}, new Object[]{"yQkJTLOQoOqqhkAClgC", 2147483446.00000});
    numEntriesScannedPostFilter = 800000;
    data.add(new Object[]{query, results, numEntriesScannedPostFilter});

    // object type aggregations
    query =
        "SELECT MINMAXRANGEMV(column7) FROM testTable GROUP BY column5 ORDER BY MINMAXRANGEMV(column7), column5 desc";
    results = Lists.newArrayList(new Object[]{"NCoFku", 2147483436.00000},
        new Object[]{"mhoVvrJm", 2147483438.00000}, new Object[]{"PbQd", 2147483443.00000},
        new Object[]{"OKyOqU", 2147483443.00000}, new Object[]{"JXRmGakTYafZFPm", 2147483443.00000},
        new Object[]{"yQkJTLOQoOqqhkAClgC", 2147483446.00000},
        new Object[]{"kCMyNVGCASKYDdQbftOPaqVMWc", 2147483446.00000},
        new Object[]{"EOFxevm", 2147483446.00000}, new Object[]{"AKXcXcIqsqOJFsdwxZ", 2147483446.00000});
    numEntriesScannedPostFilter = 800000;
    data.add(new Object[]{query, results, numEntriesScannedPostFilter});

    // object type aggregations - non comparable intermediate results
    query = "SELECT DISTINCTCOUNTMV(column7) FROM testTable GROUP BY column5 ORDER BY DISTINCTCOUNTMV(column7) top 5";
    results = Lists.newArrayList(new Object[]{"NCoFku", 26}, new Object[]{"mhoVvrJm", 65},
        new Object[]{"JXRmGakTYafZFPm", 126}, new Object[]{"PbQd", 211}, new Object[]{"OKyOqU", 216});
    numEntriesScannedPostFilter = 800000;
    data.add(new Object[]{query, results, numEntriesScannedPostFilter});

    return data.toArray(new Object[data.size()][]);
  }
}