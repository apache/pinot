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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.GroupByOrderByResults;
import org.apache.pinot.common.utils.BytesUtils;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.startree.hll.HllUtil;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class InterSegmentAggregationGroupByOrderBySingleValueQueriesTest extends BaseSingleValueQueriesTest {

  @Test
  public void testDummy() {
    String orderByQuery = "select * from testTable limit 20";
    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(orderByQuery);
    GroupByOrderByResults expectedGroupByOrderBy = new GroupByOrderByResults(null, null, null);
  }

  @Test(dataProvider = "orderByDataProvider")
  public void testAggregationGroupByOrderByResults(String query, List<String> expectedColumns,
      List<String[]> expectedKeys, List<Serializable[]> expectedResults, long expectedNumEntriesScannedPostFilter) {
    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    GroupByOrderByResults expectedGroupByOrderBy =
        new GroupByOrderByResults(expectedColumns, expectedKeys, expectedResults);
    QueriesTestUtils.testInterSegmentAggregationGroupByOrderByResult(brokerResponse, 120000L, 0L,
        expectedNumEntriesScannedPostFilter, 120000L, expectedGroupByOrderBy);
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

    query = "SELECT SUM(column1) FROM testTable GROUP BY column11 ORDER BY column11";
    columns = Lists.newArrayList("column11");
    groupByKeys = Lists.newArrayList(new String[]{""}, new String[]{"P"}, new String[]{"gFuH"}, new String[]{"o"},
        new String[]{"t"});
    results = Lists.newArrayList(new Serializable[]{5935285005452.0}, new Serializable[]{88832999206836.0},
        new Serializable[]{63202785888.0}, new Serializable[]{18105331533948.0}, new Serializable[]{16331923219264.0});
    numEntriesScannedPostFilter = 240000;
    data.add(new Object[]{query, columns, groupByKeys, results, numEntriesScannedPostFilter});

    query = "SELECT SUM(column1) FROM testTable GROUP BY column11 ORDER BY column11 DESC";
    columns = Lists.newArrayList("column11");
    groupByKeys = Lists.newArrayList(groupByKeys);
    Collections.reverse(groupByKeys);
    results = Lists.newArrayList(results);
    Collections.reverse(results);
    numEntriesScannedPostFilter = 240000;
    data.add(new Object[]{query, columns, groupByKeys, results, numEntriesScannedPostFilter});

    query = "SELECT SUM(column1) FROM testTable GROUP BY column11 ORDER BY column11 TOP 3";
    columns = Lists.newArrayList("column11");
    groupByKeys = Lists.newArrayList(groupByKeys);
    Collections.reverse(groupByKeys);
    groupByKeys = groupByKeys.subList(0, 3);
    results = Lists.newArrayList(results);
    Collections.reverse(results);
    results = results.subList(0, 3);
    numEntriesScannedPostFilter = 240000;
    data.add(new Object[]{query, columns, groupByKeys, results, numEntriesScannedPostFilter});

    query = "SELECT SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY column11, column12";
    columns = Lists.newArrayList("column11", "column12");
    groupByKeys = Lists.newArrayList(new String[]{"", "HEuxNvH"}, new String[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx"},
        new String[]{"", "MaztCmmxxgguBUxPti"}, new String[]{"", "dJWwFk"}, new String[]{"", "oZgnrlDEtjjVpUoFLol"},
        new String[]{"P", "HEuxNvH"}, new String[]{"P", "KrNxpdycSiwoRohEiTIlLqDHnx"},
        new String[]{"P", "MaztCmmxxgguBUxPti"}, new String[]{"P", "TTltMtFiRqUjvOG"}, new String[]{"P", "XcBNHe"});
    results = Lists.newArrayList(new Serializable[]{3789390396216.0}, new Serializable[]{733802350944.00000},
        new Serializable[]{1333941430664.00000}, new Serializable[]{55470665124.0000},
        new Serializable[]{22680162504.00000}, new Serializable[]{21998672845052.00000},
        new Serializable[]{18069909216728.00000}, new Serializable[]{27177029040008.00000},
        new Serializable[]{4462670055540.00000}, new Serializable[]{120021767504.00000});
    numEntriesScannedPostFilter = 360000;
    data.add(new Object[]{query, columns, groupByKeys, results, numEntriesScannedPostFilter});

    query = "SELECT SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY column11, column12 TOP 15";
    columns = Lists.newArrayList("column11", "column12");
    groupByKeys = Lists.newArrayList(groupByKeys);
    groupByKeys.add(new String[]{"P", "dJWwFk"});
    groupByKeys.add(new String[]{"P", "fykKFqiw"});
    groupByKeys.add(new String[]{"P", "gFuH"});
    groupByKeys.add(new String[]{"P", "oZgnrlDEtjjVpUoFLol"});
    groupByKeys.add(new String[]{"gFuH", "HEuxNvH"});
    results = Lists.newArrayList(results);
    results.add(new Serializable[]{6224665921376.00000});
    results.add(new Serializable[]{1574451324140.00000});
    results.add(new Serializable[]{860077643636.00000});
    results.add(new Serializable[]{8345501392852.00000});
    results.add(new Serializable[]{29872400856.00000});
    numEntriesScannedPostFilter = 360000;
    data.add(new Object[]{query, columns, groupByKeys, results, numEntriesScannedPostFilter});

    query = "SELECT SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY column11, column12 DESC";
    columns = Lists.newArrayList("column11", "column12");
    groupByKeys = Lists.newArrayList(new String[]{"", "oZgnrlDEtjjVpUoFLol"}, new String[]{"", "dJWwFk"},
        new String[]{"", "MaztCmmxxgguBUxPti"}, new String[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx"},
        new String[]{"", "HEuxNvH"}, new String[]{"P", "oZgnrlDEtjjVpUoFLol"}, new String[]{"P", "gFuH"},
        new String[]{"P", "fykKFqiw"}, new String[]{"P", "dJWwFk"}, new String[]{"P", "XcBNHe"});
    results = Lists.newArrayList(new Serializable[]{22680162504.00000}, new Serializable[]{55470665124.0000},
        new Serializable[]{1333941430664.00000}, new Serializable[]{733802350944.00000},
        new Serializable[]{3789390396216.00000}, new Serializable[]{8345501392852.00000},
        new Serializable[]{860077643636.00000}, new Serializable[]{1574451324140.00000},
        new Serializable[]{6224665921376.00000}, new Serializable[]{120021767504.00000});
    numEntriesScannedPostFilter = 360000;
    data.add(new Object[]{query, columns, groupByKeys, results, numEntriesScannedPostFilter});

    query = "SELECT SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY column11, SUM(column1)";
    columns = Lists.newArrayList("column11", "sum(column1)");
    groupByKeys = Lists.newArrayList(new String[]{"", "oZgnrlDEtjjVpUoFLol"}, new String[]{"", "dJWwFk"},
        new String[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx"}, new String[]{"", "MaztCmmxxgguBUxPti"},
        new String[]{"", "HEuxNvH"}, new String[]{"P", "XcBNHe"}, new String[]{"P", "gFuH"},
        new String[]{"P", "fykKFqiw"}, new String[]{"P", "TTltMtFiRqUjvOG"}, new String[]{"P", "dJWwFk"});
    results = Lists.newArrayList(new Serializable[]{22680162504.00000}, new Serializable[]{55470665124.0000},
        new Serializable[]{733802350944.00000}, new Serializable[]{1333941430664.00000},
        new Serializable[]{3789390396216.00000}, new Serializable[]{120021767504.00000},
        new Serializable[]{860077643636.00000}, new Serializable[]{1574451324140.00000},
        new Serializable[]{4462670055540.00000}, new Serializable[]{6224665921376.00000});
    numEntriesScannedPostFilter = 360000;
    data.add(new Object[]{query, columns, groupByKeys, results, numEntriesScannedPostFilter});

    query = "SELECT SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY SUM(column1) DESC TOP 50";
    columns = Lists.newArrayList("sum(column1)");
    groupByKeys = Lists.newArrayList(new String[]{"P", "MaztCmmxxgguBUxPti"}, new String[]{"P", "HEuxNvH"},
        new String[]{"P", "KrNxpdycSiwoRohEiTIlLqDHnx"}, new String[]{"P", "oZgnrlDEtjjVpUoFLol"},
        new String[]{"o", "MaztCmmxxgguBUxPti"}, new String[]{"P", "dJWwFk"}, new String[]{"o", "HEuxNvH"},
        new String[]{"t", "MaztCmmxxgguBUxPti"}, new String[]{"P", "TTltMtFiRqUjvOG"}, new String[]{"t", "HEuxNvH"},
        new String[]{"o", "KrNxpdycSiwoRohEiTIlLqDHnx"}, new String[]{"", "HEuxNvH"},
        new String[]{"t", "KrNxpdycSiwoRohEiTIlLqDHnx"}, new String[]{"P", "fykKFqiw"}, new String[]{"t", "dJWwFk"},
        new String[]{"", "MaztCmmxxgguBUxPti"}, new String[]{"o", "dJWwFk"}, new String[]{"t", "oZgnrlDEtjjVpUoFLol"},
        new String[]{"P", "gFuH"}, new String[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx"},
        new String[]{"o", "oZgnrlDEtjjVpUoFLol"}, new String[]{"t", "TTltMtFiRqUjvOG"}, new String[]{"t", "fykKFqiw"},
        new String[]{"t", "gFuH"}, new String[]{"o", "TTltMtFiRqUjvOG"}, new String[]{"P", "XcBNHe"},
        new String[]{"o", "fykKFqiw"}, new String[]{"", "dJWwFk"}, new String[]{"gFuH", "HEuxNvH"},
        new String[]{"gFuH", "MaztCmmxxgguBUxPti"}, new String[]{"", "oZgnrlDEtjjVpUoFLol"},
        new String[]{"t", "XcBNHe"}, new String[]{"gFuH", "KrNxpdycSiwoRohEiTIlLqDHnx"}, new String[]{"o", "gFuH"});
    results = Lists.newArrayList(new Serializable[]{27177029040008.00000}, new Serializable[]{21998672845052.00000},
        new Serializable[]{18069909216728.00000}, new Serializable[]{8345501392852.00000},
        new Serializable[]{6905624581072.00000}, new Serializable[]{6224665921376.00000},
        new Serializable[]{5026384681784.00000}, new Serializable[]{4492405624940.00000},
        new Serializable[]{4462670055540.00000}, new Serializable[]{4424489490364.00000},
        new Serializable[]{4051812250524.00000}, new Serializable[]{3789390396216.00000},
        new Serializable[]{3529048341192.00000}, new Serializable[]{1574451324140.00000},
        new Serializable[]{1349058948804.00000}, new Serializable[]{1333941430664.00000},
        new Serializable[]{1152689463360.00000}, new Serializable[]{1039101333316.00000},
        new Serializable[]{860077643636.00000}, new Serializable[]{733802350944.00000},
        new Serializable[]{699381633640.00000}, new Serializable[]{675238030848.00000},
        new Serializable[]{480973878052.00000}, new Serializable[]{330331507792.00000},
        new Serializable[]{203835153352.00000}, new Serializable[]{120021767504.00000},
        new Serializable[]{62975165296.00000}, new Serializable[]{55470665124.0000},
        new Serializable[]{29872400856.00000}, new Serializable[]{29170832184.00000},
        new Serializable[]{22680162504.00000}, new Serializable[]{11276063956.00000},
        new Serializable[]{4159552848.00000}, new Serializable[]{2628604920.00000});
    numEntriesScannedPostFilter = 360000;
    data.add(new Object[]{query, columns, groupByKeys, results, numEntriesScannedPostFilter});

    query = "SELECT MIN(column6) FROM testTable GROUP BY column12 ORDER BY MIN(column6) DESC, column12";
    columns = Lists.newArrayList("min(column6)", "column12");
    groupByKeys = Lists.newArrayList(new String[]{"XcBNHe"}, new String[]{"fykKFqiw"}, new String[]{"gFuH"},
        new String[]{"HEuxNvH"}, new String[]{"MaztCmmxxgguBUxPti"}, new String[]{"dJWwFk"},
        new String[]{"KrNxpdycSiwoRohEiTIlLqDHnx"}, new String[]{"TTltMtFiRqUjvOG"},
        new String[]{"oZgnrlDEtjjVpUoFLol"});
    results = Lists.newArrayList(new Serializable[]{329467557.00000}, new Serializable[]{296467636.00000},
        new Serializable[]{296467636.00000}, new Serializable[]{6043515.00000}, new Serializable[]{6043515.00000},
        new Serializable[]{6043515.00000}, new Serializable[]{1980174.00000}, new Serializable[]{1980174.00000},
        new Serializable[]{1689277.00000});
    numEntriesScannedPostFilter = 240000;
    data.add(new Object[]{query, columns, groupByKeys, results, numEntriesScannedPostFilter});

    // TODO: handle non Number aggregations (AVG, DISTINCTCOUNTHLL etc) once implementation

    return data.toArray(new Object[data.size()][]);
  }
}
