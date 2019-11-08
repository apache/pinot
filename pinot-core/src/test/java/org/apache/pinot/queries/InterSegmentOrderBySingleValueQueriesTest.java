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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.CommonConstants.Broker.Request;
import org.apache.pinot.common.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.CommonConstants.Broker.Request.*;


/**
 * Tests order by queries
 */
public class InterSegmentOrderBySingleValueQueriesTest extends BaseSingleValueQueriesTest {

  @Test(dataProvider = "orderByResultTableProvider")
  public void testGroupByOrderBy(String query, List<Serializable[]> expectedResults, long expectedNumDocsScanned,
      long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter, long expectedNumTotalDocs) {
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.GROUP_BY_MODE, SQL);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, SQL);
    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query, queryOptions);
    QueriesTestUtils.testInterSegmentGroupByOrderByResult(brokerResponse, expectedNumDocsScanned,
        expectedNumEntriesScannedInFilter, expectedNumEntriesScannedPostFilter, expectedNumTotalDocs, expectedResults);
  }

  /**
   * Tests the query options for groupByMode, responseFormat.
   * pql, pql - does not execute order by, returns aggregationResults
   * pql, sql - does not execute order by, returns aggregationResults
   * sql, pql - executes order by, but returns aggregationResults. Keys across all aggregations will be same
   * sql, sql - executes order by, returns resultsTable
   */
  @Test
  public void testQueryOptions() {
    String query = "SELECT SUM(column1), MIN(column6) FROM testTable GROUP BY column11 ORDER BY column11";
    Map<String, String> queryOptions = null;

    // default PQL, PQL
    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query, queryOptions);
    Assert.assertNotNull(brokerResponse.getAggregationResults());
    Assert.assertNull(brokerResponse.getResultTable());

    // PQL, PQL - don't execute order by, return aggregationResults
    queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.GROUP_BY_MODE, PQL);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, PQL);
    brokerResponse = getBrokerResponseForQuery(query, queryOptions);
    Assert.assertNotNull(brokerResponse.getAggregationResults());
    Assert.assertNull(brokerResponse.getResultTable());

    // PQL, SQL - don't execute order by, return aggregationResults.
    queryOptions.put(QueryOptionKey.GROUP_BY_MODE, PQL);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, SQL);
    brokerResponse = getBrokerResponseForQuery(query, queryOptions);
    Assert.assertNotNull(brokerResponse.getAggregationResults());
    Assert.assertNull(brokerResponse.getResultTable());

    // SQL, PQL - execute the order by, but return aggregationResults. Keys should be same across aggregation functions.
    queryOptions.put(QueryOptionKey.GROUP_BY_MODE, SQL);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, PQL);
    brokerResponse = getBrokerResponseForQuery(query, queryOptions);
    Assert.assertNotNull(brokerResponse.getAggregationResults());
    Assert.assertNull(brokerResponse.getResultTable());
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), 2);
    Iterator<GroupByResult> it1 = aggregationResults.get(0).getGroupByResult().iterator();
    Iterator<GroupByResult> it2 = aggregationResults.get(1).getGroupByResult().iterator();
    while (it1.hasNext() && it2.hasNext()) {
      GroupByResult groupByResult1 = it1.next();
      GroupByResult groupByResult2 = it2.next();
      Assert.assertEquals(groupByResult1.getGroup(), groupByResult2.getGroup());
    }

    // SQL, SQL - execute order by, return resultsTable
    queryOptions.put(QueryOptionKey.GROUP_BY_MODE, SQL);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, SQL);
    brokerResponse = getBrokerResponseForQuery(query, queryOptions);
    Assert.assertNull(brokerResponse.getAggregationResults());
    Assert.assertNotNull(brokerResponse.getResultTable());
  }

  /**
   * Provides various combinations of order by in ResultTable.
   * In order to calculate the expected results, the results from a group by were taken, and then ordered accordingly.
   */
  @DataProvider(name = "orderByResultTableProvider")
  public Object[][] orderByResultTableProvider() {

    List<Object[]> data = new ArrayList<>();
    String query;
    List<Serializable[]> results;
    long numDocsScanned = 120000;
    long numEntriesScannedInFilter = 0;
    long numEntriesScannedPostFilter;
    long numTotalDocs = 120000;

    // order by one of the group by columns
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11 ORDER BY column11";
    results = Lists.newArrayList(new Serializable[]{"", 5935285005452.0}, new Serializable[]{"P", 88832999206836.0},
        new Serializable[]{"gFuH", 63202785888.0}, new Serializable[]{"o", 18105331533948.0},
        new Serializable[]{"t", 16331923219264.0});
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // order by one of the group by columns DESC
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11 ORDER BY column11 DESC";
    results = Lists.newArrayList(results);
    Collections.reverse(results);
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // order by one of the group by columns, TOP less than default
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11 ORDER BY column11 TOP 3";
    results = Lists.newArrayList(results);
    Collections.reverse(results);
    results = results.subList(0, 3);
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // group by 2 dimensions, order by both, tie breaker
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY column11, column12";
    results = Lists.newArrayList(new Serializable[]{"", "HEuxNvH", 3789390396216.0},
        new Serializable[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx", 733802350944.00000},
        new Serializable[]{"", "MaztCmmxxgguBUxPti", 1333941430664.00000},
        new Serializable[]{"", "dJWwFk", 55470665124.0000},
        new Serializable[]{"", "oZgnrlDEtjjVpUoFLol", 22680162504.00000},
        new Serializable[]{"P", "HEuxNvH", 21998672845052.00000},
        new Serializable[]{"P", "KrNxpdycSiwoRohEiTIlLqDHnx", 18069909216728.00000},
        new Serializable[]{"P", "MaztCmmxxgguBUxPti", 27177029040008.00000},
        new Serializable[]{"P", "TTltMtFiRqUjvOG", 4462670055540.00000},
        new Serializable[]{"P", "XcBNHe", 120021767504.00000});
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // group by 2 columns, order by both, TOP more than default
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY column11, column12 TOP 15";
    results = Lists.newArrayList(results);
    results.add(new Serializable[]{"P", "dJWwFk", 6224665921376.00000});
    results.add(new Serializable[]{"P", "fykKFqiw", 1574451324140.00000});
    results.add(new Serializable[]{"P", "gFuH", 860077643636.00000});
    results.add(new Serializable[]{"P", "oZgnrlDEtjjVpUoFLol", 8345501392852.00000});
    results.add(new Serializable[]{"gFuH", "HEuxNvH", 29872400856.00000});
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // group by 2 columns, order by both, one of them DESC
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY column11, column12 DESC";
    results = Lists.newArrayList(new Serializable[]{"", "oZgnrlDEtjjVpUoFLol", 22680162504.00000},
        new Serializable[]{"", "dJWwFk", 55470665124.0000},
        new Serializable[]{"", "MaztCmmxxgguBUxPti", 1333941430664.00000},
        new Serializable[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx", 733802350944.00000},
        new Serializable[]{"", "HEuxNvH", 3789390396216.00000},
        new Serializable[]{"P", "oZgnrlDEtjjVpUoFLol", 8345501392852.00000},
        new Serializable[]{"P", "gFuH", 860077643636.00000}, new Serializable[]{"P", "fykKFqiw", 1574451324140.00000},
        new Serializable[]{"P", "dJWwFk", 6224665921376.00000}, new Serializable[]{"P", "XcBNHe", 120021767504.00000});
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // order by group by column and an aggregation
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY column11, SUM(column1)";
    results = Lists.newArrayList(new Serializable[]{"", "oZgnrlDEtjjVpUoFLol", 22680162504.00000},
        new Serializable[]{"", "dJWwFk", 55470665124.0000},
        new Serializable[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx", 733802350944.00000},
        new Serializable[]{"", "MaztCmmxxgguBUxPti", 1333941430664.00000},
        new Serializable[]{"", "HEuxNvH", 3789390396216.00000}, new Serializable[]{"P", "XcBNHe", 120021767504.00000},
        new Serializable[]{"P", "gFuH", 860077643636.00000}, new Serializable[]{"P", "fykKFqiw", 1574451324140.00000},
        new Serializable[]{"P", "TTltMtFiRqUjvOG", 4462670055540.00000},
        new Serializable[]{"P", "dJWwFk", 6224665921376.00000});
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // order by only aggregation, DESC, TOP
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY SUM(column1) DESC TOP 50";
    results = Lists.newArrayList(new Serializable[]{"P", "MaztCmmxxgguBUxPti", 27177029040008.00000},
        new Serializable[]{"P", "HEuxNvH", 21998672845052.00000},
        new Serializable[]{"P", "KrNxpdycSiwoRohEiTIlLqDHnx", 18069909216728.00000},
        new Serializable[]{"P", "oZgnrlDEtjjVpUoFLol", 8345501392852.00000},
        new Serializable[]{"o", "MaztCmmxxgguBUxPti", 6905624581072.00000},
        new Serializable[]{"P", "dJWwFk", 6224665921376.00000}, new Serializable[]{"o", "HEuxNvH", 5026384681784.00000},
        new Serializable[]{"t", "MaztCmmxxgguBUxPti", 4492405624940.00000},
        new Serializable[]{"P", "TTltMtFiRqUjvOG", 4462670055540.00000},
        new Serializable[]{"t", "HEuxNvH", 4424489490364.00000},
        new Serializable[]{"o", "KrNxpdycSiwoRohEiTIlLqDHnx", 4051812250524.00000},
        new Serializable[]{"", "HEuxNvH", 3789390396216.00000},
        new Serializable[]{"t", "KrNxpdycSiwoRohEiTIlLqDHnx", 3529048341192.00000},
        new Serializable[]{"P", "fykKFqiw", 1574451324140.00000},
        new Serializable[]{"t", "dJWwFk", 1349058948804.00000},
        new Serializable[]{"", "MaztCmmxxgguBUxPti", 1333941430664.00000},
        new Serializable[]{"o", "dJWwFk", 1152689463360.00000},
        new Serializable[]{"t", "oZgnrlDEtjjVpUoFLol", 1039101333316.00000},
        new Serializable[]{"P", "gFuH", 860077643636.00000},
        new Serializable[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx", 733802350944.00000},
        new Serializable[]{"o", "oZgnrlDEtjjVpUoFLol", 699381633640.00000},
        new Serializable[]{"t", "TTltMtFiRqUjvOG", 675238030848.00000},
        new Serializable[]{"t", "fykKFqiw", 480973878052.00000}, new Serializable[]{"t", "gFuH", 330331507792.00000},
        new Serializable[]{"o", "TTltMtFiRqUjvOG", 203835153352.00000},
        new Serializable[]{"P", "XcBNHe", 120021767504.00000}, new Serializable[]{"o", "fykKFqiw", 62975165296.00000},
        new Serializable[]{"", "dJWwFk", 55470665124.0000}, new Serializable[]{"gFuH", "HEuxNvH", 29872400856.00000},
        new Serializable[]{"gFuH", "MaztCmmxxgguBUxPti", 29170832184.00000},
        new Serializable[]{"", "oZgnrlDEtjjVpUoFLol", 22680162504.00000},
        new Serializable[]{"t", "XcBNHe", 11276063956.00000},
        new Serializable[]{"gFuH", "KrNxpdycSiwoRohEiTIlLqDHnx", 4159552848.00000},
        new Serializable[]{"o", "gFuH", 2628604920.00000});
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // multiple aggregations
    query = "SELECT SUM(column1), MIN(column6) FROM testTable GROUP BY column11 ORDER BY column11";
    results = Lists.newArrayList(new Serializable[]{"", 5935285005452.0, 2.96467636E8},
        new Serializable[]{"P", 88832999206836.0, 1689277.0}, new Serializable[]{"gFuH", 63202785888.0, 2.96467636E8},
        new Serializable[]{"o", 18105331533948.0, 2.96467636E8}, new Serializable[]{"t", 16331923219264.0, 1980174.0});
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // order by aggregation with space/tab in order by
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY SUM  ( column1) DESC TOP 3";
    results = Lists.newArrayList(new Serializable[]{"P", "MaztCmmxxgguBUxPti", 27177029040008.00000},
        new Serializable[]{"P", "HEuxNvH", 21998672845052.00000},
        new Serializable[]{"P", "KrNxpdycSiwoRohEiTIlLqDHnx", 18069909216728.00000});
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // order by an aggregation DESC, and group by column
    query = "SELECT MIN(column6) FROM testTable GROUP BY column12 ORDER BY MIN(column6) DESC, column12";
    results = Lists
        .newArrayList(new Serializable[]{"XcBNHe", 329467557.00000}, new Serializable[]{"fykKFqiw", 296467636.00000},
            new Serializable[]{"gFuH", 296467636.00000}, new Serializable[]{"HEuxNvH", 6043515.00000},
            new Serializable[]{"MaztCmmxxgguBUxPti", 6043515.00000}, new Serializable[]{"dJWwFk", 6043515.00000},
            new Serializable[]{"KrNxpdycSiwoRohEiTIlLqDHnx", 1980174.00000},
            new Serializable[]{"TTltMtFiRqUjvOG", 1980174.00000},
            new Serializable[]{"oZgnrlDEtjjVpUoFLol", 1689277.00000});
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // numeric dimension should follow numeric ordering
    query = "select count(*) from testTable group by column17 order by column17 top 15";
    results = Lists.newArrayList(new Serializable[]{83386499, 2924L}, new Serializable[]{217787432, 3892L},
        new Serializable[]{227908817, 6564L}, new Serializable[]{402773817, 7304L},
        new Serializable[]{423049234, 6556L}, new Serializable[]{561673250, 7420L},
        new Serializable[]{635942547, 3308L}, new Serializable[]{638936844, 3816L},
        new Serializable[]{939479517, 3116L}, new Serializable[]{984091268, 3824L},
        new Serializable[]{1230252339, 5620L}, new Serializable[]{1284373442, 7428L},
        new Serializable[]{1555255521, 2900L}, new Serializable[]{1618904660, 2744L},
        new Serializable[]{1670085862, 3388L});
    numEntriesScannedPostFilter = 120000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // group by UDF order by UDF
    query = "SELECT COUNT(*) FROM testTable GROUP BY sub(column1, 100000) TOP 3 ORDER BY sub(column1, 100000)";
    results = Lists.newArrayList(new Serializable[]{140528.0, 28L}, new Serializable[]{194355.0, 12L},
        new Serializable[]{532157.0, 12L});
    numEntriesScannedPostFilter = 120000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // spaces in UDF
    query = "SELECT COUNT(*) FROM testTable GROUP BY sub(column1, 100000) TOP 3 ORDER BY SUB(   column1, 100000 )";
    results = Lists.newArrayList(new Serializable[]{140528.0, 28L}, new Serializable[]{194355.0, 12L},
        new Serializable[]{532157.0, 12L});
    numEntriesScannedPostFilter = 120000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // Object type aggregation - comparable intermediate results (AVG, MINMAXRANGE)
    query = "SELECT AVG(column6) FROM testTable GROUP BY column11  ORDER BY column11";
    results = Lists.newArrayList(new Serializable[]{"", 296467636.0}, new Serializable[]{"P", 909380310.3521485},
        new Serializable[]{"gFuH", 296467636.0}, new Serializable[]{"o", 296467636.0},
        new Serializable[]{"t", 526245333.3900426});
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    query = "SELECT AVG(column6) FROM testTable GROUP BY column11 ORDER BY AVG(column6), column11 DESC";
    results = Lists.newArrayList(new Serializable[]{"o", 296467636.0}, new Serializable[]{"gFuH", 296467636.0},
        new Serializable[]{"", 296467636.0}, new Serializable[]{"t", 526245333.3900426},
        new Serializable[]{"P", 909380310.3521485});
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // Object type aggregation - non comparable intermediate results (DISTINCTCOUNT)
    query = "SELECT DISTINCTCOUNT(column11) FROM testTable GROUP BY column12 ORDER BY column12";
    results = Lists.newArrayList(new Serializable[]{"HEuxNvH", 5}, new Serializable[]{"KrNxpdycSiwoRohEiTIlLqDHnx", 5},
        new Serializable[]{"MaztCmmxxgguBUxPti", 5}, new Serializable[]{"TTltMtFiRqUjvOG", 3},
        new Serializable[]{"XcBNHe", 2}, new Serializable[]{"dJWwFk", 4}, new Serializable[]{"fykKFqiw", 3},
        new Serializable[]{"gFuH", 3}, new Serializable[]{"oZgnrlDEtjjVpUoFLol", 4});
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    query =
        "SELECT DISTINCTCOUNT(column11) FROM testTable GROUP BY column12 ORDER BY DISTINCTCOUNT(column11), column12 DESC";
    results = Lists
        .newArrayList(new Serializable[]{"XcBNHe", 2}, new Serializable[]{"gFuH", 3}, new Serializable[]{"fykKFqiw", 3},
            new Serializable[]{"TTltMtFiRqUjvOG", 3}, new Serializable[]{"oZgnrlDEtjjVpUoFLol", 4},
            new Serializable[]{"dJWwFk", 4}, new Serializable[]{"MaztCmmxxgguBUxPti", 5},
            new Serializable[]{"KrNxpdycSiwoRohEiTIlLqDHnx", 5}, new Serializable[]{"HEuxNvH", 5});
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // empty results
    query =
        "SELECT MIN(column6) FROM testTable where column12='non-existent-value' GROUP BY column11 order by column11";
    results = new ArrayList<>(0);
    numDocsScanned = 0;
    numEntriesScannedPostFilter = 0;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    return data.toArray(new Object[data.size()][]);
  }
}