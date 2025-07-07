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
import java.util.List;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.testng.annotations.Test;


/**
 * FST-based regexp like queries test.
 * Extends the base class and uses FST index type.
 */
public class FSTBasedRegexpLikeQueriesTest extends BaseFSTBasedRegexpLikeQueriesTest {

  @Override
  protected String getIndexType() {
    return "fst";
  }

  @Test
  public void testFSTBasedRegexLike() {
    // Select queries on col with FST + inverted index.
    String query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.sd.domain1.*') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, '.*domain1.*') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 512, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, '.*domain.*') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 1024, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, '.*com') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);

    // Select queries on col with just FST index.
    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, 'www.domain1.*') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, 'www.domain1.*') LIMIT 5";
    List<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{1000, "www.domain1.com/a"});
    expected.add(new Object[]{1001, "www.domain1.co.ab/b"});
    expected.add(new Object[]{1002, "www.domain1.co.bc/c"});
    expected.add(new Object[]{1003, "www.domain1.co.cd/d"});
    expected.add(new Object[]{1016, "www.domain1.com/a"});
    testInnerSegmentSelectionQuery(query, 5, expected);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, 'www.sd.domain1.*') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*domain1.*') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 512, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*domain.*') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 1024, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*domain.*') LIMIT 5";
    expected = new ArrayList<>();
    expected.add(new Object[]{1000, "www.domain1.com/a"});
    expected.add(new Object[]{1001, "www.domain1.co.ab/b"});
    expected.add(new Object[]{1002, "www.domain1.co.bc/c"});
    expected.add(new Object[]{1003, "www.domain1.co.cd/d"});
    expected.add(new Object[]{1004, "www.sd.domain1.com/a"});
    testInnerSegmentSelectionQuery(query, 5, expected);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') LIMIT 5";
    expected = new ArrayList<>();
    expected.add(new Object[]{1000, "www.domain1.com/a"});
    expected.add(new Object[]{1004, "www.sd.domain1.com/b"});
    expected.add(new Object[]{1008, "www.domain2.com/c"});
    expected.add(new Object[]{1012, "www.sd.domain2.co.cd/d"});
    expected.add(new Object[]{1016, "www.domain1.com/a"});
    testInnerSegmentSelectionQuery(query, 5, null);
  }

  @Test
  public void testLikeOperator() {
    String query = "SELECT INT_COL, URL_COL FROM MyTable WHERE DOMAIN_NAMES LIKE 'www.dom_in1.com' LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 64, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE DOMAIN_NAMES LIKE 'www.do_ai%' LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 512, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE DOMAIN_NAMES LIKE 'www.domain1%' LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE DOMAIN_NAMES LIKE 'www.sd.domain1%' LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE DOMAIN_NAMES LIKE '%domain1%' LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 512, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE DOMAIN_NAMES LIKE '%com' LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);
  }

  @Test
  public void testFSTBasedRegexpLikeWithOtherFilters() {
    // Select queries on columns with combination of FST Index , (FST + Inverted Index), No index and other constraints.
    String query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') AND "
        + "REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 52, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/b') AND "
        + "REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 51, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') AND "
        + "REGEXP_LIKE(URL_COL, '.*/a') AND REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 13, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.co\\..*') AND "
        + "REGEXP_LIKE(URL_COL, '.*/a') AND REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 0, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.co\\..*') AND "
        + "REGEXP_LIKE(URL_COL, '.*/b') AND REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 12, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') AND "
        + "REGEXP_LIKE(NO_INDEX_COL, 'test1') AND INT_COL=1000 LIMIT 50000";
    List<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{1000, "www.domain1.com/a"});
    testInnerSegmentSelectionQuery(query, 1, expected);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') AND "
        + "REGEXP_LIKE(URL_COL, '.*/b') AND REGEXP_LIKE(NO_INDEX_COL, 'test2') AND INT_COL=1001 LIMIT 50000";
    expected = new ArrayList<>();
    expected.add(new Object[]{1001, "www.domain1.co.ab/b"});
    testInnerSegmentSelectionQuery(query, 1, expected);
  }

  @Test
  public void testGroupByOnFSTBasedRegexpLike() {
    String query;
    query = "SELECT DOMAIN_NAMES, count(*) FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') GROUP BY "
        + "DOMAIN_NAMES LIMIT 50000";
    AggregationGroupByResult result = getGroupByResults(query);
    matchGroupResult(result, "www.domain1.com", 64);
    matchGroupResult(result, "www.domain1.co.ab", 64);
    matchGroupResult(result, "www.domain1.co.bc", 64);
    matchGroupResult(result, "www.domain1.co.cd", 64);

    query = "SELECT URL_COL, count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') AND "
        + "REGEXP_LIKE(NO_INDEX_COL, 'test1') GROUP BY URL_COL LIMIT 5000";
    result = getGroupByResults(query);
    matchGroupResult(result, "www.domain1.com/a", 13);
    matchGroupResult(result, "www.sd.domain1.com/a", 13);
    matchGroupResult(result, "www.domain2.com/a", 13);
    matchGroupResult(result, "www.sd.domain2.com/a", 13);

    query = "SELECT URL_COL, count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/b') AND "
        + "REGEXP_LIKE(NO_INDEX_COL, 'test1') GROUP BY URL_COL LIMIT 5000";
    result = getGroupByResults(query);
    matchGroupResult(result, "www.domain1.co.ab/b", 12);
    matchGroupResult(result, "www.sd.domain1.co.ab/b", 13);
    matchGroupResult(result, "www.domain2.co.ab/b", 13);
    matchGroupResult(result, "www.sd.domain2.co.ab/b", 13);

    query = "SELECT URL_COL, count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/b') AND "
        + "REGEXP_LIKE(NO_INDEX_COL, 'test1') AND INT_COL > 1005 GROUP BY URL_COL LIMIT 5000";
    result = getGroupByResults(query);
    matchGroupResult(result, "www.domain1.co.ab/b", 12);
    matchGroupResult(result, "www.sd.domain1.co.ab/b", 12);
    matchGroupResult(result, "www.domain2.co.ab/b", 13);
    matchGroupResult(result, "www.sd.domain2.co.ab/b", 13);

    query = "SELECT URL_COL, count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, 'www.domain1.*/a') GROUP BY URL_COL "
        + "LIMIT 50000";
    result = getGroupByResults(query);
    matchGroupResult(result, "www.domain1.com/a", 64);
  }

  @Test
  public void testInterSegment() {
    String query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') AND "
        + "REGEXP_LIKE(URL_COL, '.*/b') AND REGEXP_LIKE(NO_INDEX_COL, 'test2') AND INT_COL=1001 LIMIT 50000";
    List<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{1001, "www.domain1.co.ab/b"});
    expected.add(new Object[]{1001, "www.domain1.co.ab/b"});
    expected.add(new Object[]{1001, "www.domain1.co.ab/b"});
    expected.add(new Object[]{1001, "www.domain1.co.ab/b"});
    testInterSegmentsSelectionQuery(query, 4, expected);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') AND "
        + "REGEXP_LIKE(NO_INDEX_COL, 'test1') AND INT_COL=1000 LIMIT 50000";
    expected = new ArrayList<>();
    expected.add(new Object[]{1000, "www.domain1.com/a"});
    expected.add(new Object[]{1000, "www.domain1.com/a"});
    expected.add(new Object[]{1000, "www.domain1.com/a"});
    expected.add(new Object[]{1000, "www.domain1.com/a"});
    testInterSegmentsSelectionQuery(query, 4, expected);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.co\\..*') AND "
        + "REGEXP_LIKE(URL_COL, '.*/b') AND REGEXP_LIKE(NO_INDEX_COL, 'test1') ORDER  BY INT_COL LIMIT 5000";
    testInterSegmentsSelectionQuery(query, 48, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.co\\..*') AND "
        + "REGEXP_LIKE(URL_COL, '.*/a') AND REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testInterSegmentsSelectionQuery(query, 0, null);

    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*') AND "
        + "REGEXP_LIKE(URL_COL, '.*/a') AND REGEXP_LIKE(NO_INDEX_COL, 'test1') LIMIT 50000";
    testInterSegmentsSelectionQuery(query, 52, null);

    query = "SELECT count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, 'www.domain1.*/a')";
    testInterSegmentsCountQuery(query, 256);

    query = "SELECT count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/b') AND REGEXP_LIKE(NO_INDEX_COL, 'test1') "
        + "AND INT_COL > 1005 ";
    testInterSegmentsCountQuery(query, 200);

    query = "SELECT count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/b') AND REGEXP_LIKE(NO_INDEX_COL, 'test1')";
    testInterSegmentsCountQuery(query, 204);

    query = "SELECT count(*) FROM MyTable WHERE REGEXP_LIKE(URL_COL, '.*/a') AND REGEXP_LIKE(NO_INDEX_COL, 'test1')";
    testInterSegmentsCountQuery(query, 208);

    query = "SELECT count(*) FROM MyTable WHERE REGEXP_LIKE(DOMAIN_NAMES, 'www.domain1.*')";
    testInterSegmentsCountQuery(query, 1024);
  }
}
