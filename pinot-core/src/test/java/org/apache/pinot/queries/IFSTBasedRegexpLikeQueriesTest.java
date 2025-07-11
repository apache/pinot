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
 * IFST-based regexp like queries test.
 * Extends the base class and uses IFST index type.
 */
public class IFSTBasedRegexpLikeQueriesTest extends BaseFSTBasedRegexpLikeQueriesTest {

  @Override
  protected String getIndexType() {
    return "ifst";
  }

  @Test
  public void testIFSTBasedRegexpLike() {
    // Test : Basic IFST matching (case-insensitive pattern should match data)
    String query =
        "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE_CI(DOMAIN_NAMES, 'WWW.DOMAIN1.*') LIMIT 50000";
    testInnerSegmentSelectionQuery(query, 256, null);

    // Test : Selection query with case-insensitive pattern
    query = "SELECT INT_COL, URL_COL FROM MyTable WHERE REGEXP_LIKE_CI(DOMAIN_NAMES, 'WWW.DOMAIN1.*') LIMIT 5";
    List<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{1000, "www.domain1.com/a"});
    expected.add(new Object[]{1001, "www.domain1.co.ab/b"});
    expected.add(new Object[]{1002, "www.domain1.co.bc/c"});
    expected.add(new Object[]{1003, "www.domain1.co.cd/d"});
    expected.add(new Object[]{1016, "www.domain1.com/a"});
    testInnerSegmentSelectionQuery(query, 5, expected);

    // Test : GroupBy with case-insensitive pattern
    query = "SELECT DOMAIN_NAMES, count(*) FROM MyTable WHERE REGEXP_LIKE_CI(DOMAIN_NAMES, 'WWW.DOMAIN1.*') GROUP BY "
        + "DOMAIN_NAMES LIMIT 50000";
    AggregationGroupByResult result = getGroupByResults(query);
    matchGroupResult(result, "www.domain1.com", 64);
    matchGroupResult(result, "www.domain1.co.ab", 64);
    matchGroupResult(result, "www.domain1.co.bc", 64);
    matchGroupResult(result, "www.domain1.co.cd", 64);

    query = "SELECT URL_COL, count(*) FROM MyTable WHERE REGEXP_LIKE_CI(URL_COL, '.*/A') AND "
        + "REGEXP_LIKE_CI(NO_INDEX_COL, 'test1') GROUP BY URL_COL LIMIT 5000";
    result = getGroupByResults(query);
    matchGroupResult(result, "www.domain1.com/a", 13);
    matchGroupResult(result, "www.sd.domain1.com/a", 13);
    matchGroupResult(result, "www.domain2.com/a", 13);
    matchGroupResult(result, "www.sd.domain2.com/a", 13);
  }
}
