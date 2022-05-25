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
package org.apache.pinot.core.operator.filter;

import java.util.Arrays;
import java.util.Collections;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertTrue;


public class FilterOperatorUtilsTest {
  private static final QueryContext QUERY_CONTEXT = mock(QueryContext.class);
  private static final int NUM_DOCS = 10;
  private static final BaseFilterOperator EMPTY_FILTER_OPERATOR = EmptyFilterOperator.getInstance();
  private static final BaseFilterOperator MATCH_ALL_FILTER_OPERATOR = new MatchAllFilterOperator(NUM_DOCS);
  private static final BaseFilterOperator REGULAR_FILTER_OPERATOR = new TestFilterOperator(new int[]{1, 4, 7});

  @Test
  public void testGetAndFilterOperator() {
    BaseFilterOperator filterOperator =
        FilterOperatorUtils.getAndFilterOperator(QUERY_CONTEXT, Collections.emptyList(), NUM_DOCS);
    assertTrue(filterOperator instanceof MatchAllFilterOperator);

    filterOperator =
        FilterOperatorUtils.getAndFilterOperator(QUERY_CONTEXT, Collections.singletonList(EMPTY_FILTER_OPERATOR),
            NUM_DOCS);
    assertTrue(filterOperator instanceof EmptyFilterOperator);

    filterOperator =
        FilterOperatorUtils.getAndFilterOperator(QUERY_CONTEXT, Collections.singletonList(MATCH_ALL_FILTER_OPERATOR),
            NUM_DOCS);
    assertTrue(filterOperator instanceof MatchAllFilterOperator);

    filterOperator =
        FilterOperatorUtils.getAndFilterOperator(QUERY_CONTEXT, Collections.singletonList(REGULAR_FILTER_OPERATOR),
            NUM_DOCS);
    assertTrue(filterOperator instanceof TestFilterOperator);

    filterOperator = FilterOperatorUtils.getAndFilterOperator(QUERY_CONTEXT,
        Arrays.asList(EMPTY_FILTER_OPERATOR, MATCH_ALL_FILTER_OPERATOR), NUM_DOCS);
    assertTrue(filterOperator instanceof EmptyFilterOperator);

    filterOperator = FilterOperatorUtils.getAndFilterOperator(QUERY_CONTEXT,
        Arrays.asList(EMPTY_FILTER_OPERATOR, REGULAR_FILTER_OPERATOR), NUM_DOCS);
    assertTrue(filterOperator instanceof EmptyFilterOperator);

    filterOperator = FilterOperatorUtils.getAndFilterOperator(QUERY_CONTEXT,
        Arrays.asList(MATCH_ALL_FILTER_OPERATOR, REGULAR_FILTER_OPERATOR), NUM_DOCS);
    assertTrue(filterOperator instanceof TestFilterOperator);
  }

  @Test
  public void testGetOrFilterOperator() {
    BaseFilterOperator filterOperator =
        FilterOperatorUtils.getOrFilterOperator(QUERY_CONTEXT, Collections.emptyList(), NUM_DOCS);
    assertTrue(filterOperator instanceof EmptyFilterOperator);

    filterOperator =
        FilterOperatorUtils.getOrFilterOperator(QUERY_CONTEXT, Collections.singletonList(EMPTY_FILTER_OPERATOR),
            NUM_DOCS);
    assertTrue(filterOperator instanceof EmptyFilterOperator);

    filterOperator =
        FilterOperatorUtils.getOrFilterOperator(QUERY_CONTEXT, Collections.singletonList(MATCH_ALL_FILTER_OPERATOR),
            NUM_DOCS);
    assertTrue(filterOperator instanceof MatchAllFilterOperator);

    filterOperator =
        FilterOperatorUtils.getOrFilterOperator(QUERY_CONTEXT, Collections.singletonList(REGULAR_FILTER_OPERATOR),
            NUM_DOCS);
    assertTrue(filterOperator instanceof TestFilterOperator);

    filterOperator = FilterOperatorUtils.getOrFilterOperator(QUERY_CONTEXT,
        Arrays.asList(EMPTY_FILTER_OPERATOR, MATCH_ALL_FILTER_OPERATOR), NUM_DOCS);
    assertTrue(filterOperator instanceof MatchAllFilterOperator);

    filterOperator = FilterOperatorUtils.getOrFilterOperator(QUERY_CONTEXT,
        Arrays.asList(EMPTY_FILTER_OPERATOR, REGULAR_FILTER_OPERATOR), NUM_DOCS);
    assertTrue(filterOperator instanceof TestFilterOperator);

    filterOperator = FilterOperatorUtils.getOrFilterOperator(QUERY_CONTEXT,
        Arrays.asList(MATCH_ALL_FILTER_OPERATOR, REGULAR_FILTER_OPERATOR), NUM_DOCS);
    assertTrue(filterOperator instanceof MatchAllFilterOperator);
  }
}
