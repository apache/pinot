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
package org.apache.pinot.core.query.aggregation.groupby;

import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class GroupByResultHolderTest {

  @Test(dataProvider = "groupByResultHolderCapacityDataProvider")
  public void testGetGroupByResultHolderCapacity(String query, Integer expectedCapacity) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    Assert.assertEquals(queryContext.getMaxInitialResultHolderCapacity(), expectedCapacity);
  }

  @DataProvider(name = "groupByResultHolderCapacityDataProvider")
  public Object[][] groupByResultHolderCapacityDataProvider() {
    return new Object[][]{
        // Single IN predicate
        {"SELECT COUNT(column1), MAX(column1) FROM testTable WHERE column1 IN (10, 20, 30) GROUP BY column1 LIMIT 10",
            3},
        // Single EQ predicate
        {"SELECT COUNT(column1), MAX(column1) FROM testTable WHERE column1 = 10 GROUP BY column1 LIMIT 10", 1},
        // Multiple IN predicates
        {"SELECT COUNT(column1), MAX(column1) FROM testTable WHERE column1 IN (10, 20, 30) AND column3 IN (40, 50)"
            + " GROUP BY column1, column3 LIMIT 10", 3},
        // Multiple EQ predicates
        {"SELECT COUNT(column1), MAX(column1) FROM testTable WHERE column1 = 10 AND column3 = 40 GROUP BY column1,"
            + " column3 LIMIT 10", 1},
        // Mixed predicates
        {"SELECT COUNT(column1), MAX(column1) FROM testTable WHERE column1 IN (10, 20, 30) AND column3 = 40"
            + " GROUP BY column1, column3 LIMIT 10", 3},
        {"SELECT COUNT(column1), MAX(column1) FROM testTable WHERE column1 = 10 AND column3 IN (40, 50)"
            + " GROUP BY column1, column3 LIMIT 10", 2},
        // No filter
        {"SELECT COUNT(column1), MAX(column1) FROM testTable GROUP BY column1 LIMIT 10",
            InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY},
        // No matching filter EQ predicate in group-by expression
        {"SELECT COUNT(column1), MAX(column1) FROM testTable WHERE column1 = 10 GROUP BY column2 LIMIT 10",
            InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY},
        // No matching filter IN predicate in group-by expression
        {"SELECT COUNT(column1), MAX(column1) FROM testTable WHERE column1 IN (10, 20, 30) GROUP BY column2 LIMIT 10",
            InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY},
        // Only one matching filter predicate in group-by expression
        {"SELECT COUNT(column1), MAX(column1) FROM testTable WHERE column1 IN (10, 20, 30) GROUP BY column1, column2"
            + " LIMIT 10", InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY},
    };
  }
}
