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

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BaseFilterOperatorTest {

  @Test
  public void testBaseWithFalses() {
    int numDocs = 10;
    int[] docIds = new int[]{0, 1, 2, 3};
    TestFilterOperator testFilterOperator = new TestFilterOperator(docIds, numDocs);

    Assert.assertEquals(TestUtils.getDocIds(testFilterOperator.getTrues()), ImmutableList.of(0, 1, 2, 3));
    Assert.assertEquals(TestUtils.getDocIds(testFilterOperator.getFalses()), ImmutableList.of(4, 5, 6, 7, 8, 9));
  }

  @Test
  public void testBaseWithNullHandling() {
    int numDocs = 10;
    int[] docIds = new int[]{0, 1, 2, 3};
    int[] nullDocIds = new int[]{4, 5, 6, 7, 8, 9};
    TestFilterOperator testFilterOperator = new TestFilterOperator(docIds, nullDocIds, numDocs);

    Assert.assertEquals(TestUtils.getDocIds(testFilterOperator.getTrues()), ImmutableList.of(0, 1, 2, 3));
    Assert.assertEquals(TestUtils.getDocIds(testFilterOperator.getFalses()), Collections.emptyList());
  }

  @Test
  public void testBaseWithAllTrue() {
    int numDocs = 10;
    int[] docIds = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    TestFilterOperator testFilterOperator = new TestFilterOperator(docIds, numDocs);
    Assert.assertEquals(TestUtils.getDocIds(testFilterOperator.getTrues()),
        ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    Assert.assertEquals(TestUtils.getDocIds(testFilterOperator.getFalses()), Collections.emptyList());
  }

  @Test
  public void testBaseWithAllFalse() {
    int numDocs = 10;
    int[] docIds = new int[]{};
    TestFilterOperator testFilterOperator = new TestFilterOperator(docIds, numDocs);
    Assert.assertEquals(TestUtils.getDocIds(testFilterOperator.getTrues()), Collections.emptyList());
    Assert.assertEquals(TestUtils.getDocIds(testFilterOperator.getFalses()),
        ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
  }

  @Test
  public void testBaseWithAllNulls() {
    int numDocs = 10;
    int[] docIds = new int[]{};
    int[] nullDocIds = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    TestFilterOperator testFilterOperator = new TestFilterOperator(docIds, nullDocIds, numDocs);
    Assert.assertEquals(TestUtils.getDocIds(testFilterOperator.getTrues()), Collections.emptyList());
    Assert.assertEquals(TestUtils.getDocIds(testFilterOperator.getFalses()), Collections.emptyList());
  }
}
