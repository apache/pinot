/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.predicate;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.utils.EqualityUtils;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.BaseInPredicate;
import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link InPredicate}
 */
public class InPredicateTest {

  /**
   * This test ensures the FilterQueryTree & InPredicate are constructed correctly for both cases of:
   * <ul>
   *   <li> Splitting the in clause values. </li>
   *   <li> Joining the in clause values with delimiter. </li>
   * </ul>
   */
  @Test
  public void testSplitInClause() {
    String query = "select * from foo where values in ('abc', 'xyz', '123')";
    testSplit(query);
  }

  @Test
  public void testSplitNotInClause() {
    String query = "select * from foo where values not in ('abc', 'xyz', '123')";
    testSplit(query);
  }

  private void testSplit(String query) {
    Pql2Compiler compiler = new Pql2Compiler();

    String[] expectedValues = new String[]{"abc", "xyz", "123"};
    Arrays.sort(expectedValues); /* InPredicateAstNode sorts the predicate values. */

    /* Ensure that predicates are returned as separate strings, and not one concatenation of all strings. */
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(query);
    FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
    BaseInPredicate predicate = (BaseInPredicate) Predicate.newPredicate(filterQueryTree);
    String[] actualValues = predicate.getValues();
    Arrays.sort(actualValues);

    Assert.assertEquals(actualValues, expectedValues);
    Assert.assertTrue(EqualityUtils.isEqualIgnoreOrder(filterQueryTree.getValue(), Arrays.asList(expectedValues)));
  }
}

