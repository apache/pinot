/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.request;

import org.testng.Assert;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;


public class FilterOptimizerTest {
//  @Test
  public void test1() throws Exception {

    Pql2Compiler pql2Compiler = new Pql2Compiler();
    BrokerRequest req;
    FilterQueryTree tree;

    req = pql2Compiler.compileToBrokerRequest("SELECT * FROM T WHERE (A = 4 AND B = 5) AND (C=7)");
    tree = RequestUtils.generateFilterQueryTree(req);
    Assert.assertEquals(tree.getChildren().size(), 3);
    Assert.assertEquals(tree.getOperator(), FilterOperator.AND);
    for (FilterQueryTree node: tree.getChildren()) {
      Assert.assertNull(node.getChildren());
      Assert.assertEquals(node.getOperator(), FilterOperator.EQUALITY);
    }

    req = pql2Compiler.compileToBrokerRequest("SELECT * FROM T WHERE ((A = 4 AND B = 5) AND (C=7)) AND D=8");
    tree = RequestUtils.generateFilterQueryTree(req);
    Assert.assertEquals(tree.getChildren().size(), 4);
    Assert.assertEquals(tree.getOperator(), FilterOperator.AND);
    for (FilterQueryTree node: tree.getChildren()) {
      Assert.assertNull(node.getChildren());
      Assert.assertEquals(node.getOperator(), FilterOperator.EQUALITY);
    }

    req = pql2Compiler.compileToBrokerRequest("SELECT * FROM T WHERE (A = 4 OR B = 5) OR (C=7)");
    tree = RequestUtils.generateFilterQueryTree(req);
    Assert.assertEquals(tree.getChildren().size(), 3);
    Assert.assertEquals(tree.getOperator(), FilterOperator.OR);
    for (FilterQueryTree node: tree.getChildren()) {
      Assert.assertNull(node.getChildren());
      Assert.assertEquals(node.getOperator(), FilterOperator.EQUALITY);
    }

    req = pql2Compiler.compileToBrokerRequest("SELECT * FROM T WHERE ((A = 4 OR B = 5) OR (C=7)) OR D=8");
    tree = RequestUtils.generateFilterQueryTree(req);
    Assert.assertEquals(tree.getChildren().size(), 4);
    Assert.assertEquals(tree.getOperator(), FilterOperator.OR);
    for (FilterQueryTree node: tree.getChildren()) {
      Assert.assertNull(node.getChildren());
      Assert.assertEquals(node.getOperator(), FilterOperator.EQUALITY);
    }
  }
}
