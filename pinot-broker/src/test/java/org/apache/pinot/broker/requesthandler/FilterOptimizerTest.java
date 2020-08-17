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
package org.apache.pinot.broker.requesthandler;

import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.requesthandler.BrokerRequestOptimizer;
import org.apache.pinot.core.requesthandler.FlattenNestedPredicatesFilterQueryTreeOptimizer;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FilterOptimizerTest {
  private final BrokerRequestOptimizer _optimizer = new BrokerRequestOptimizer();

  // Testing positive cases of flattening the query tree.
  @Test
  public void testPositive()
      throws Exception {

    Pql2Compiler pql2Compiler = new Pql2Compiler();
    BrokerRequest req;
    String timeColumn = null;
    FilterQueryTree tree;
    int numLeaves;
    int numOps;

    req = pql2Compiler.compileToBrokerRequest("SELECT * FROM T WHERE (A = 4 AND B = 5) AND (C=7)");
    tree = RequestUtils.generateFilterQueryTree(_optimizer.optimize(req, timeColumn));
    Assert.assertEquals(tree.getChildren().size(), 3);
    Assert.assertEquals(tree.getOperator(), FilterOperator.AND);
    for (FilterQueryTree node : tree.getChildren()) {
      Assert.assertNull(node.getChildren());
      Assert.assertEquals(node.getOperator(), FilterOperator.EQUALITY);
    }

    req = pql2Compiler.compileToBrokerRequest("SELECT * FROM T WHERE ((A = 4 AND B = 5) AND (C=7)) AND D=8");
    tree = RequestUtils.generateFilterQueryTree(_optimizer.optimize(req, timeColumn));
    Assert.assertEquals(tree.getChildren().size(), 4);
    Assert.assertEquals(tree.getOperator(), FilterOperator.AND);
    for (FilterQueryTree node : tree.getChildren()) {
      Assert.assertNull(node.getChildren());
      Assert.assertEquals(node.getOperator(), FilterOperator.EQUALITY);
    }

    req = pql2Compiler.compileToBrokerRequest("SELECT * FROM T WHERE (A = 4 OR B = 5) OR (C=7)");
    tree = RequestUtils.generateFilterQueryTree(_optimizer.optimize(req, timeColumn));
    Assert.assertEquals(tree.getChildren().size(), 3);
    Assert.assertEquals(tree.getOperator(), FilterOperator.OR);
    for (FilterQueryTree node : tree.getChildren()) {
      Assert.assertNull(node.getChildren());
      Assert.assertEquals(node.getOperator(), FilterOperator.EQUALITY);
    }

    req = pql2Compiler.compileToBrokerRequest("SELECT * FROM T WHERE ((A = 4 OR B = 5) OR (C=7)) OR D=8");
    tree = RequestUtils.generateFilterQueryTree(_optimizer.optimize(req, timeColumn));
    Assert.assertEquals(tree.getChildren().size(), 4);
    Assert.assertEquals(tree.getOperator(), FilterOperator.OR);
    for (FilterQueryTree node : tree.getChildren()) {
      Assert.assertNull(node.getChildren());
      Assert.assertEquals(node.getOperator(), FilterOperator.EQUALITY);
    }

    // 3-level test case
    req = pql2Compiler.compileToBrokerRequest("SELECT * FROM T WHERE ((A = 4 OR (B = 5 OR D = 9)) OR (C=7)) OR E=8");
    tree = RequestUtils.generateFilterQueryTree(_optimizer.optimize(req, timeColumn));
    Assert.assertEquals(tree.getChildren().size(), 5);
    Assert.assertEquals(tree.getOperator(), FilterOperator.OR);
    for (FilterQueryTree node : tree.getChildren()) {
      Assert.assertNull(node.getChildren());
      Assert.assertEquals(node.getOperator(), FilterOperator.EQUALITY);
    }

    // Mixed case.
    req = pql2Compiler.compileToBrokerRequest("SELECT * FROM T WHERE ((A = 4 OR (B = 5 AND D = 9)) OR (C=7)) OR E=8");
    tree = RequestUtils.generateFilterQueryTree(_optimizer.optimize(req, timeColumn));
    Assert.assertEquals(tree.getChildren().size(), 4);
    Assert.assertEquals(tree.getOperator(), FilterOperator.OR);
    numLeaves = 0;
    numOps = 0;
    for (FilterQueryTree node : tree.getChildren()) {
      if (node.getOperator().equals(FilterOperator.EQUALITY)) {
        Assert.assertNull(node.getChildren());
        numLeaves++;
      } else {
        Assert.assertNotNull(node.getChildren());
        Assert.assertEquals(node.getOperator(), FilterOperator.AND);
        numOps++;
      }
    }
    Assert.assertEquals(1, numOps);
    Assert.assertEquals(3, numLeaves);

    final int maxNodesAtTopLevel = FlattenNestedPredicatesFilterQueryTreeOptimizer.MAX_OPTIMIZING_DEPTH;
    String whereClause = constructWhereClause(FilterOperator.OR, maxNodesAtTopLevel + 50);
    req = pql2Compiler.compileToBrokerRequest("SELECT * FROM T WHERE " + whereClause);
    tree = RequestUtils.generateFilterQueryTree(_optimizer.optimize(req, timeColumn));
    Assert.assertEquals(tree.getChildren().size(), maxNodesAtTopLevel + 1);
    Assert.assertEquals(tree.getOperator(), FilterOperator.OR);
    numLeaves = 0;
    numOps = 0;
    for (FilterQueryTree node : tree.getChildren()) {
      if (node.getOperator().equals(FilterOperator.EQUALITY)) {
        Assert.assertNull(node.getChildren());
        numLeaves++;
      } else {
        Assert.assertNotNull(node.getChildren());
        numOps++;
      }
    }
    Assert.assertEquals(maxNodesAtTopLevel, numLeaves);
    Assert.assertEquals(1, numOps);
  }

  // Tests cases where we should not do any flattening.
  @Test
  public void testNegative()
      throws Exception {
    Pql2Compiler pql2Compiler = new Pql2Compiler();
    BrokerRequest req;
    FilterQueryTree tree;
    int numLeaves;
    int numOps;

    req = pql2Compiler.compileToBrokerRequest("SELECT * FROM T WHERE (A = 4 AND (B = 5 OR D = 9))");
    tree = RequestUtils.generateFilterQueryTree(_optimizer.optimize(req, null /* timeColumn */));
    Assert.assertEquals(tree.getChildren().size(), 2);
    Assert.assertEquals(tree.getOperator(), FilterOperator.AND);
    numOps = 0;
    numLeaves = 0;
    for (FilterQueryTree node : tree.getChildren()) {
      if (node.getOperator().equals(FilterOperator.OR)) {
        Assert.assertEquals(2, node.getChildren().size());
        numOps++;
      } else {
        numLeaves++;
      }
    }
    Assert.assertEquals(1, numOps);
    Assert.assertEquals(1, numLeaves);
  }

  private String constructWhereClause(FilterOperator operator, int depth) {
    if (depth == 1) {
      return "(A = " + depth + ")";
    } else {
      return "(A" + depth + " = " + depth + " " + operator + " " + constructWhereClause(operator, depth - 1) + ")";
    }
  }
}
