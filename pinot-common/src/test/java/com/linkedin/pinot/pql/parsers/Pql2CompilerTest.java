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
package com.linkedin.pinot.pql.parsers;

import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.pql.parsers.pql2.ast.TopAstNode;


/**
 * Some tests for the PQL 2 compiler.
 */
public class Pql2CompilerTest {
  @Test
  public void testQuotedStrings() {
    Pql2Compiler compiler = new Pql2Compiler();

    // Two single quotes in a single quoted string
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(
        "select * from vegetables where origin = 'Martha''s Vineyard'");
    Assert.assertEquals(brokerRequest.getFilterQuery().getValue().get(0), "Martha's Vineyard");

    brokerRequest = compiler.compileToBrokerRequest(
        "select * from vegetables where origin = 'Martha\"\"s Vineyard'");
    Assert.assertEquals(brokerRequest.getFilterQuery().getValue().get(0), "Martha\"\"s Vineyard");

    brokerRequest = compiler.compileToBrokerRequest(
        "select * from vegetables where origin = \"Martha\"\"s Vineyard\"");
    Assert.assertEquals(brokerRequest.getFilterQuery().getValue().get(0), "Martha\"s Vineyard");

    brokerRequest = compiler.compileToBrokerRequest(
        "select * from vegetables where origin = \"Martha''s Vineyard\"");
    Assert.assertEquals(brokerRequest.getFilterQuery().getValue().get(0), "Martha''s Vineyard");
  }

  @Test
  public void testTopZero() throws Exception {
    Pql2Compiler compiler  = new Pql2Compiler();
    testTopZeroFor(compiler, "select count(*) from someTable where c = 5 group by X top 0", TopAstNode.DEFAULT_TOP_N, false);
    testTopZeroFor(compiler, "select count(*) from someTable where c = 5 group by X top 1", 1, false);
    testTopZeroFor(compiler, "select count(*) from someTable where c = 5 group by X top -1", TopAstNode.DEFAULT_TOP_N, true);
  }

  private void testTopZeroFor(Pql2Compiler compiler, String s, final int expectedTopN, boolean parseException) throws Exception {
    BrokerRequest req;
    try {
      req = compiler.compileToBrokerRequest(s);
    } catch (Pql2CompilationException e) {
      if (parseException) {
        return;
      }
      throw e;
    }
    Assert.assertTrue(req.isSetGroupBy());
    GroupBy groupBy = req.getGroupBy();
    Assert.assertTrue(groupBy.isSetTopN());
    Assert.assertEquals(expectedTopN, groupBy.getTopN());
  }
}
