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
package org.apache.pinot.sql.parsers.rewriter;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.sql.parsers.CalciteSqlParser.QUERY_REWRITERS;


public class QueryRewriterFactoryTest {

  @Test
  public void testQueryRewriters() {
    // Default behavior
    QueryRewriterFactory.init(null);
    Assert.assertEquals(QUERY_REWRITERS.size(), 6);
    Assert.assertTrue(QUERY_REWRITERS.get(0) instanceof CompileTimeFunctionsInvoker);
    Assert.assertTrue(QUERY_REWRITERS.get(1) instanceof SelectionsRewriter);
    Assert.assertTrue(QUERY_REWRITERS.get(2) instanceof PredicateComparisonRewriter);
    Assert.assertTrue(QUERY_REWRITERS.get(3) instanceof AliasApplier);
    Assert.assertTrue(QUERY_REWRITERS.get(4) instanceof OrdinalsUpdater);
    Assert.assertTrue(QUERY_REWRITERS.get(5) instanceof NonAggregationGroupByToDistinctQueryRewriter);

    // Check init with other configs
    QueryRewriterFactory.init("org.apache.pinot.sql.parsers.rewriter.PredicateComparisonRewriter,"
        + "org.apache.pinot.sql.parsers.rewriter.CompileTimeFunctionsInvoker,"
        + "org.apache.pinot.sql.parsers.rewriter.SelectionsRewriter");
    Assert.assertEquals(QUERY_REWRITERS.size(), 3);
    Assert.assertTrue(QUERY_REWRITERS.get(0) instanceof PredicateComparisonRewriter);
    Assert.assertTrue(QUERY_REWRITERS.get(1) instanceof CompileTimeFunctionsInvoker);
    Assert.assertTrue(QUERY_REWRITERS.get(2) instanceof SelectionsRewriter);

    // Revert back to default behavior
    QueryRewriterFactory.init(null);
    Assert.assertEquals(QUERY_REWRITERS.size(), 6);
    Assert.assertTrue(QUERY_REWRITERS.get(0) instanceof CompileTimeFunctionsInvoker);
    Assert.assertTrue(QUERY_REWRITERS.get(1) instanceof SelectionsRewriter);
    Assert.assertTrue(QUERY_REWRITERS.get(2) instanceof PredicateComparisonRewriter);
    Assert.assertTrue(QUERY_REWRITERS.get(3) instanceof AliasApplier);
    Assert.assertTrue(QUERY_REWRITERS.get(4) instanceof OrdinalsUpdater);
    Assert.assertTrue(QUERY_REWRITERS.get(5) instanceof NonAggregationGroupByToDistinctQueryRewriter);
  }
}
