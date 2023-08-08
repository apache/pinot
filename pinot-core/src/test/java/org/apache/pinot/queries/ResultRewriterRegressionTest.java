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

import org.apache.pinot.core.query.utils.rewriter.ResultRewriterFactory;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriterFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Regression test for queries with result rewriter.
 */
public class ResultRewriterRegressionTest {

  @Test
  public static class StatsQueriesRegressionTest extends StatisticalQueriesTest {
    @BeforeClass
    public void setupRewriter()
        throws Exception {
      QueryRewriterFactory.init(String.join(",", QueryRewriterFactory.DEFAULT_QUERY_REWRITERS_CLASS_NAMES)
          + ",org.apache.pinot.sql.parsers.rewriter.ArgMinMaxRewriter");
      ResultRewriterFactory
          .init("org.apache.pinot.core.query.utils.rewriter.ParentAggregationResultRewriter");
    }
  }

  @Test
  public static class HistogramQueriesRegressionTest extends HistogramQueriesTest {
    @BeforeClass
    public void setupRewriter()
        throws Exception {
      QueryRewriterFactory.init(String.join(",", QueryRewriterFactory.DEFAULT_QUERY_REWRITERS_CLASS_NAMES)
          + ",org.apache.pinot.sql.parsers.rewriter.ArgMinMaxRewriter");
      ResultRewriterFactory
          .init("org.apache.pinot.core.query.utils.rewriter.ParentAggregationResultRewriter");
    }
  }

  @Test
  public static class InterSegmentAggregationMultiValueQueriesRegressionTest
      extends InterSegmentAggregationMultiValueQueriesTest {
    @BeforeClass
    public void setupRewriter()
        throws Exception {
      QueryRewriterFactory.init(String.join(",", QueryRewriterFactory.DEFAULT_QUERY_REWRITERS_CLASS_NAMES)
          + ",org.apache.pinot.sql.parsers.rewriter.ArgMinMaxRewriter");
      ResultRewriterFactory
          .init("org.apache.pinot.core.query.utils.rewriter.ParentAggregationResultRewriter");
    }
  }
}
