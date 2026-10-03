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
package org.apache.pinot.core.operator.combine;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryErrorMessage;
import org.testng.annotations.Test;

import static org.testng.Assert.assertSame;

/// Verifies cleanup errors cannot overwrite a primary combine error block.
public class BaseSingleBlockCombineOperatorCleanupTest {
  @Test
  public void testCleanupFailurePreservesPrimaryErrorBlock() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable");
    queryContext.setEndTimeMs(System.currentTimeMillis() + 30_000L);
    ExceptionResultsBlock primary = new ExceptionResultsBlock(
        new QueryErrorMessage(QueryErrorCode.EXECUTION_TIMEOUT, "timed out", "timed out"));
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      BaseSingleBlockCombineOperator<ExceptionResultsBlock> combine =
          new BaseSingleBlockCombineOperator<>(null, List.of(), queryContext, executor) {
            @Override
            public String toExplainString() {
              return "TEST_COMBINE";
            }

            @Override
            protected BaseResultsBlock mergeResults() {
              return primary;
            }

            @Override
            protected void onProcessStopped() {
              throw new IllegalStateException("native close failed");
            }
          };
      assertSame(combine.nextBlock(), primary);
    } finally {
      executor.shutdownNow();
    }
  }
}
