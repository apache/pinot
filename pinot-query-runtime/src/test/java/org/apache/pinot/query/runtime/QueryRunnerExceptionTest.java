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
package org.apache.pinot.query.runtime;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.service.QueryDispatcher;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class QueryRunnerExceptionTest extends QueryRunnerTestBase {

  @Test(dataProvider = "testDataWithSqlExecutionExceptions")
  public void testSqlWithExceptionMsgChecker(String sql, String exceptionMsg) {
    QueryPlan queryPlan = _queryEnvironment.planQuery(sql);
    Map<String, String> requestMetadataMap =
        ImmutableMap.of("REQUEST_ID", String.valueOf(RANDOM_REQUEST_ID_GEN.nextLong()));
    MailboxReceiveOperator mailboxReceiveOperator = null;
    for (int stageId : queryPlan.getStageMetadataMap().keySet()) {
      if (queryPlan.getQueryStageMap().get(stageId) instanceof MailboxReceiveNode) {
        MailboxReceiveNode reduceNode = (MailboxReceiveNode) queryPlan.getQueryStageMap().get(stageId);
        mailboxReceiveOperator = QueryDispatcher.createReduceStageOperator(_mailboxService,
            queryPlan.getStageMetadataMap().get(reduceNode.getSenderStageId()).getServerInstances(),
            Long.parseLong(requestMetadataMap.get("REQUEST_ID")), reduceNode.getSenderStageId(),
            reduceNode.getDataSchema(), "localhost", _reducerGrpcPort);
      } else {
        for (ServerInstance serverInstance : queryPlan.getStageMetadataMap().get(stageId).getServerInstances()) {
          DistributedStagePlan distributedStagePlan =
              QueryDispatcher.constructDistributedStagePlan(queryPlan, stageId, serverInstance);
          _servers.get(serverInstance).processQuery(distributedStagePlan, requestMetadataMap);
        }
      }
    }
    Preconditions.checkNotNull(mailboxReceiveOperator);

    try {
      QueryDispatcher.reduceMailboxReceive(mailboxReceiveOperator);
    } catch (RuntimeException rte) {
      Assert.assertTrue(rte.getMessage().contains("Received error query execution result block"));
      Assert.assertTrue(rte.getMessage().contains(exceptionMsg));
    }
  }

  @DataProvider(name = "testDataWithSqlExecutionExceptions")
  private Object[][] provideTestSqlWithExecutionException() {
    return new Object[][] {
        // Function with incorrect argument signature should throw runtime exception
        new Object[]{"SELECT least(a.col2, b.col3) FROM a JOIN b ON a.col1 = b.col1",
            "ArithmeticFunctions.least(double,double) with arguments"},
        // Function that tries to cast String to Number should throw runtime exception
        new Object[]{"SELECT a.col2, b.col1 FROM a JOIN b ON a.col1 = b.col3", "transform function: cast"},
    };
  }
}
