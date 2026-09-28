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

import java.util.List;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.trace.DefaultRequestContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class BrokerRequestHandlerDelegateTest {

  @Test
  public void testOnlyQueriesReachTheQueryEngines()
      throws Exception {
    BaseSingleStageBrokerRequestHandler singleStageHandler = mock(BaseSingleStageBrokerRequestHandler.class);
    BrokerResponseNative queryResponse = new BrokerResponseNative();
    when(singleStageHandler.handleRequest(any(), any(), any(), any(), any())).thenReturn(queryResponse);
    BrokerRequestHandlerDelegate delegate = new BrokerRequestHandlerDelegate(singleStageHandler, null, null, null);

    // DML is executed by the SQL executor, which the gRPC endpoint and custom containers do not dispatch to: the
    // statement is rejected instead of failing to compile as a query
    for (String sql : List.of("DELETE FROM myTable WHERE col1 = 'a'",
        "INSERT INTO myTable FROM FILE 'file:///tmp/data'")) {
      RequestContext requestContext = new DefaultRequestContext();
      BrokerResponse response =
          delegate.handleRequest(JsonUtils.newObjectNode().put(Request.SQL, sql), null, null, requestContext, null);
      assertEquals(response.getExceptions().size(), 1, sql);
      QueryProcessingException exception = response.getExceptions().get(0);
      assertEquals(exception.getErrorCode(), QueryErrorCode.SQL_PARSING.getId());
      assertTrue(exception.getMessage().contains("only supports DQL"), exception.getMessage());
      assertEquals(requestContext.getErrorCode(), QueryErrorCode.SQL_PARSING.getId());
    }
    verify(singleStageHandler, never()).handleRequest(any(), any(), any(), any(), any());

    // Queries are handed to the query engine
    BrokerResponse response = delegate.handleRequest(
        JsonUtils.newObjectNode().put(Request.SQL, "SELECT * FROM myTable"), null, null, new DefaultRequestContext(),
        null);
    assertSame(response, queryResponse);
  }
}
