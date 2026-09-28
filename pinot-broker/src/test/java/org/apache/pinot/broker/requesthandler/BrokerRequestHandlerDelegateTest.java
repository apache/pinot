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

import org.apache.pinot.common.response.BrokerResponse;
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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class BrokerRequestHandlerDelegateTest {

  @Test
  public void testDeleteDoesNotReachTheQueryEngines()
      throws Exception {
    BaseSingleStageBrokerRequestHandler singleStageHandler = mock(BaseSingleStageBrokerRequestHandler.class);
    BrokerRequestHandlerDelegate delegate = new BrokerRequestHandlerDelegate(singleStageHandler, null, null, null);

    // gRPC and custom containers bypass the REST authorization path.
    RequestContext requestContext = new DefaultRequestContext();
    BrokerResponse response = delegate.handleRequest(
        JsonUtils.newObjectNode().put(Request.SQL, "DELETE FROM myTable WHERE col1 = 'a'"), null, null, requestContext,
        null);
    assertEquals(response.getExceptions().size(), 1);
    QueryProcessingException exception = response.getExceptions().get(0);
    assertEquals(exception.getErrorCode(), QueryErrorCode.SQL_PARSING.getId());
    assertTrue(exception.getMessage().contains("broker SQL endpoint"), exception.getMessage());
    assertEquals(requestContext.getErrorCode(), QueryErrorCode.SQL_PARSING.getId());
    verify(singleStageHandler, never()).handleRequest(any(), any(), any(), any(), any());
  }
}
