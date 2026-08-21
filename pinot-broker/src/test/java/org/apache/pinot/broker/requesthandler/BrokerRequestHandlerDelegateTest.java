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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.pinot.broker.broker.BrokerDrainManager;
import org.apache.pinot.common.cursors.AbstractResponseStore;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.trace.DefaultRequestContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.JsonUtils;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class BrokerRequestHandlerDelegateTest {
  @Test
  public void testRejectsSqlQueryWhenBrokerIsDraining()
      throws Exception {
    BaseSingleStageBrokerRequestHandler singleStageBrokerRequestHandler =
        Mockito.mock(BaseSingleStageBrokerRequestHandler.class);
    BrokerDrainManager drainManager = BrokerDrainManager.localOnly("Broker_localhost_8099", () -> {
    }, () -> {
    }, 10_000L);
    BrokerRequestHandlerDelegate delegate =
        new BrokerRequestHandlerDelegate(singleStageBrokerRequestHandler, null, null,
            Mockito.mock(AbstractResponseStore.class), drainManager);

    drainManager.drain(0L, false);

    RequestContext requestContext = new DefaultRequestContext();
    BrokerResponse response = delegate.handleRequest(sqlRequest("SELECT * FROM myTable"), null, null, requestContext,
        null);

    assertEquals(response.getExceptions().get(0).getErrorCode(), QueryErrorCode.BROKER_SHUTTING_DOWN.getId());
    assertEquals(requestContext.getErrorCode(), QueryErrorCode.BROKER_SHUTTING_DOWN.getId());
    verify(singleStageBrokerRequestHandler, never()).handleRequest(any(), any(), any(), any(), any());
  }

  @Test
  public void testReleasesPermitAfterSqlQueryFinishes()
      throws Exception {
    BaseSingleStageBrokerRequestHandler singleStageBrokerRequestHandler =
        Mockito.mock(BaseSingleStageBrokerRequestHandler.class);
    BrokerDrainManager drainManager = BrokerDrainManager.localOnly("Broker_localhost_8099", () -> {
    }, () -> {
    }, 10_000L);
    BrokerRequestHandlerDelegate delegate =
        new BrokerRequestHandlerDelegate(singleStageBrokerRequestHandler, null, null,
            Mockito.mock(AbstractResponseStore.class), drainManager);

    when(singleStageBrokerRequestHandler.handleRequest(any(), any(), any(), any(), any()))
        .thenAnswer(invocation -> {
          assertEquals(drainManager.getStatus().getInFlightQueries(), 1);
          return BrokerResponseNative.EMPTY_RESULT;
        });

    BrokerResponse response = delegate.handleRequest(sqlRequest("SELECT * FROM myTable"), null, null,
        new DefaultRequestContext(), null);

    assertEquals(response.getExceptionsSize(), 0);
    assertEquals(drainManager.getStatus().getInFlightQueries(), 0);
  }

  private static JsonNode sqlRequest(String sql)
      throws Exception {
    return JsonUtils.stringToJsonNode("{\"sql\":\"" + sql + "\"}");
  }
}
