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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.PinotBrokerQueryEventListenerFactory;
import org.apache.pinot.spi.trace.DefaultRequestContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MultiStageBrokerRequestHandlerTest {

  private PinotConfiguration _config;
  @Mock
  private BrokerRoutingManager _routingManager;

  private AccessControlFactory _accessControlFactory;
  @Mock
  private QueryQuotaManager _queryQuotaManager;
  @Mock
  private TableCache _tableCache;

  @Mock
  private BrokerMetrics _brokerMetrics;

  private MultiStageBrokerRequestHandler _requestHandler;

  @BeforeClass
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    _config = new PinotConfiguration();
    _config.setProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, "localhost");
    _config.setProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, "12345");
    _accessControlFactory = new AllowAllAccessControlFactory();
    _requestHandler =
        new MultiStageBrokerRequestHandler(_config, "Broker_localhost", _routingManager,
                _accessControlFactory, _queryQuotaManager, _tableCache, _brokerMetrics,
                PinotBrokerQueryEventListenerFactory.getBrokerQueryEventListener());
  }

  @Test
  public void testSetRequestId()
      throws Exception {
    String sampleSqlQuery = "SELECT * FROM testTable";
    String sampleJsonRequest = String.format("{\"sql\":\"%s\"}", sampleSqlQuery);
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonRequest = objectMapper.readTree(sampleJsonRequest);
    RequestContext requestContext = new DefaultRequestContext();

    List<Long> requestIds = new ArrayList<>();
    // Request id should be unique each time, and there should be a difference of 1 between consecutive requestIds.
    for (int iteration = 0; iteration < 10; iteration++) {
      _requestHandler.handleRequest(jsonRequest, null, null, requestContext, null);
      Assert.assertTrue(requestContext.getRequestId() >= 0, "Request ID should be non-negative");
      requestIds.add(requestContext.getRequestId());
      if (iteration != 0) {
        Assert.assertEquals(1, requestIds.get(iteration) - requestIds.get(iteration - 1),
            "Request Id should have difference of 1");
      }
    }
    Assert.assertEquals(10, requestIds.stream().distinct().count(), "Request Id should be unique");
    Assert.assertEquals(1, requestIds.stream().map(x -> (x >> 32)).distinct().count(),
        "Request Id should have a broker-id specific mask for the 32 MSB");
    Assert.assertTrue(requestIds.stream().noneMatch(x -> x < 0), "Request Id should not be negative");
  }

  @AfterClass
  public void tearDown() {
    _requestHandler.shutDown();
  }
}
