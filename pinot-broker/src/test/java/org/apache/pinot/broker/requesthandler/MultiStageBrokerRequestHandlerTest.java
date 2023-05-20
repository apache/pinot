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
import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
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
    _config.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_TIMEOUT_MS, "10000");
    _config.setProperty(QueryConfig.KEY_OF_QUERY_RUNNER_PORT, "12345");
    _accessControlFactory = new AllowAllAccessControlFactory();
    _requestHandler =
        new MultiStageBrokerRequestHandler(_config, "testBrokerId", _routingManager, _accessControlFactory,
            _queryQuotaManager, _tableCache, _brokerMetrics);
  }

  @Test
  public void testSetRequestId()
      throws Exception {
    String sampleSqlQuery = "SELECT * FROM testTable";
    String sampleJsonRequest = String.format("{\"sql\":\"%s\"}", sampleSqlQuery);
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonRequest = objectMapper.readTree(sampleJsonRequest);
    RequestContext requestContext = new DefaultRequestContext();

    Set<Long> requestIds = new HashSet<>();
    // Request id is a long and generated randomly each time. Running it a bunch of times should yield a unique id
    // each time. Also, the requestId should always be non-negative.
    for (int iteration = 0; iteration < 10; iteration++) {
      _requestHandler.handleRequest(jsonRequest, null, null, requestContext);
      Assert.assertTrue(requestContext.getRequestId() >= 0, "Request ID should be non-negative");
      requestIds.add(requestContext.getRequestId());
    }
    Assert.assertEquals(10, requestIds.size(), "Random request-id should generate unique ids across 10 runs");
  }

  @AfterClass
  public void tearDown() {
    _requestHandler.shutDown();
  }
}
