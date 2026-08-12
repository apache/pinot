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
package org.apache.pinot.broker.api.resources;

import java.lang.reflect.Field;
import javax.ws.rs.WebApplicationException;
import org.apache.pinot.broker.routing.manager.BrokerRoutingManager;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


public class PinotBrokerHealthCheckTest {
  private static final String BROKER_INSTANCE = "Broker_localhost_8099";
  private static final String SERVER_INSTANCE = "Server_localhost_8098";

  private BrokerRoutingManager _routingManager;
  private PinotBrokerHealthCheck _healthCheck;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _routingManager = mock(BrokerRoutingManager.class);
    _healthCheck = new PinotBrokerHealthCheck();
    setField("_instanceId", BROKER_INSTANCE);
    setField("_brokerMetrics", mock(BrokerMetrics.class));
    setField("_routingManager", _routingManager);

    ServiceStatus.ServiceStatusCallback callback = mock(ServiceStatus.ServiceStatusCallback.class);
    when(callback.getServiceStatus()).thenReturn(ServiceStatus.Status.GOOD);
    ServiceStatus.setServiceStatusCallback(BROKER_INSTANCE, callback);
  }

  @AfterMethod
  public void tearDown() {
    ServiceStatus.removeServiceStatusCallback(BROKER_INSTANCE);
  }

  @Test
  public void testRoutingReadiness() {
    assertEquals(_healthCheck.getBrokerHealth(null), "OK");

    when(_routingManager.isServerRoutable(SERVER_INSTANCE)).thenReturn(false);
    WebApplicationException exception =
        expectThrows(WebApplicationException.class, () -> _healthCheck.getBrokerHealth(SERVER_INSTANCE));
    assertEquals(exception.getResponse().getStatus(), 503);

    when(_routingManager.isServerRoutable(SERVER_INSTANCE)).thenReturn(true);
    assertEquals(_healthCheck.getBrokerHealth(SERVER_INSTANCE), CommonConstants.Broker.SERVER_ROUTING_READY_RESPONSE);
  }

  private void setField(String name, Object value)
      throws Exception {
    Field field = PinotBrokerHealthCheck.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(_healthCheck, value);
  }
}
