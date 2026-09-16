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
import javax.ws.rs.core.Response;
import org.apache.pinot.broker.broker.BrokerDrainManager;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.ServiceStatus;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


public class PinotBrokerHealthCheckTest {
  private static final String INSTANCE_ID = "Broker_health_check_test";

  @AfterMethod
  public void tearDown() {
    ServiceStatus.removeServiceStatusCallback(INSTANCE_ID);
  }

  @Test
  public void testLivenessRemainsHealthyWhileDrainFailsReadiness()
      throws Exception {
    ServiceStatus.ServiceStatusCallback serviceStatusCallback =
        Mockito.mock(ServiceStatus.ServiceStatusCallback.class);
    Mockito.when(serviceStatusCallback.getServiceStatus()).thenReturn(ServiceStatus.Status.GOOD);
    ServiceStatus.setServiceStatusCallback(INSTANCE_ID, serviceStatusCallback);

    BrokerDrainManager drainManager = BrokerDrainManager.localOnly(INSTANCE_ID, () -> {
    }, () -> {
    }, 10_000L);
    PinotBrokerHealthCheck healthCheck = new PinotBrokerHealthCheck();
    setField(healthCheck, "_instanceId", INSTANCE_ID);
    setField(healthCheck, "_brokerMetrics", Mockito.mock(BrokerMetrics.class));
    setField(healthCheck, "_brokerDrainManager", drainManager);

    assertEquals(healthCheck.getBrokerHealth(), "OK");
    drainManager.drain(0L, false);

    WebApplicationException readinessFailure =
        expectThrows(WebApplicationException.class, () -> healthCheck.getBrokerHealth(null));
    assertEquals(readinessFailure.getResponse().getStatus(), Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
    assertEquals(healthCheck.getBrokerHealth("liveness"), "OK");
  }

  private static void setField(Object target, String fieldName, Object value)
      throws ReflectiveOperationException {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
