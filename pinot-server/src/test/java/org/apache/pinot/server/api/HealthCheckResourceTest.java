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
package org.apache.pinot.server.api;

import javax.ws.rs.core.Response;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.utils.ServiceStatus;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class HealthCheckResourceTest extends BaseResourceTest {

  @Test
  public void checkHealthProbes() {
    String healthPath = "/health";
    String livenessPath = "/health/liveness";
    String readinessPath = "/health/readiness";

    ServiceStatus.ServiceStatusCallback mockFailureCallback = mock(ServiceStatus.ServiceStatusCallback.class);
    when(mockFailureCallback.getServiceStatus()).thenReturn(ServiceStatus.Status.BAD);

    assertEquals(_webTarget.path(livenessPath).request().get(Response.class).getStatus(), 200);
    assertEquals(
        _webTarget.path(healthPath).queryParam("checkType", "liveness").request().get(Response.class).getStatus(), 200);
    assertEquals(_webTarget.path(healthPath).request().get(Response.class).getStatus(), 200);
    assertEquals(_webTarget.path(readinessPath).request().get(Response.class).getStatus(), 200);
    assertEquals(
        _webTarget.path(healthPath).queryParam("checkType", "readiness").request().get(Response.class).getStatus(),
        200);

    _isServerReadyToServeQueries.set(false);
    assertEquals(_webTarget.path(livenessPath).request().get(Response.class).getStatus(), 200);
    assertEquals(_webTarget.path(healthPath).request().get(Response.class).getStatus(), 503);
    assertEquals(_webTarget.path(readinessPath).request().get(Response.class).getStatus(), 503);
    assertEquals(
        _webTarget.path(healthPath).queryParam("checkType", "readiness").request().get(Response.class).getStatus(),
        503);
    _isServerReadyToServeQueries.set(true);

    // The readiness supplier already includes the server service-status check, so a separate callback must not gate it.
    ServiceStatus.setServiceStatusCallback(_instanceId, mockFailureCallback);
    assertEquals(_webTarget.path(livenessPath).request().get(Response.class).getStatus(), 200);
    assertEquals(
        _webTarget.path(healthPath).queryParam("checkType", "liveness").request().get(Response.class).getStatus(), 200);
    assertEquals(_webTarget.path(healthPath).request().get(Response.class).getStatus(), 200);
    assertEquals(_webTarget.path(readinessPath).request().get(Response.class).getStatus(), 200);
    assertEquals(
        _webTarget.path(healthPath).queryParam("checkType", "readiness").request().get(Response.class).getStatus(),
        200);
    ServiceStatus.removeServiceStatusCallback(_instanceId);

    verify(_serverMetrics, times(6)).addMeteredGlobalValue(ServerMeter.READINESS_CHECK_OK_CALLS, 1);
    verify(_serverMetrics, times(3)).addMeteredGlobalValue(ServerMeter.READINESS_CHECK_BAD_CALLS, 1);

    // Start shutting down the HTTP server, only liveness check should go through
    _adminApiApplication.startShuttingDown();
    assertEquals(_webTarget.path(livenessPath).request().get(Response.class).getStatus(), 200);
    assertEquals(
        _webTarget.path(healthPath).queryParam("checkType", "liveness").request().get(Response.class).getStatus(), 200);
    assertEquals(_webTarget.path(healthPath).request().get(Response.class).getStatus(), 503);
    assertEquals(_webTarget.path(readinessPath).request().get(Response.class).getStatus(), 503);
    assertEquals(
        _webTarget.path(healthPath).queryParam("checkType", "readiness").request().get(Response.class).getStatus(),
        503);
  }
}
