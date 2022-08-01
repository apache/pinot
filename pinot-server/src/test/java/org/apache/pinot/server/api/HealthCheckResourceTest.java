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
import org.apache.pinot.common.utils.ServiceStatus;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class HealthCheckResourceTest extends BaseResourceTest {
  @Test
  public void checkHealthProbes()
      throws Exception {
    String healthPath = "/health";
    String livenessPath = "/health/liveness";
    String readinessPath = "/health/readiness";

    ServiceStatus.ServiceStatusCallback mockSuccessCallback = mock(ServiceStatus.ServiceStatusCallback.class);
    ServiceStatus.ServiceStatusCallback mockFailureCallback = mock(ServiceStatus.ServiceStatusCallback.class);
    when(mockSuccessCallback.getServiceStatus()).thenReturn(ServiceStatus.Status.GOOD);
    when(mockFailureCallback.getServiceStatus()).thenReturn(ServiceStatus.Status.BAD);

    Assert.assertEquals(_webTarget.path(livenessPath).request().get(Response.class).getStatus(), 200);
    Assert.assertEquals(_webTarget.path(healthPath).request().get(Response.class).getStatus(), 503);
    Assert.assertEquals(_webTarget.path(readinessPath).request().get(Response.class).getStatus(), 503);

    ServiceStatus.setServiceStatusCallback(_instanceId, mockSuccessCallback);
    Assert.assertEquals(_webTarget.path(livenessPath).request().get(Response.class).getStatus(), 200);
    Assert.assertEquals(_webTarget.path(healthPath).request().get(Response.class).getStatus(), 200);
    Assert.assertEquals(_webTarget.path(readinessPath).request().get(Response.class).getStatus(), 200);

    ServiceStatus.setServiceStatusCallback(_instanceId, mockFailureCallback);
    Assert.assertEquals(_webTarget.path(livenessPath).request().get(Response.class).getStatus(), 200);
    Assert.assertEquals(_webTarget.path(healthPath).request().get(Response.class).getStatus(), 503);
    Assert.assertEquals(_webTarget.path(readinessPath).request().get(Response.class).getStatus(), 503);
    Assert.assertEquals(_webTarget.path(healthPath).queryParam("checkType", "readiness")
        .request().get(Response.class).getStatus(), 503);
    Assert.assertEquals(_webTarget.path(healthPath).queryParam("checkType", "liveness")
        .request().get(Response.class).getStatus(), 200);
  }
}
