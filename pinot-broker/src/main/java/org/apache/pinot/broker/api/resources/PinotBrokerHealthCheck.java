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

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.pinot.broker.api.services.PinotBrokerHealthCheckService;
import org.apache.pinot.broker.broker.BrokerAdminApiApplication;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.ServiceStatus;


@Path("/")
public class PinotBrokerHealthCheck implements PinotBrokerHealthCheckService {
  @Inject
  @Named(BrokerAdminApiApplication.BROKER_INSTANCE_ID)
  private String _instanceId;

  @Inject
  private BrokerMetrics _brokerMetrics;

  @Override
  public String getBrokerHealth() {
    ServiceStatus.Status status = ServiceStatus.getServiceStatus(_instanceId);
    if (status == ServiceStatus.Status.GOOD) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.HEALTHCHECK_OK_CALLS, 1);
      return "OK";
    }
    _brokerMetrics.addMeteredGlobalValue(BrokerMeter.HEALTHCHECK_BAD_CALLS, 1);
    String errMessage = String.format("Pinot broker status is %s", status);
    Response response =
        Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(errMessage).build();
    throw new WebApplicationException(errMessage, response);
  }
}
