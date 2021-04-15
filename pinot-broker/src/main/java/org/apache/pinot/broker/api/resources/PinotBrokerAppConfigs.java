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

import io.swagger.annotations.Api;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.broker.broker.BrokerAdminApiApplication;
import org.apache.pinot.common.utils.PinotAppConfigs;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Resource to get the app configs {@link PinotAppConfigs} for
 * the broker.
 */
@Api(tags = "AppConfig")
@Path("/")
public class PinotBrokerAppConfigs {

  @Context
  private Application _application;

  @GET
  @Path("/appconfigs")
  @Produces(MediaType.APPLICATION_JSON)
  public String getAppConfigs() {
    PinotConfiguration pinotConfiguration =
        (PinotConfiguration) _application.getProperties().get(BrokerAdminApiApplication.PINOT_CONFIGURATION);
    PinotAppConfigs pinotAppConfigs = new PinotAppConfigs(pinotConfiguration);
    return pinotAppConfigs.toJSONString();
  }
}
