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
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.core.api.AutoLoadedServiceForTest;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;


/**
 * This class is a typical "echo" service that will return whatever string you call GET with a path.
 * It is both an integration test and a demonstration of how to dynamically add an endpoint to broker,
 * create auto-service discovery
 */
@Api(tags = "Test")
@Path("/test")
public class BrokerEchoWithAutoDiscovery {
    @Inject
    public AutoLoadedServiceForTest _injectedService;
    @GET
    @Path("/echo/{table}")
    @Produces(MediaType.TEXT_PLAIN)
    public String echo(@PathParam("table") String table) {
        return _injectedService.echo(table);
    }
}
