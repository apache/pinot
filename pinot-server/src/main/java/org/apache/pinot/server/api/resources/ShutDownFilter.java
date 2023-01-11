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
package org.apache.pinot.server.api.resources;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;


/**
 * This filter is used to reject all requests when server is shutting down, and only allow the liveness check to go
 * through.
 */
@Provider
public class ShutDownFilter implements ContainerRequestFilter {

  @Inject
  private AtomicBoolean _shutDownInProgress;

  @Override
  public void filter(ContainerRequestContext requestContext)
      throws IOException {
    // NOTE: Allow health check requests for liveness check
    if (_shutDownInProgress.get() && !(requestContext.getUriInfo().getMatchedResources()
        .get(0) instanceof HealthCheckResource)) {
      String errMessage = "Server is shutting down";
      throw new WebApplicationException(errMessage,
          Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(errMessage).build());
    }
  }
}
