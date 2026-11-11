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

import java.io.IOException;
import java.lang.reflect.Method;
import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.Priorities;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.server.access.AccessControlFactory;
import org.apache.pinot.server.access.HttpRequesterIdentity;
import org.apache.pinot.server.api.resources.HealthCheckResource;
import org.apache.pinot.server.api.resources.TablesResource;
import org.apache.pinot.spi.auth.AuthorizationResult;


/// Enforces the authorization boundary for the Server administrative API.
///
/// Only the Server health endpoints are public. Methods marked with [ServerDataAccess] retain their explicit table-data
/// access checks. Every other Server route, including custom resource routes, requires administrative authorization
/// from the configured Server access control.
@Priority(Priorities.AUTHENTICATION)
public class ServerAdminApiAccessControlFilter implements ContainerRequestFilter {
  private static final String GET = "GET";

  @Inject
  private AccessControlFactory _accessControlFactory;

  @Context
  private ResourceInfo _resourceInfo;

  @Context
  private HttpHeaders _httpHeaders;

  @Override
  public void filter(ContainerRequestContext requestContext)
      throws IOException {
    Method endpointMethod = _resourceInfo.getResourceMethod();
    Object originalRequestMethod = requestContext.getProperty(ServerRequestMethodCaptureFilter.REQUEST_METHOD_PROPERTY);
    String requestMethod = originalRequestMethod instanceof String ? (String) originalRequestMethod
        : requestContext.getMethod();
    if (GET.equals(requestMethod) && HealthCheckResource.class.equals(_resourceInfo.getResourceClass())
        && endpointMethod != null && endpointMethod.isAnnotationPresent(ServerPublicAccess.class)) {
      return;
    }
    if (TablesResource.class.equals(_resourceInfo.getResourceClass()) && endpointMethod != null
        && endpointMethod.isAnnotationPresent(ServerDataAccess.class)) {
      return;
    }

    HttpRequesterIdentity requesterIdentity = new HttpRequesterIdentity(_httpHeaders);
    requesterIdentity.setEndpointUrl(requestContext.getUriInfo().getRequestUri().toString());
    AuthorizationResult authorizationResult = _accessControlFactory.create().authorizeAdminAccess(requesterIdentity);
    if (!authorizationResult.hasAccess()) {
      throw new WebApplicationException(Response.status(Response.Status.FORBIDDEN)
          .type(MediaType.TEXT_PLAIN_TYPE).entity("Forbidden").build());
    }
  }
}
