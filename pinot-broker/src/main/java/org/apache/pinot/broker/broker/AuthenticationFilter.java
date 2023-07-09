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

package org.apache.pinot.broker.broker;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.api.HttpRequesterIdentity;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.RBACAuthUtils;
import org.apache.pinot.core.auth.RBACAuthorization;
import org.glassfish.grizzly.http.server.Request;

/**
 * A container filter class responsible for automatic authentication of REST endpoints. Any rest endpoints not annotated
 * with {@link org.apache.pinot.core.auth.ManualAuthorization} annotation, will go through authentication.
 */
@javax.ws.rs.ext.Provider
public class AuthenticationFilter implements ContainerRequestFilter {
  private static final Set<String> UNPROTECTED_PATHS =
      new HashSet<>(Arrays.asList("", "help", "health", "help#"));

  @Inject
  Provider<Request> _requestProvider;

  @Inject
  AccessControlFactory _accessControlFactory;

  @Context
  ResourceInfo _resourceInfo;

  @Context
  HttpHeaders _httpHeaders;

  @Override
  public void filter(ContainerRequestContext requestContext)
      throws IOException {
    Request request = _requestProvider.get();

    Method endpointMethod = _resourceInfo.getResourceMethod();
    AccessControl accessControl = _accessControlFactory.create();
    UriInfo uriInfo = requestContext.getUriInfo();

    // exclude public/unprotected paths
    if (isBaseFile(uriInfo.getPath()) || UNPROTECTED_PATHS.contains(uriInfo.getPath())) {
      return;
    }

    // check if the method's authorization is disabled (i.e. performed manually within method)
    if (endpointMethod.isAnnotationPresent(ManualAuthorization.class)) {
      return;
    }

    HttpRequesterIdentity httpRequestIdentity = HttpRequesterIdentity.fromRequest(request);

    // default authorization handling
    if (!accessControl.hasAccess(httpRequestIdentity)) {
      throw new WebApplicationException("Failed access check for " + httpRequestIdentity.getEndpointUrl(),
          Response.Status.FORBIDDEN);
    }

    handleRBACAuthorization(endpointMethod, uriInfo, accessControl, httpRequestIdentity);
  }

  private void handleRBACAuthorization(Method endpointMethod, UriInfo uriInfo, AccessControl accessControl,
      HttpRequesterIdentity httpRequestIdentity) {
    if (endpointMethod.isAnnotationPresent(RBACAuthorization.class)) {
      RBACAuthorization rbacAuthorization = endpointMethod.getAnnotation(RBACAuthorization.class);

      // If targetId is not specified (null or empty), pass null to the accessControl.hasRBACAccess() method.
      if (StringUtils.isEmpty(rbacAuthorization.targetId())) {
        if (!accessControl.hasRBACAccess(httpRequestIdentity, rbacAuthorization.targetType(), null,
            rbacAuthorization.permission())) {
          throw new WebApplicationException("Permission denied to " + rbacAuthorization.permission(),
              Response.Status.FORBIDDEN);
        }
      } else {
        String targetId = RBACAuthUtils.getTargetId(rbacAuthorization.targetId(), uriInfo.getPathParameters(),
            uriInfo.getQueryParameters());
        if (targetId != null) {
          if (!accessControl.hasRBACAccess(httpRequestIdentity, rbacAuthorization.targetType(), targetId,
              rbacAuthorization.permission())) {
            throw new WebApplicationException(
                "Permission denied to " + rbacAuthorization.permission() + " for targetId: "
                    + rbacAuthorization.targetId() + " of type: " + rbacAuthorization.targetType(),
                Response.Status.FORBIDDEN);
          }
        } else {
          throw new WebApplicationException(
              "Permission denied: not able to find targetId: " + rbacAuthorization.targetId()
                  + " in the path or query parameters", Response.Status.FORBIDDEN);
        }
      }
    } else if (!accessControl.defaultRBACAuthorization(httpRequestIdentity)) {
      throw new WebApplicationException("Permission denied - default authorization failed", Response.Status.FORBIDDEN);
    }
  }

  private static boolean isBaseFile(String path) {
    return !path.contains("/") && path.contains(".");
  }
}
