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
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.RBACAuthUtils;
import org.apache.pinot.core.auth.TargetType;
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

    handleFinerGrainAuth(endpointMethod, uriInfo, accessControl, httpRequestIdentity);
  }

  /**
   * Check for finer grain authorization of APIs.
   * There are 2 possible cases:
   * 1. {@link Authorize} annotation is present on the method. In this case, do the finer grain authorization using the
   *    fields of the annotation. There are 2 possibilities depending on the targetType ({@link TargetType}):
   *    a. The targetType is {@link TargetType#CLUSTER}. In this case, the paramName field ({@link Authorize#paramName()})
   *    is not used, since the target is the Pinot cluster.
   *    b. The targetType is {@link TargetType#TABLE}. In this case, the paramName field ({@link Authorize#paramName()})
   *    is mandatory, and it must be found in either the path parameters or the query parameters.
   * 2. {@link Authorize} annotation is not present on the method. In this use the default authorization.
   *
   * @param endpointMethod of the API
   * @param uriInfo of the API
   * @param accessControl to check the access
   * @param httpRequestIdentity of the requester
   */
  private void handleFinerGrainAuth(Method endpointMethod, UriInfo uriInfo, AccessControl accessControl,
      HttpRequesterIdentity httpRequestIdentity) {
    if (endpointMethod.isAnnotationPresent(Authorize.class)) {
      final Authorize auth = endpointMethod.getAnnotation(Authorize.class);
      String targetId = null;
      // Message to use in the access denied exception
      String accessDeniedMsg;
      if (auth.targetType() == TargetType.TABLE) {
        // paramName is mandatory for table level authorization
        if (StringUtils.isEmpty(auth.paramName())) {
          throw new WebApplicationException(
              "paramName not found for table level authorization in API: " + uriInfo.getRequestUri(),
              Response.Status.INTERNAL_SERVER_ERROR);
        }

        // find the paramName in the path or query params
        targetId = RBACAuthUtils.findParam(auth.paramName(), uriInfo.getPathParameters(), uriInfo.getQueryParameters());

        if (StringUtils.isEmpty(targetId)) {
          throw new WebApplicationException(
              "Could not find paramName " + auth.paramName() + " in path or query params of the API: "
                  + uriInfo.getRequestUri(), Response.Status.INTERNAL_SERVER_ERROR);
        }
        accessDeniedMsg = "Access denied to " + auth.action() + " for table: " + targetId;
      } else if (auth.targetType() == TargetType.CLUSTER) {
        accessDeniedMsg = "Access denied to " + auth.action() + " in the cluster";
      } else {
        throw new WebApplicationException(
            "Unsupported targetType: " + auth.targetType() + " in API: " + uriInfo.getRequestUri(),
            Response.Status.INTERNAL_SERVER_ERROR);
      }

      // Check for access now
      if (!accessControl.hasAccess(httpRequestIdentity, auth.targetType(), targetId, auth.action())) {
        throw new WebApplicationException(accessDeniedMsg, Response.Status.FORBIDDEN);
      }
    } else if (!accessControl.defaultAccess(httpRequestIdentity)) {
      throw new WebApplicationException("Access denied - default authorization failed", Response.Status.FORBIDDEN);
    }
  }

  private static boolean isBaseFile(String path) {
    return !path.contains("/") && path.contains(".");
  }
}
