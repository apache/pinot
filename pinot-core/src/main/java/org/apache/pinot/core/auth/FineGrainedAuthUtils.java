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

package org.apache.pinot.core.auth;

import java.lang.reflect.Method;
import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Utility methods to share in Broker and Controller request filters related to fine grain authorization.
public class FineGrainedAuthUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(FineGrainedAuthUtils.class);

  private FineGrainedAuthUtils() {
  }

  /// Returns the parameter from the path or query params.
  /// @param paramName to look for
  /// @param pathParams path params
  /// @param queryParams query params
  /// @return the value of the parameter
  private static String findParam(String paramName, MultivaluedMap<String, String> pathParams,
      MultivaluedMap<String, String> queryParams) {
    String name = pathParams.getFirst(paramName);
    if (name == null) {
      name = queryParams.getFirst(paramName);
    }
    return name;
  }

  /// Finds the raw target parameter identified by an [Authorize] annotation.
  ///
  /// @param auth annotation identifying the authorization target
  /// @param pathParams request path parameters
  /// @param queryParams request query parameters
  /// @return the unnormalized table parameter value, or `null` for a cluster target or missing table parameter
  @Nullable
  public static String findRawTargetId(Authorize auth, MultivaluedMap<String, String> pathParams,
      MultivaluedMap<String, String> queryParams) {
    return auth.targetType() == TargetType.TABLE ? findParam(auth.paramName(), pathParams, queryParams) : null;
  }

  /// Validate fine-grained authorization for APIs.
  /// There are 2 possible cases:
  /// 1. [Authorize] annotation is present on the method. In this case, do the finer grain authorization using the
  ///    fields of the annotation. There are 2 possibilities depending on the targetType ([TargetType]):
  ///    a. The targetType is [TargetType#CLUSTER]. In this case, the paramName field
  ///       ([Authorize#paramName()]) is not used, since the target is the Pinot cluster.
  ///    b. The targetType is [TargetType#TABLE]. In this case, the paramName field
  ///       ([Authorize#paramName()]) is mandatory, and it must be found in either the path parameters or the
  ///       query parameters.
  /// 2. [Authorize] annotation is not present on the method. In this use the default authorization.
  ///
  /// @param endpointMethod of the API
  /// @param uriInfo of the API
  /// @param httpHeaders of the API
  /// @param accessControl to check the fine-grained authorization
  public static void validateFineGrainedAuth(Method endpointMethod, UriInfo uriInfo, HttpHeaders httpHeaders,
      FineGrainedAccessControl accessControl) {
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
        targetId = findRawTargetId(auth, uriInfo.getPathParameters(), uriInfo.getQueryParameters());

        if (StringUtils.isBlank(targetId)) {
          throw new WebApplicationException(
              "Missing required table parameter '" + auth.paramName() + "' for API: " + uriInfo.getRequestUri(),
              Response.Status.BAD_REQUEST);
        }

        // Table name may contain type, hence get raw table name for checking access
        try {
          targetId = DatabaseUtils.translateTableName(TableNameBuilder.extractRawTableName(targetId), httpHeaders);
        } catch (RuntimeException e) {
          throw new WebApplicationException("Invalid table parameter '" + auth.paramName() + "': " + e.getMessage(),
              e, Response.Status.BAD_REQUEST);
        }

        accessDeniedMsg = "Access denied to " + auth.action() + " for table: " + targetId;
      } else if (auth.targetType() == TargetType.CLUSTER) {
        accessDeniedMsg = "Access denied to " + auth.action() + " in the cluster";
      } else {
        throw new WebApplicationException(
            "Unsupported targetType: " + auth.targetType() + " in API: " + uriInfo.getRequestUri(),
            Response.Status.INTERNAL_SERVER_ERROR);
      }

      boolean hasAccess;
      try {
        hasAccess = accessControl.hasAccess(httpHeaders, auth.targetType(), targetId, auth.action());
      } catch (Throwable t) {
        // catch and log Throwable for NoSuchMethodError which can happen when there are classpath conflicts
        // otherwise, grizzly will return a 500 without any logs or indication of what failed
        String errorMsg = "Failed to check for access for target type " + auth.targetType() + " and target ID "
            + targetId + " with action " + auth.action();
        LOGGER.error(errorMsg, t);
        throw new WebApplicationException(errorMsg, t, Response.Status.INTERNAL_SERVER_ERROR);
      }

      // Check for access now
      if (!hasAccess) {
        throw new WebApplicationException(accessDeniedMsg, Response.Status.FORBIDDEN);
      }
    } else if (!accessControl.defaultAccess(httpHeaders)) {
      throw new WebApplicationException("Access denied - default authorization failed", Response.Status.FORBIDDEN);
    }
  }
}
