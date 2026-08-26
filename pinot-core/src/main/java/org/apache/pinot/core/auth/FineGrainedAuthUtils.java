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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.ws.rs.QueryParam;
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
  /// Memoizes [#declaredQueryParams(Method)]; bounded by the number of endpoints.
  private static final Map<Method, Set<String>> DECLARED_QUERY_PARAMS = new ConcurrentHashMap<>();

  private FineGrainedAuthUtils() {
  }

  /// Returns the names `endpointMethod` binds as [QueryParam]s.
  ///
  /// Only method-level `@QueryParam` binding is recognized; an endpoint binding parameters through `@BeanParam` or
  /// resource-class field injection is treated as declaring none. That direction denies table scope rather than
  /// granting it, so it fails closed. No in-tree controller or broker resource uses either form today.
  ///
  /// Results are memoized because [Method#getParameterAnnotations()] re-parses the class-file annotation bytes on
  /// every call, and this runs on every request before authentication. The key set is bounded by the number of
  /// endpoints.
  public static Set<String> declaredQueryParams(Method endpointMethod) {
    return DECLARED_QUERY_PARAMS.computeIfAbsent(endpointMethod, FineGrainedAuthUtils::findDeclaredQueryParams);
  }

  private static Set<String> findDeclaredQueryParams(Method endpointMethod) {
    Set<String> declared = new HashSet<>();
    for (Annotation[] parameterAnnotations : endpointMethod.getParameterAnnotations()) {
      for (Annotation parameterAnnotation : parameterAnnotations) {
        if (parameterAnnotation instanceof QueryParam queryParam) {
          declared.add(queryParam.value());
        }
      }
    }
    return Set.copyOf(declared);
  }

  /// Finds the raw target parameter identified by an [Authorize] annotation.
  ///
  /// Path parameters are template variables of the endpoint's own `@Path` and are always trusted. Query parameters
  /// are caller-supplied, so only a name the method binds as `@QueryParam` may identify the table.
  ///
  /// @param auth annotation identifying the authorization target
  /// @param endpointMethod the resource method, used to decide which query parameters are declared
  /// @param pathParams request path parameters
  /// @param queryParams request query parameters
  /// @return the unnormalized table parameter value, or `null` for a cluster target or missing table parameter
  @Nullable
  public static String findRawTargetId(Authorize auth, Method endpointMethod,
      MultivaluedMap<String, String> pathParams, MultivaluedMap<String, String> queryParams) {
    if (auth.targetType() != TargetType.TABLE) {
      return null;
    }
    String targetId = pathParams.getFirst(auth.paramName());
    if (targetId == null && declaredQueryParams(endpointMethod).contains(auth.paramName())) {
      targetId = queryParams.getFirst(auth.paramName());
    }
    return targetId;
  }

  /// Validate fine-grained authorization for APIs.
  /// There are 2 possible cases:
  /// 1. [Authorize] annotation is present on the method. In this case, do the finer grain authorization using the
  ///    fields of the annotation. There are 2 possibilities depending on the targetType ([TargetType]):
  ///    a. The targetType is [TargetType#CLUSTER]. In this case, the paramName field
  ///       ([Authorize#paramName()]) is not used, since the target is the Pinot cluster.
  ///    b. The targetType is [TargetType#TABLE]. In this case, the paramName field
  ///       ([Authorize#paramName()]) is mandatory, and it must be found in the path parameters or a query
  ///       parameter the method binds as `@QueryParam`.
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

        // Path params are part of the endpoint declaration. Query params are caller-supplied, so only a name
        // the method binds as @QueryParam may identify the table. Otherwise a caller could append
        // ?tableName=<a table it is scoped to> and have a cluster-wide request authorized as table-scoped.
        targetId = findRawTargetId(auth, endpointMethod, uriInfo.getPathParameters(), uriInfo.getQueryParameters());

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
