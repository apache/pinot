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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Utility methods to share in Broker and Controller request filters related to fine grain authorization.
 */
public class FineGrainedAuthUtils {
  private FineGrainedAuthUtils() {
  }

  /**
   * Returns the parameter from the path or query params.
   * @param paramName to look for
   * @param pathParams path params
   * @param queryParams query params
   * @return the value of the parameter
   */
  private static String findParam(String paramName, MultivaluedMap<String, String> pathParams,
      MultivaluedMap<String, String> queryParams) {
    String name = pathParams.getFirst(paramName);
    if (name == null) {
      name = queryParams.getFirst(paramName);
    }
    return name;
  }

  /**
   * Validate fine-grained authorization for APIs.
   * There are 2 possible cases:
   * 1. {@link Authorize} annotation is present on the method. In this case, do the finer grain authorization using the
   *    fields of the annotation. There are 2 possibilities depending on the targetType ({@link TargetType}):
   *    a. The targetType is {@link TargetType#CLUSTER}. In this case, the paramName field
   *       ({@link Authorize#paramName()}) is not used, since the target is the Pinot cluster.
   *    b. The targetType is {@link TargetType#TABLE}. In this case, the paramName field
   *       ({@link Authorize#paramName()}) is mandatory, and it must be found in either the path parameters or the
   *       query parameters.
   * 2. {@link Authorize} annotation is not present on the method. In this use the default authorization.
   *
   * @param endpointMethod of the API
   * @param uriInfo of the API
   * @param httpHeaders of the API
   * @param accessControl to check the fine-grained authorization
   */
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
        targetId = findParam(auth.paramName(), uriInfo.getPathParameters(), uriInfo.getQueryParameters());

        if (StringUtils.isEmpty(targetId)) {
          throw new WebApplicationException(
              "Could not find paramName " + auth.paramName() + " in path or query params of the API: "
                  + uriInfo.getRequestUri(), Response.Status.INTERNAL_SERVER_ERROR);
        }

        // Table name may contain type, hence get raw table name for checking access
        targetId = DatabaseUtils.translateTableName(TableNameBuilder.extractRawTableName(targetId), httpHeaders);

        accessDeniedMsg = "Access denied to " + auth.action() + " for table: " + targetId;
      } else if (auth.targetType() == TargetType.CLUSTER) {
        accessDeniedMsg = "Access denied to " + auth.action() + " in the cluster";
      } else {
        throw new WebApplicationException(
            "Unsupported targetType: " + auth.targetType() + " in API: " + uriInfo.getRequestUri(),
            Response.Status.INTERNAL_SERVER_ERROR);
      }

      // Check for access now
      if (!accessControl.hasAccess(httpHeaders, auth.targetType(), targetId, auth.action())) {
        throw new WebApplicationException(accessDeniedMsg, Response.Status.FORBIDDEN);
      }
    } else if (!accessControl.defaultAccess(httpHeaders)) {
      throw new WebApplicationException("Access denied - default authorization failed", Response.Status.FORBIDDEN);
    }
  }
}
