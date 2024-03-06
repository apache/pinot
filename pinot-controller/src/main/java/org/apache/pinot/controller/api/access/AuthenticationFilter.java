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

package org.apache.pinot.controller.api.access;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.core.auth.FineGrainedAuthUtils;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.glassfish.grizzly.http.server.Request;


/**
 * A container filter class responsible for automatic authentication of REST endpoints. Any rest endpoints annotated
 * with {@link Authenticate} annotation, will go through authentication.
 */
@javax.ws.rs.ext.Provider
public class AuthenticationFilter implements ContainerRequestFilter {
  private static final Set<String> UNPROTECTED_PATHS =
      new HashSet<>(Arrays.asList("", "help", "auth/info", "auth/verify", "health"));
  private static final String KEY_TABLE_NAME = "tableName";
  private static final String KEY_TABLE_NAME_WITH_TYPE = "tableNameWithType";
  private static final String KEY_SCHEMA_NAME = "schemaName";

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
    String endpointUrl = request.getRequestURI().substring(request.getContextPath().length()); // extract path only
    UriInfo uriInfo = requestContext.getUriInfo();

    // exclude public/unprotected paths
    if (isBaseFile(uriInfo.getPath()) || UNPROTECTED_PATHS.contains(uriInfo.getPath())) {
      return;
    }

    // check if authentication is required implicitly
    if (accessControl.protectAnnotatedOnly() && !endpointMethod.isAnnotationPresent(Authenticate.class)) {
      return;
    }

    // check if the method's authorization is disabled (i.e. performed manually within method)
    if (endpointMethod.isAnnotationPresent(ManualAuthorization.class)) {
      return;
    }

    // Note that table name is extracted from "path parameters" or "query parameters" if it's defined as one of the
    // followings:
    //     - "tableName",
    //     - "tableNameWithType", or
    //     - "schemaName"
    // If table name is not available, it means the endpoint is not a table-level endpoint.
    String tableName = extractTableName(uriInfo.getPathParameters(), uriInfo.getQueryParameters());
    if (tableName != null) {
      // If table name is present translat it to the fully qualified name based on database header.
      tableName = DatabaseUtils.translateTableName(tableName, _httpHeaders);
    }
    AccessType accessType = extractAccessType(endpointMethod);
    AccessControlUtils.validatePermission(tableName, accessType, _httpHeaders, endpointUrl, accessControl);

    FineGrainedAuthUtils.validateFineGrainedAuth(endpointMethod, uriInfo, _httpHeaders, accessControl);
  }

  @VisibleForTesting
  AccessType extractAccessType(Method endpointMethod) {
    if (endpointMethod.isAnnotationPresent(Authenticate.class)) {
      return endpointMethod.getAnnotation(Authenticate.class).value();
    } else {
      // heuristically infer access type via javax.ws.rs annotations
      if (endpointMethod.getAnnotation(POST.class) != null) {
        return AccessType.CREATE;
      } else if (endpointMethod.getAnnotation(PUT.class) != null) {
        return AccessType.UPDATE;
      } else if (endpointMethod.getAnnotation(DELETE.class) != null) {
        return AccessType.DELETE;
      }
    }

    return AccessType.READ;
  }

  @VisibleForTesting
  static String extractTableName(MultivaluedMap<String, String> pathParameters,
      MultivaluedMap<String, String> queryParameters) {
    String tableName = extractTableName(pathParameters);
    if (tableName != null) {
      return tableName;
    }
    return extractTableName(queryParameters);
  }

  private static String extractTableName(MultivaluedMap<String, String> mmap) {
    if (mmap.containsKey(KEY_TABLE_NAME)) {
      return mmap.getFirst(KEY_TABLE_NAME);
    }
    if (mmap.containsKey(KEY_TABLE_NAME_WITH_TYPE)) {
      return mmap.getFirst(KEY_TABLE_NAME_WITH_TYPE);
    }
    if (mmap.containsKey(KEY_SCHEMA_NAME)) {
      return mmap.getFirst(KEY_SCHEMA_NAME);
    }
    return null;
  }

  private static boolean isBaseFile(String path) {
    return !path.contains("/") && path.contains(".");
  }
}
