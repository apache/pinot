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
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.glassfish.grizzly.http.server.Request;


/**
 * A container filter class responsible for automatic authentication of REST endpoints. Any rest endpoints annotated
 * with {@link Authenticate} annotation, will go through authentication.
 */
@javax.ws.rs.ext.Provider
public class AuthenticationFilter implements ContainerRequestFilter {

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
    // check if authentication is required
    Method endpointMethod = _resourceInfo.getResourceMethod();
    if (endpointMethod.isAnnotationPresent(Authenticate.class)) {
      // Perform authentication:
      // Note that table name is extracted from "path parameters" or "query parameters" if it's defined as one of the
      // followings:
      //     - "tableName",
      //     - "tableNameWithType", or
      //     - "schemaName"
      // If table name is not available, it means the endpoint is not a table-level endpoint.
      AccessControlUtils accessControlUtils = new AccessControlUtils();
      AccessType accessType = endpointMethod.getAnnotation(Authenticate.class).value();
      String endpointUrl = _requestProvider.get().getRequestURL().toString();
      UriInfo uriInfo = requestContext.getUriInfo();
      Optional<String> tableName = extractTableName(uriInfo.getPathParameters(), uriInfo.getQueryParameters());
      accessControlUtils
          .validatePermission(tableName, accessType, _httpHeaders, endpointUrl, _accessControlFactory.create());
    }
  }

  @VisibleForTesting
  Optional<String> extractTableName(MultivaluedMap<String, String> pathParameters,
      MultivaluedMap<String, String> queryParameters) {
    Optional<String> tableName = extractTableName(pathParameters);
    if (tableName.isPresent()) {
      return tableName;
    }
    return extractTableName(queryParameters);
  }

  private Optional<String> extractTableName(MultivaluedMap<String, String> mmap) {
    String tableName = mmap.getFirst("tableName");
    if (tableName == null) {
      tableName = mmap.getFirst("tableNameWithType");
      if (tableName == null) {
        tableName = mmap.getFirst("schemaName");
      }
    }
    return Optional.ofNullable(tableName);
  }
}
