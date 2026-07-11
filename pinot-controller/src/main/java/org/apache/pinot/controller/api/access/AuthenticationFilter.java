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
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.FineGrainedAuthUtils;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.TargetType;
import org.glassfish.grizzly.http.server.Request;


/// A container filter class responsible for automatic authentication of REST endpoints. Any rest endpoints annotated
/// with [Authenticate] annotation, will go through authentication.
@javax.ws.rs.ext.Provider
public class AuthenticationFilter implements ContainerRequestFilter {
  private static final Set<String> UNPROTECTED_PATHS =
      new HashSet<>(Arrays.asList("", "help", "auth/info", "auth/verify", "auth/verify/v2", "health"));
  private static final String KEY_TABLE_NAME = "tableName";
  private static final String KEY_TABLE_NAME_WITH_TYPE = "tableNameWithType";
  private static final String KEY_SCHEMA_NAME = "schemaName";
  /// Parameter names that identify the table an endpoint acts on, in the order they are consulted.
  private static final List<String> TABLE_NAME_KEYS =
      List.of(KEY_TABLE_NAME, KEY_TABLE_NAME_WITH_TYPE, KEY_SCHEMA_NAME);
  /// Memoizes [#findDeclaredQueryParams(Method)]; bounded by the number of endpoints.
  private static final Map<Method, Set<String>> DECLARED_QUERY_PARAMS = new ConcurrentHashMap<>();

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
    if (isBaseFile(AuthProviderUtils.stripMatrixParams(uriInfo.getPath()))
        || UNPROTECTED_PATHS.contains(AuthProviderUtils.stripMatrixParams(uriInfo.getPath()))) {
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
    // A declared table target can identify a custom parameter name. For cluster-targeted annotations, retain the
    // parameter-name heuristics because several legacy table-scoped endpoints use cluster actions for fine-grained
    // authorization. If table name is not available, it means the endpoint is not a table-level endpoint.
    String tableName = extractTableName(endpointMethod, uriInfo.getPathParameters(), uriInfo.getQueryParameters());
    if (tableName != null) {
      // If table name is present, translate it to the fully qualified name based on database header.
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

  /// Resolves the table `endpointMethod` acts on, or `null` when the request addresses the cluster rather than a
  /// table. `AccessControlUtils.validatePermission` picks the table-scoped or the cluster-wide check on that answer,
  /// so it must not be steerable by the caller.
  ///
  /// Path parameters are template variables of the endpoint's own `@Path`, hence part of its declaration. Query
  /// parameters are caller-supplied and JAX-RS surfaces every one present on the URI, so only those the endpoint
  /// declares may name the table: otherwise a caller could append `?tableName=<a table it is scoped to>` to a cluster
  /// endpoint and have it authorized as a table-scoped request.
  @Nullable
  @VisibleForTesting
  static String extractTableName(Method endpointMethod, MultivaluedMap<String, String> pathParameters,
      MultivaluedMap<String, String> queryParameters) {
    Authorize authorize = endpointMethod.getAnnotation(Authorize.class);
    if (authorize != null && authorize.targetType() == TargetType.TABLE) {
      // The annotation names the parameter, but it is only trustworthy where the endpoint also binds it.
      MultivaluedMap<String, String> trustedQueryParameters =
          declaredQueryParams(endpointMethod).contains(authorize.paramName()) ? queryParameters
              : new MultivaluedHashMap<>();
      return FineGrainedAuthUtils.findRawTargetId(authorize, pathParameters, trustedQueryParameters);
    }
    String tableName = extractTableName(pathParameters);
    if (tableName != null) {
      return tableName;
    }
    Set<String> declaredQueryParams = declaredQueryParams(endpointMethod);
    for (String key : TABLE_NAME_KEYS) {
      if (queryParameters.containsKey(key) && declaredQueryParams.contains(key)) {
        return queryParameters.getFirst(key);
      }
    }
    return null;
  }

  /// Returns the names `endpointMethod` binds as [QueryParam]s.
  ///
  /// Only method-level `@QueryParam` binding is recognized; an endpoint binding parameters through `@BeanParam` or
  /// resource-class field injection is treated as declaring none. That direction denies table scope rather than
  /// granting it, so it fails closed — no controller resource uses either form today.
  ///
  /// Results are memoized because [Method#getParameterAnnotations()] re-parses the class-file annotation bytes on
  /// every call, and this runs on every request before authentication. The key set is bounded by the number of
  /// endpoints.
  private static Set<String> declaredQueryParams(Method endpointMethod) {
    return DECLARED_QUERY_PARAMS.computeIfAbsent(endpointMethod, AuthenticationFilter::findDeclaredQueryParams);
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

  @Nullable
  private static String extractTableName(MultivaluedMap<String, String> mmap) {
    for (String key : TABLE_NAME_KEYS) {
      if (mmap.containsKey(key)) {
        return mmap.getFirst(key);
      }
    }
    return null;
  }

  private static boolean isBaseFile(String path) {
    return !path.contains("/") && path.contains(".");
  }
}
