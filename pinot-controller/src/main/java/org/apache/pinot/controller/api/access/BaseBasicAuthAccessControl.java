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

import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.core.auth.BasicAuthPrincipal;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Shared controller BasicAuth policy, independent of how principals are loaded and credentials are verified.
/// This class is stateless and thread-safe; subclass principal resolution must also be thread-safe.
abstract class BaseBasicAuthAccessControl<P extends BasicAuthPrincipal> implements AccessControl {
  @Override
  public final boolean protectAnnotatedOnly() {
    return false;
  }

  @Override
  public final boolean hasAccess(@Nullable String tableName, AccessType accessType, HttpHeaders httpHeaders,
      String endpointUrl) {
    // A null table name means the request named no table, which makes it cluster-wide however the caller reached this
    // overload. Route it to the cluster check so the scope rule cannot be sidestepped by omitting the table: callers
    // include AccessControl's own default cluster overload and the /auth/verify probe, whose table name is optional.
    if (tableName == null) {
      return hasAccess(accessType, httpHeaders, endpointUrl);
    }
    Optional<P> principal = getPrincipal(httpHeaders);
    if (principal.isEmpty()) {
      throw new NotAuthorizedException("Basic");
    }
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    P authenticatedPrincipal = principal.get();
    return authenticatedPrincipal.hasTable(rawTableName)
        && authenticatedPrincipal.hasPermission(Objects.toString(accessType));
  }

  /// Guards endpoints that name no table. Such a request is cluster-wide, so beyond the requested permission it
  /// requires a principal whose table scope is unrestricted: a principal confined to a subset of tables must not reach
  /// cluster state that lies outside that subset.
  @Override
  public final boolean hasAccess(AccessType accessType, HttpHeaders httpHeaders, String endpointUrl) {
    Optional<P> principal = getPrincipal(httpHeaders);
    if (principal.isEmpty()) {
      throw new NotAuthorizedException("Basic");
    }
    P authenticatedPrincipal = principal.get();
    return authenticatedPrincipal.hasUnrestrictedTableAccess()
        && authenticatedPrincipal.hasPermission(Objects.toString(accessType));
  }

  @Override
  public final boolean hasAccess(HttpHeaders httpHeaders, TargetType targetType, String targetId, String action) {
    // Basic auth permissions are CRUD access types, not action names. AuthenticationFilter enforces the resolved
    // AccessType before invoking this fine-grained check, so this overload must only prevent unauthenticated access.
    // In particular it must not require unrestricted table scope for TargetType.CLUSTER: several table-scoped
    // endpoints declare a cluster action, and for those the filter already resolved the table name and ran the
    // table-scoped check above. Endpoints that name no table go through the cluster-wide check above instead.
    return getPrincipal(httpHeaders).isPresent();
  }

  /// Reports whether the principal may act on the given target type at all, without a specific target or action.
  /// This backs `auth/verify/v2`, which the controller UI calls as its login gate and renders as "invalid credentials"
  /// when it returns false, so it must stay an authentication check rather than report cluster capability — otherwise
  /// a table-scoped principal is told its correct credentials are invalid. Authorization for a specific request is
  /// decided by the overloads above, so such a principal signs in and reaches its own tables through the table-scoped
  /// endpoints, while cluster-wide views such as `GET /tables` remain denied.
  @Override
  public final boolean hasAccess(HttpHeaders httpHeaders, TargetType targetType) {
    return getPrincipal(httpHeaders).isPresent();
  }

  @Override
  public final AuthWorkflowInfo getAuthWorkflowInfo() {
    return new AuthWorkflowInfo(AccessControl.WORKFLOW_BASIC);
  }

  protected abstract Optional<P> getPrincipal(HttpHeaders headers);
}
