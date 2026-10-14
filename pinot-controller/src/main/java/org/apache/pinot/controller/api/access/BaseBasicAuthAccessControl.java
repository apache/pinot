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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.core.auth.BasicAuthPrincipal;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Shared controller BasicAuth policy, independent of how principals are loaded and credentials are verified.
/// Thread-safe; its only state is the set of principals already reported in a table-scope denial log. Subclass
/// principal resolution must also be thread-safe.
abstract class BaseBasicAuthAccessControl<P extends BasicAuthPrincipal> implements AccessControl {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseBasicAuthAccessControl.class);

  /// Principal names already reported by [#warnOnceAboutTableScope]; bounded by the number of configured principals.
  private final Set<String> _tableScopeDenialsReported = ConcurrentHashMap.newKeySet();

  @Override
  public final boolean protectAnnotatedOnly() {
    return false;
  }

  @Override
  public final boolean hasAccess(@Nullable String tableName, AccessType accessType, HttpHeaders httpHeaders,
      String endpointUrl) {
    // A null table name means the request named no table, which makes it cluster-wide however the caller reached this
    // overload. Route it to the cluster check so the scope rule cannot be sidestepped by omitting the table — the
    // /auth/verify probe takes an optional table name and reaches this overload directly. Safe because the cluster
    // overload below is overridden here rather than left as the interface default, which would delegate back.
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
  /// cluster state that lies outside that subset. That includes realtime segment-completion callbacks
  /// (`LLCSegmentCompletionHandlers`) and minion task callbacks that carry no table name, so a table-scoped server
  /// or minion token is denied and ingestion stalls until the token is moved to an unrestricted principal.
  @Override
  public final boolean hasAccess(AccessType accessType, HttpHeaders httpHeaders, String endpointUrl) {
    Optional<P> principal = getPrincipal(httpHeaders);
    if (principal.isEmpty()) {
      throw new NotAuthorizedException("Basic");
    }
    P authenticatedPrincipal = principal.get();
    if (!authenticatedPrincipal.hasUnrestrictedTableAccess()) {
      warnOnceAboutTableScope(authenticatedPrincipal, endpointUrl);
      return false;
    }
    return authenticatedPrincipal.hasPermission(Objects.toString(accessType));
  }

  /// Reports the first cluster-level denial caused by table scope for each principal. Without it the only trace is a
  /// generic 403, which gives an operator no hint that the table list is the cause — and the affected callers include
  /// service identities driving realtime segment completion and minion task callbacks, where the visible symptom is
  /// stalled ingestion rather than a failed API call. Reported on denial rather than at startup so that principals
  /// created later, which is how the ZooKeeper-backed factory is used, are covered too.
  private void warnOnceAboutTableScope(P principal, String endpointUrl) {
    if (_tableScopeDenialsReported.add(principal.getName())) {
      LOGGER.warn("Denied BasicAuth principal '{}' access to '{}': the endpoint names no table, so it requires a "
              + "principal without a table allow-list or exclude-list. Grant unrestricted table scope to any "
              + "principal used as a service identity.", principal.getName(), endpointUrl);
    }
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
