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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.common.auth.BasicAuthTokenUtils;
import org.apache.pinot.core.auth.BasicAuthPrincipal;
import org.apache.pinot.core.auth.BasicAuthPrincipalUtils;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Session-based Authentication Access Control Factory for Pinot Controller.
 *
 * <p><strong>NOTE: This factory is an OPTIONAL convenience class.</strong>
 * The preferred approach is to use your existing factory unchanged and just add
 * {@code controller.ui.session.authentication.enabled=true} to your configuration.
 * That approach works with ALL auth backends (BasicAuth, ZkBasicAuth, LDAP, custom).
 *
 * <p>This factory is identical to {@link BasicAuthAccessControlFactory} in terms of credential
 * validation (username/password via HTTP Basic Auth headers), but it reports the
 * {@link AccessControl#WORKFLOW_SESSION} workflow to the UI instead of {@code BASIC}.
 *
 * <p>Use this factory ONLY if you cannot add
 * {@code controller.ui.session.authentication.enabled=true} to your configuration
 * (e.g., in environments where config changes are restricted).
 *
 * <p><strong>Preferred configuration (works with any factory):</strong>
 * <pre>
 *   controller.ui.session.authentication.enabled=true
 *   controller.admin.access.control.factory.class=\
 *     org.apache.pinot.controller.api.access.BasicAuthAccessControlFactory
 * </pre>
 *
 * <p><strong>Alternative configuration using this factory:</strong>
 * <pre>
 *   controller.admin.access.control.factory.class=\
 *     org.apache.pinot.controller.api.access.SessionBasicAuthAccessControlFactory
 *   controller.admin.access.control.principals=admin,user
 *   controller.admin.access.control.principals.admin.password=verysecret
 * </pre>
 */
public class SessionBasicAuthAccessControlFactory implements AccessControlFactory {
  private static final String PREFIX = "controller.admin.access.control.principals";
  private static final String HEADER_AUTHORIZATION = "Authorization";

  private AccessControl _accessControl;

  @Override
  public void init(PinotConfiguration configuration) {
    _accessControl = new SessionBasicAuthAccessControl(
        BasicAuthPrincipalUtils.extractBasicAuthPrincipals(configuration, PREFIX));
  }

  @Override
  public AccessControl create() {
    return _accessControl;
  }

  /**
   * Access Control that uses Basic Auth credential validation but reports SESSION workflow.
   *
   * <p>When the UI calls {@code GET /auth/info}, this returns {@code {"workflow":"SESSION"}}
   * which causes the UI to use POST /auth/login instead of sending Authorization headers.
   */
  private static class SessionBasicAuthAccessControl implements AccessControl {
    private final Map<String, BasicAuthPrincipal> _token2principal;

    SessionBasicAuthAccessControl(Collection<BasicAuthPrincipal> principals) {
      _token2principal = principals.stream().collect(Collectors.toMap(BasicAuthPrincipal::getToken, p -> p));
    }

    @Override
    public boolean protectAnnotatedOnly() {
      return false;
    }

    @Override
    public boolean hasAccess(String tableName, AccessType accessType, HttpHeaders httpHeaders, String endpointUrl) {
      return getPrincipal(httpHeaders)
          .filter(p -> p.hasTable(tableName) && p.hasPermission(Objects.toString(accessType))).isPresent();
    }

    @Override
    public boolean hasAccess(AccessType accessType, HttpHeaders httpHeaders, String endpointUrl) {
      if (getPrincipal(httpHeaders).isEmpty()) {
        throw new NotAuthorizedException("Basic");
      }
      return true;
    }

    @Override
    public boolean hasAccess(HttpHeaders httpHeaders, TargetType targetType) {
      return getPrincipal(httpHeaders).isPresent();
    }

    /**
     * Reports SESSION workflow to the UI.
     * This causes the UI to use POST /auth/login instead of sending Authorization headers.
     */
    @Override
    public AuthWorkflowInfo getAuthWorkflowInfo() {
      return new AuthWorkflowInfo(AccessControl.WORKFLOW_SESSION);
    }

    private Optional<BasicAuthPrincipal> getPrincipal(HttpHeaders headers) {
      if (headers == null) {
        return Optional.empty();
      }
      List<String> authHeaders = headers.getRequestHeader(HEADER_AUTHORIZATION);
      if (authHeaders == null) {
        return Optional.empty();
      }
      return authHeaders.stream().map(BasicAuthTokenUtils::normalizeBase64Token)
          .map(_token2principal::get)
          .filter(Objects::nonNull).findFirst();
    }
  }
}
