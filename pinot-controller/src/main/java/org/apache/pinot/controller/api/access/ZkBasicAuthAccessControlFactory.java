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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.common.config.provider.AccessControlUserCache;
import org.apache.pinot.common.utils.BcryptUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.BasicAuthUtils;
import org.apache.pinot.core.auth.ZkBasicAuthPrincipal;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Zookeeper Basic Authentication based on Pinot Controller UI.
 * The user role has been distinguished by user and admin. Only admin can have access to the
 * user console page in Pinot controller UI. And admin can change user info (table permission/
 * number of tables/password etc.) or add/delete user without restarting your Pinot clusters,
 * and these changes happen immediately.
 * Users Configuration store in Helix Zookeeper and encrypted user password via Bcrypt Encryption Algorithm.
 *
 */
public class ZkBasicAuthAccessControlFactory implements AccessControlFactory {
  private static final String HEADER_AUTHORIZATION = "Authorization";

  private AccessControl _accessControl;

  @Override
  public void init(PinotConfiguration pinotConfiguration, PinotHelixResourceManager pinotHelixResourceManager)
      throws IOException {
    pinotHelixResourceManager.initUserACLConfig((ControllerConf) pinotConfiguration);
    _accessControl =
        new BasicAuthAccessControl(new AccessControlUserCache(pinotHelixResourceManager.getPropertyStore()));
  }

  @Override
  public AccessControl create() {
    return _accessControl;
  }

  /**
   * Access Control using header-based basic http authentication
   */
  private static class BasicAuthAccessControl implements AccessControl {
    private Map<String, ZkBasicAuthPrincipal> _name2principal;
    private final AccessControlUserCache _userCache;

    public BasicAuthAccessControl(AccessControlUserCache userCache) {
      _userCache = userCache;
    }

    @Override
    public boolean protectAnnotatedOnly() {
      return false;
    }

    @Override
    public boolean hasDataAccess(HttpHeaders httpHeaders, String tableName) {
      return getPrincipal(httpHeaders).filter(p -> p.hasTable(tableName)).isPresent();
    }

    @Override
    public boolean hasAccess(String tableName, AccessType accessType, HttpHeaders httpHeaders, String endpointUrl) {
      return getPrincipal(httpHeaders).filter(
          p -> p.hasTable(tableName) && p.hasPermission(Objects.toString(accessType))).isPresent();
    }

    @Override
    public boolean hasAccess(AccessType accessType, HttpHeaders httpHeaders, String endpointUrl) {
      return getPrincipal(httpHeaders).isPresent();
    }

    private Optional<ZkBasicAuthPrincipal> getPrincipal(HttpHeaders headers) {
      if (headers == null) {
        return Optional.empty();
      }

      _name2principal = BasicAuthUtils.extractBasicAuthPrincipals(_userCache.getAllControllerUserConfig()).stream()
          .collect(Collectors.toMap(ZkBasicAuthPrincipal::getName, p -> p));

      List<String> authHeaders = headers.getRequestHeader(HEADER_AUTHORIZATION);
      if (authHeaders == null) {
        return Optional.empty();
      }
      Map<String, String> name2password = authHeaders.stream().collect(
          Collectors.toMap(org.apache.pinot.common.auth.BasicAuthUtils::extractUsername,
              org.apache.pinot.common.auth.BasicAuthUtils::extractPassword));
      Map<String, ZkBasicAuthPrincipal> password2principal =
          name2password.keySet().stream().collect(Collectors.toMap(name2password::get, _name2principal::get));

      return password2principal.entrySet().stream().filter(
          entry -> BcryptUtils.checkpwWithCache(entry.getKey(), entry.getValue().getPassword(),
              _userCache.getUserPasswordAuthCache())).map(u -> u.getValue()).filter(Objects::nonNull).findFirst();
    }

    @Override
    public AuthWorkflowInfo getAuthWorkflowInfo() {
      return new AuthWorkflowInfo(AccessControl.WORKFLOW_BASIC);
    }
  }
}
