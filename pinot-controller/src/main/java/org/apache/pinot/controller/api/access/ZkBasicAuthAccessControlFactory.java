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
import java.util.Optional;
import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.auth.BasicAuthTokenUtils;
import org.apache.pinot.common.config.provider.AccessControlUserCache;
import org.apache.pinot.common.utils.BcryptUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.BasicAuthPrincipalUtils;
import org.apache.pinot.core.auth.ZkBasicAuthPrincipal;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Zookeeper Basic Authentication based on Pinot Controller UI.
/// The user role has been distinguished by user and admin. Only admin can have access to the
/// user console page in Pinot controller UI. And admin can change user info (table permission/
/// number of tables/password etc.) or add/delete user without restarting your Pinot clusters,
/// and these changes happen immediately.
/// Users Configuration store in Helix Zookeeper and encrypted user password via Bcrypt Encryption Algorithm.
public class ZkBasicAuthAccessControlFactory implements AccessControlFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkBasicAuthAccessControlFactory.class);
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

  /// Access Control using header-based basic http authentication
  private static class BasicAuthAccessControl extends BaseBasicAuthAccessControl<ZkBasicAuthPrincipal> {
    private final AccessControlUserCache _userCache;

    public BasicAuthAccessControl(AccessControlUserCache userCache) {
      _userCache = userCache;
    }

    @Override
    protected Optional<ZkBasicAuthPrincipal> getPrincipal(HttpHeaders headers) {
      if (headers == null) {
        return Optional.empty();
      }

      List<String> authHeaders = headers.getRequestHeader(HEADER_AUTHORIZATION);
      if (authHeaders == null) {
        return Optional.empty();
      }

      for (String authHeader : authHeaders) {
        String username;
        String password;
        try {
          username = BasicAuthTokenUtils.extractUsername(authHeader);
          password = BasicAuthTokenUtils.extractPassword(authHeader);
        } catch (RuntimeException e) {
          continue;
        }
        if (StringUtils.isEmpty(username) || StringUtils.isEmpty(password)) {
          continue;
        }

        UserConfig userConfig = _userCache.getControllerUserConfigForUsername(username);
        if (userConfig == null) {
          continue;
        }

        ZkBasicAuthPrincipal principal;
        try {
          principal = BasicAuthPrincipalUtils.extractBasicAuthPrincipals(List.of(userConfig)).get(0);
        } catch (RuntimeException e) {
          // The cached user config is server-side state. Surface corrupt records without logging usernames, passwords,
          // authorization headers, or serialized user configs.
          LOGGER.warn("Failed to construct a BasicAuth principal from a cached controller user config due to {}",
              e.getClass().getSimpleName());
          continue;
        }

        if (passwordMatches(principal, password)) {
          return Optional.of(principal);
        }
      }
      return Optional.empty();
    }

    private boolean passwordMatches(ZkBasicAuthPrincipal principal, String password) {
      return BcryptUtils.checkpwWithCache(
          password,
          principal.getPassword(),
          _userCache.getUserPasswordAuthCache());
    }
  }
}
