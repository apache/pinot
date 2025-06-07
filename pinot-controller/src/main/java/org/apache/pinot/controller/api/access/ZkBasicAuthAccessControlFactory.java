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
import java.util.Optional;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.common.config.provider.AccessControlUserCache;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.BasicAuthUtils;
import org.apache.pinot.core.auth.ZkBasicAuthPrincipal;
import org.apache.pinot.spi.config.user.ComponentType;
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
  private static class BasicAuthAccessControl extends AbstractBasicAuthAccessControl {
    private final AccessControlUserCache _userCache;

    public BasicAuthAccessControl(AccessControlUserCache userCache) {
      _userCache = userCache;
    }

    @Override
    protected Optional<ZkBasicAuthPrincipal> getPrincipal(HttpHeaders httpHeaders) {
      return BasicAuthUtils.getPrincipal(getTokens(httpHeaders), _userCache, ComponentType.CONTROLLER);
    }
  }
}
