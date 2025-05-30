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
package org.apache.pinot.broker.broker;

import java.util.Optional;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.common.config.provider.AccessControlUserCache;
import org.apache.pinot.core.auth.BasicAuthUtils;
import org.apache.pinot.core.auth.ZkBasicAuthPrincipal;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Zookeeper Basic Authentication for brokers. The user role has been distinguished by user and admin.
 * Users Configurations are stored in Helix Zookeeper and user password are encrypted via Bcrypt Encryption Algorithm.
 */
public class ZkBasicAuthAccessControlFactory extends AccessControlFactory {

  private AccessControl _accessControl;

  @Override
  public void init(PinotConfiguration configuration, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    // Build an AccessControlUserCache for user info from ZK, then create AccessControl.
    _accessControl = new BasicAuthAccessControl(new AccessControlUserCache(propertyStore));
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

    BasicAuthAccessControl(AccessControlUserCache userCache) {
      _userCache = userCache;
    }

    @Override
    protected Optional<ZkBasicAuthPrincipal> getPrincipal(RequesterIdentity requesterIdentity) {
      return BasicAuthUtils.getPrincipal(getTokens(requesterIdentity), _userCache, ComponentType.BROKER);
    }
  }
}
