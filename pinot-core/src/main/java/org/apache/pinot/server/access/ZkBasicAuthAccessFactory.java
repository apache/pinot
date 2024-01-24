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
package org.apache.pinot.server.access;

import io.netty.channel.ChannelHandlerContext;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.config.provider.AccessControlUserCache;
import org.apache.pinot.common.utils.BcryptUtils;
import org.apache.pinot.core.auth.BasicAuthUtils;
import org.apache.pinot.core.auth.ZkBasicAuthPrincipal;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Zookeeper Basic Authentication based on Pinot Controller UI.
 * The user role has been distinguished by user and admin. Only admin can have access to the
 * user console page in Pinot controller UI. And admin can change user info (table permission/
 * number of tables/password etc.) or add/delete user without restarting your Pinot clusters,
 * and these changes happen immediately.
 * Users Configuration store in Helix Zookeeper and encrypted user password via Bcrypt Encryption Algorithm.
 *
 */
public class ZkBasicAuthAccessFactory implements AccessControlFactory {
  private static final String AUTHORIZATION_KEY = "authorization";

  private HelixManager _helixManager;
  private AccessControl _accessControl;

  @Override
  public void init(PinotConfiguration configuration, HelixManager helixManager) {
    _helixManager = helixManager;
  }

  @Override
  public AccessControl create() {
    if (_accessControl == null) {
      _accessControl = new BasicAuthAccessControl(_helixManager);
    }
    return _accessControl;
  }

  /**
   * Access Control using metadata-based basic grpc authentication
   */
  private static class BasicAuthAccessControl implements AccessControl {
    private Map<String, ZkBasicAuthPrincipal> _name2principal;
    private AccessControlUserCache _userCache;
    private HelixManager _innerHelixManager;

    public BasicAuthAccessControl(HelixManager helixManager) {
      _innerHelixManager = helixManager;
    }

    public void initUserCache() {
      if (_userCache == null) {
        _userCache = new AccessControlUserCache(_innerHelixManager.getHelixPropertyStore());
      }
    }

    @Override
    public boolean isAuthorizedChannel(ChannelHandlerContext channelHandlerContext) {
      return true;
    }

    @Override
    public boolean hasDataAccess(RequesterIdentity requesterIdentity, String tableName) {
      if (_userCache == null) {
        initUserCache();
      }
      Collection<String> tokens = getTokens(requesterIdentity);
      _name2principal = BasicAuthUtils.extractBasicAuthPrincipals(_userCache.getAllServerUserConfig()).stream()
          .collect(Collectors.toMap(ZkBasicAuthPrincipal::getName, p -> p));

      Map<String, String> name2password = tokens.stream().collect(
          Collectors.toMap(org.apache.pinot.common.auth.BasicAuthUtils::extractUsername,
              org.apache.pinot.common.auth.BasicAuthUtils::extractPassword));
      Map<String, ZkBasicAuthPrincipal> password2principal =
          name2password.keySet().stream().collect(Collectors.toMap(name2password::get, _name2principal::get));
      return password2principal.entrySet().stream().filter(
          entry -> BcryptUtils.checkpwWithCache(entry.getKey(), entry.getValue().getPassword(),
              _userCache.getUserPasswordAuthCache())).map(u -> u.getValue()).filter(Objects::nonNull).findFirst().map(
          zkprincipal -> StringUtils.isEmpty(tableName) || zkprincipal.hasTable(
              TableNameBuilder.extractRawTableName(tableName))).orElse(false);
    }

    private Collection<String> getTokens(RequesterIdentity requesterIdentity) {
      if (requesterIdentity instanceof GrpcRequesterIdentity) {
        GrpcRequesterIdentity identity = (GrpcRequesterIdentity) requesterIdentity;
        return identity.getGrpcMetadata().get(AUTHORIZATION_KEY);
      }
      if (requesterIdentity instanceof HttpRequesterIdentity) {
        HttpRequesterIdentity identity = (HttpRequesterIdentity) requesterIdentity;
        return identity.getHttpHeaders().get(AUTHORIZATION_KEY);
      }
      throw new UnsupportedOperationException("GrpcRequesterIdentity or HttpRequesterIdentity is required");
    }
  }
}
