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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.ws.rs.NotAuthorizedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.auth.BasicAuthTokenUtils;
import org.apache.pinot.common.config.provider.AccessControlUserCache;
import org.apache.pinot.common.utils.BcryptUtils;
import org.apache.pinot.core.auth.BasicAuthPrincipalUtils;
import org.apache.pinot.core.auth.ZkBasicAuthPrincipal;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.apache.pinot.spi.auth.BasicAuthorizationResultImpl;
import org.apache.pinot.spi.auth.server.RequesterIdentity;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.RoleType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Zookeeper Basic Authentication based on Pinot Controller UI.
/// The user role has been distinguished by user and admin. Only admin can have access to the
/// user console page in Pinot controller UI. And admin can change user info (table permission/
/// number of tables/password etc.) or add/delete user without restarting your Pinot clusters,
/// and these changes happen immediately.
/// Users Configuration store in Helix Zookeeper and encrypted user password via Bcrypt Encryption Algorithm.
public class ZkBasicAuthAccessFactory implements AccessControlFactory {
  private static final String AUTHORIZATION_KEY = "authorization";

  private HelixManager _helixManager;
  private final AtomicReference<AccessControl> _accessControl = new AtomicReference<>();

  @Override
  public void init(PinotConfiguration configuration, HelixManager helixManager) {
    _helixManager = helixManager;
  }

  @Override
  public AccessControl create() {
    AccessControl accessControl = _accessControl.get();
    if (accessControl == null) {
      _accessControl.compareAndSet(null, new BasicAuthAccessControl(_helixManager));
      accessControl = _accessControl.get();
    }
    return accessControl;
  }

  /// Access Control using metadata-based basic grpc authentication
  private static class BasicAuthAccessControl implements AccessControl {
    private volatile AccessControlUserCache _userCache;
    private final HelixManager _innerHelixManager;

    public BasicAuthAccessControl(HelixManager helixManager) {
      _innerHelixManager = helixManager;
    }

    public synchronized void initUserCache() {
      if (_userCache == null) {
        _userCache = new AccessControlUserCache(_innerHelixManager.getHelixPropertyStore());
      }
    }

    @Override
    public boolean isAuthorizedChannel(ChannelHandlerContext channelHandlerContext) {
      return true;
    }

    @Override
    public AuthorizationResult authorizeAdminAccess(RequesterIdentity requesterIdentity) {
      Optional<ZkBasicAuthPrincipal> principal = getPrincipal(requesterIdentity);
      if (!principal.isPresent()) {
        throw new NotAuthorizedException("Basic");
      }
      return new BasicAuthorizationResultImpl(
          principal.get().hasPermission(RoleType.ADMIN, ComponentType.SERVER));
    }

    @Override
    public boolean hasDataAccess(RequesterIdentity requesterIdentity, String tableName) {
      return getPrincipal(requesterIdentity)
          .map(principal -> StringUtils.isEmpty(tableName) || principal.hasTable(
              TableNameBuilder.extractRawTableName(tableName)))
          .orElse(false);
    }

    private Optional<ZkBasicAuthPrincipal> getPrincipal(RequesterIdentity requesterIdentity) {
      Collection<String> tokens = getTokens(requesterIdentity);
      if (tokens.isEmpty()) {
        return Optional.empty();
      }

      Map<String, String> name2password = new LinkedHashMap<>();
      for (String token : tokens) {
        String decodedToken;
        try {
          decodedToken = BasicAuthTokenUtils.decodeBasicAuthToken(token);
        } catch (IllegalArgumentException e) {
          continue;
        }
        int separatorIndex = decodedToken != null ? decodedToken.indexOf(':') : -1;
        if (separatorIndex <= 0 || separatorIndex == decodedToken.length() - 1) {
          continue;
        }
        String username = decodedToken.substring(0, separatorIndex);
        String password = decodedToken.substring(separatorIndex + 1);
        name2password.put(username, password);
      }
      if (name2password.isEmpty()) {
        return Optional.empty();
      }

      if (_userCache == null) {
        initUserCache();
      }
      Map<String, ZkBasicAuthPrincipal> name2principal =
          BasicAuthPrincipalUtils.extractBasicAuthPrincipals(_userCache.getAllServerUserConfig()).stream()
              .collect(Collectors.toMap(ZkBasicAuthPrincipal::getName, p -> p));

      for (Map.Entry<String, String> entry : name2password.entrySet()) {
        ZkBasicAuthPrincipal principal = name2principal.get(entry.getKey());
        if (principal != null && BcryptUtils.checkpwWithCache(entry.getValue(), principal.getPassword(),
            _userCache.getUserPasswordAuthCache())) {
          return Optional.of(principal);
        }
      }
      return Optional.empty();
    }

    private Collection<String> getTokens(RequesterIdentity requesterIdentity) {
      Collection<String> tokens;
      if (requesterIdentity instanceof GrpcRequesterIdentity) {
        GrpcRequesterIdentity identity = (GrpcRequesterIdentity) requesterIdentity;
        tokens = identity.getGrpcMetadata().get(AUTHORIZATION_KEY);
      } else if (requesterIdentity instanceof HttpRequesterIdentity) {
        HttpRequesterIdentity identity = (HttpRequesterIdentity) requesterIdentity;
        tokens = identity.getHeaderValues(AUTHORIZATION_KEY);
      } else {
        return List.of();
      }
      return tokens != null ? tokens : List.of();
    }
  }
}
