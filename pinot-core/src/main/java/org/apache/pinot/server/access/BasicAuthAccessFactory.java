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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.ws.rs.NotAuthorizedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.auth.BasicAuthTokenUtils;
import org.apache.pinot.core.auth.BasicAuthPrincipal;
import org.apache.pinot.core.auth.BasicAuthPrincipalUtils;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.apache.pinot.spi.auth.BasicAuthorizationResultImpl;
import org.apache.pinot.spi.auth.server.RequesterIdentity;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class BasicAuthAccessFactory implements AccessControlFactory {
  private static final String PREFIX = "principals";

  private static final String AUTHORIZATION_KEY = "authorization";
  private static final String ADMIN_PERMISSION = "admin";

  private AccessControl _accessControl;

  @Override
  public void init(PinotConfiguration configuration) {
    _accessControl = new BasicAuthAccessControl(
        BasicAuthPrincipalUtils.extractBasicAuthPrincipals(configuration, PREFIX));
  }

  public AccessControl create() {
    return _accessControl;
  }

  /// Access Control using metadata-based basic grpc authentication
  private static class BasicAuthAccessControl implements AccessControl {
    private final Map<String, BasicAuthPrincipal> _token2principal;

    public BasicAuthAccessControl(Collection<BasicAuthPrincipal> principals) {
      _token2principal = principals.stream().collect(Collectors.toMap(BasicAuthPrincipal::getToken, p -> p));
    }

    @Override
    public boolean isAuthorizedChannel(ChannelHandlerContext channelHandlerContext) {
      return true;
    }

    @Override
    public AuthorizationResult authorizeAdminAccess(RequesterIdentity requesterIdentity) {
      Optional<BasicAuthPrincipal> principal = getPrincipal(requesterIdentity);
      if (!principal.isPresent()) {
        throw new NotAuthorizedException("Basic");
      }
      return new BasicAuthorizationResultImpl(principal.get().hasExplicitPermission(ADMIN_PERMISSION));
    }

    @Override
    public boolean hasDataAccess(RequesterIdentity requesterIdentity, String tableName) {
      return getPrincipal(requesterIdentity)
          .map(principal -> StringUtils.isEmpty(tableName) || principal.hasTable(
              TableNameBuilder.extractRawTableName(tableName)))
          .orElse(false);
    }

    private Optional<BasicAuthPrincipal> getPrincipal(RequesterIdentity requesterIdentity) {
      for (String token : getTokens(requesterIdentity)) {
        String normalizedToken;
        try {
          normalizedToken = BasicAuthTokenUtils.normalizeBase64Token(token);
        } catch (IllegalArgumentException e) {
          continue;
        }
        BasicAuthPrincipal principal = _token2principal.get(normalizedToken);
        if (principal != null) {
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
