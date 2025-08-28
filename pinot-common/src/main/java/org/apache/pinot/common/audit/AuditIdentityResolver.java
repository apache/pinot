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
package org.apache.pinot.common.audit;

import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class AuditIdentityResolver {

  private static final Logger LOG = LoggerFactory.getLogger(AuditIdentityResolver.class);
  private static final String BEARER_PREFIX = "Bearer ";

  private final AuditConfigManager _configManager;

  @Inject
  public AuditIdentityResolver(AuditConfigManager configManager) {
    _configManager = configManager;
  }

  public AuditEvent.UserIdentity resolveIdentity(ContainerRequestContext requestContext) {
    AuditConfig config = _configManager.getCurrentConfig();

    // Priority 1: Check custom identity header
    String identityHeader = config.getUseridHeader();
    if (StringUtils.isNotBlank(identityHeader)) {
      String principal = requestContext.getHeaderString(identityHeader);
      if (StringUtils.isNotBlank(principal)) {
        return new AuditEvent.UserIdentity().setPrincipal(principal);
      }
    }

    // Priority 2: Check JWT in Authorization header
    String authHeader = requestContext.getHeaderString(HttpHeaders.AUTHORIZATION);
    if (StringUtils.isNotBlank(authHeader) && authHeader.startsWith(BEARER_PREFIX)) {
      String token = authHeader.substring(BEARER_PREFIX.length()).trim();
      String principal = extractJwtPrincipal(token, config.getUseridJwtClaimName());
      if (principal != null) {
        return new AuditEvent.UserIdentity().setPrincipal(principal);
      }
    }

    // Return null instead of anonymous
    return null;
  }

  private String extractJwtPrincipal(String token, String claimName) {
    try {
      JWT jwt = JWTParser.parse(token);
      JWTClaimsSet claims = jwt.getJWTClaimsSet();

      // Try configured claim first
      if (StringUtils.isNotBlank(claimName)) {
        Object claimValue = claims.getClaim(claimName);
        if (claimValue != null) {
          return claimValue.toString();
        }
      }

      // Fallback to subject
      return claims.getSubject();
    } catch (Exception e) {
      LOG.error("Failed to parse JWT token", e);
      return null;
    }
  }
}
