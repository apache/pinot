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
  private static final String ANONYMOUS_PRINCIPAL = "anonymous";

  public AuditEvent.UserIdentity resolveIdentity(ContainerRequestContext requestContext) {
    AuditEvent.UserIdentity userIdentity = new AuditEvent.UserIdentity();

    String authHeader = requestContext.getHeaderString(HttpHeaders.AUTHORIZATION);
    if (StringUtils.isBlank(authHeader)) {
      userIdentity.setPrincipal(ANONYMOUS_PRINCIPAL);
      return userIdentity;
    }

    if (authHeader.startsWith(BEARER_PREFIX)) {
      String principal = extractJwtPrincipal(authHeader.substring(BEARER_PREFIX.length()).trim());
      userIdentity.setPrincipal(principal != null ? principal : ANONYMOUS_PRINCIPAL);
      return userIdentity;
    }

    userIdentity.setPrincipal(ANONYMOUS_PRINCIPAL);
    return userIdentity;
  }

  private String extractJwtPrincipal(String token) {
    try {
      JWT jwt = JWTParser.parse(token);
      JWTClaimsSet claims = jwt.getJWTClaimsSet();
      return claims.getSubject();
    } catch (Exception e) {
      LOG.debug("Failed to parse JWT token", e);
      return null;
    }
  }
}
