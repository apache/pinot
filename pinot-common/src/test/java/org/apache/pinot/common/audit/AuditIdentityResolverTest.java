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

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.spi.audit.AuditTokenResolver;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;


public class AuditIdentityResolverTest {

  @Mock
  private AuditConfigManager _auditConfigManager;

  @Mock
  private ContainerRequestContext _requestContext;

  private AuditIdentityResolver _auditIdentityResolver;
  private AuditConfig _auditConfig;

  @BeforeMethod
  public void setUp()
      throws Exception {
    MockitoAnnotations.openMocks(this);
    MockAuditTokenResolver.reset();
    _auditIdentityResolver = new AuditIdentityResolver(_auditConfigManager);

    _auditConfig = new AuditConfig();
    when(_auditConfigManager.getCurrentConfig()).thenReturn(_auditConfig);
  }

  // ==================== Custom Header Tests ====================

  @Test
  public void testResolveIdentityFromCustomHeader() {
    _auditConfig.setUseridHeader("X-User-Email");
    when(_requestContext.getHeaderString("X-User-Email")).thenReturn("user@example.com");

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("user@example.com");
  }

  // ==================== JWT Tests ====================

  @Test
  public void testResolveIdentityFromJwtCustomClaim()
      throws Exception {
    _auditConfig.setUseridJwtClaimName("email");
    String validJwt = createJwtToken("user123", Map.of("email", "jwt@example.com"));
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + validJwt);

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("jwt@example.com");
  }

  @Test
  public void testResolveIdentityFromJwtSubjectWhenClaimMissing()
      throws Exception {
    _auditConfig.setUseridJwtClaimName("email");
    String validJwt = createJwtToken("user123", new HashMap<>());
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + validJwt);

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("user123");
  }

  @Test
  public void testInvalidJwtTokenReturnsNull() {
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer invalid-token");

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNull();
  }

  // ==================== Custom Resolver Tests ====================

  @Test
  public void testCustomTokenResolverSuccess() {
    AuditTokenResolver mockResolver = new MockAuditTokenResolver("resolved-user");
    AuditIdentityResolver resolver = new AuditIdentityResolver(_auditConfigManager, mockResolver);
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer custom-token");

    AuditEvent.UserIdentity result = resolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("resolved-user");
  }

  @Test
  public void testCustomResolverReturnsNullFallsBackToJwt()
      throws Exception {
    AuditTokenResolver mockResolver = new MockAuditTokenResolver(null);
    AuditIdentityResolver resolver = new AuditIdentityResolver(_auditConfigManager, mockResolver);
    String validJwt = createJwtToken("jwt-user", new HashMap<>());
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + validJwt);

    AuditEvent.UserIdentity result = resolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("jwt-user");
  }

  @Test
  public void testCustomResolverReceivesFullAuthHeader() {
    String expectedAuthHeader = "Bearer custom-token-value";
    MockAuditTokenResolver mockResolver = new MockAuditTokenResolver("user");
    AuditIdentityResolver resolver = new AuditIdentityResolver(_auditConfigManager, mockResolver);
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn(expectedAuthHeader);

    resolver.resolveIdentity(_requestContext);

    assertThat(MockAuditTokenResolver.getLastAuthHeader()).isEqualTo(expectedAuthHeader);
  }

  // ==================== PluginManager Tests ====================

  @Test
  public void testResolverLoadedViaPluginManager() {
    _auditConfig.setTokenResolverClass(MockAuditTokenResolver.class.getName());
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer test-token");

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("mock-resolved-user");
  }

  @Test
  public void testInvalidResolverClassFallsBackToJwt()
      throws Exception {
    _auditConfig.setTokenResolverClass("com.invalid.NonExistentResolver");
    String validJwt = createJwtToken("jwt-user", new HashMap<>());
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + validJwt);

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("jwt-user");
  }

  // ==================== Priority Tests ====================

  @Test
  public void testPriorityHeaderOverJwt()
      throws Exception {
    _auditConfig.setUseridHeader("X-User-Email");
    when(_requestContext.getHeaderString("X-User-Email")).thenReturn("header-user");
    String validJwt = createJwtToken("jwt-user", new HashMap<>());
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + validJwt);

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("header-user");
  }

  @Test
  public void testPriorityHeaderOverResolver() {
    AuditTokenResolver mockResolver = new MockAuditTokenResolver("resolver-user");
    AuditIdentityResolver resolver = new AuditIdentityResolver(_auditConfigManager, mockResolver);
    _auditConfig.setUseridHeader("X-User-Email");
    when(_requestContext.getHeaderString("X-User-Email")).thenReturn("header-user");
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer token");

    AuditEvent.UserIdentity result = resolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("header-user");
  }

  // ==================== Edge Cases ====================

  @Test
  public void testNoAuthenticationReturnsNull() {
    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNull();
  }

  @Test
  public void testNonBearerAuthorizationReturnsNull() {
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Basic dXNlcjpwYXNz");

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNull();
  }

  // ==================== Helper Methods ====================

  private String createJwtToken(String subject, Map<String, Object> customClaims)
      throws Exception {
    JWTClaimsSet.Builder claimsBuilder = new JWTClaimsSet.Builder().subject(subject)
        .issueTime(new Date())
        .expirationTime(new Date(System.currentTimeMillis() + 300000));

    for (Map.Entry<String, Object> entry : customClaims.entrySet()) {
      claimsBuilder.claim(entry.getKey(), entry.getValue());
    }

    JWTClaimsSet claims = claimsBuilder.build();
    SignedJWT signedJWT = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);

    JWSSigner signer = new MACSigner("test-secret-key-that-is-long-enough");
    signedJWT.sign(signer);

    return signedJWT.serialize();
  }
}
