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
import java.lang.reflect.Field;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.HttpHeaders;
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
    _auditIdentityResolver = new AuditIdentityResolver();

    Field field = AuditIdentityResolver.class.getDeclaredField("_configManager");
    field.setAccessible(true);
    field.set(_auditIdentityResolver, _auditConfigManager);

    _auditConfig = new AuditConfig();
    when(_auditConfigManager.getCurrentConfig()).thenReturn(_auditConfig);
  }

  @Test
  public void testResolveIdentityFromCustomHeaderSuccess() {
    _auditConfig.setUseridHeader("X-User-Email");
    when(_requestContext.getHeaderString("X-User-Email")).thenReturn("user@example.com");

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("user@example.com");
  }

  @Test
  public void testResolveIdentityFromCustomHeaderEmptyHeaderValue()
      throws Exception {
    _auditConfig.setUseridHeader("X-User-Email");
    _auditConfig.setUseridJwtClaimName("email");

    when(_requestContext.getHeaderString("X-User-Email")).thenReturn("");
    String validJwt = createJwtToken("user123", Map.of("email", "jwt@example.com"));
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + validJwt);

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("jwt@example.com");
  }

  @Test
  public void testResolveIdentityFromCustomHeaderMissingHeader()
      throws Exception {
    _auditConfig.setUseridHeader("X-User-Email");
    _auditConfig.setUseridJwtClaimName("email");

    when(_requestContext.getHeaderString("X-User-Email")).thenReturn(null);
    String validJwt = createJwtToken("user123", Map.of("email", "jwt@example.com"));
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + validJwt);

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("jwt@example.com");
  }

  @Test
  public void testResolveIdentityFromJwtCustomClaimSuccess()
      throws Exception {
    _auditConfig.setUseridJwtClaimName("email");

    String validJwt = createJwtToken("user123", Map.of("email", "jwt@example.com"));
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + validJwt);

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("jwt@example.com");
  }

  @Test
  public void testResolveIdentityFromJwtSubjectWhenCustomClaimNotConfigured()
      throws Exception {
    String validJwt = createJwtToken("user123", new HashMap<>());
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + validJwt);

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("user123");
  }

  @Test
  public void testResolveIdentityFromJwtSubjectWhenCustomClaimMissing()
      throws Exception {
    _auditConfig.setUseridJwtClaimName("email");

    String validJwt = createJwtToken("user123", new HashMap<>());
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + validJwt);

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("user123");
  }

  @Test
  public void testInvalidJwtToken() {
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer invalid-token");

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNull();
  }

  @Test
  public void testHeaderTakesPriorityOverJwt()
      throws Exception {
    _auditConfig.setUseridHeader("X-User-Email");
    _auditConfig.setUseridJwtClaimName("email");

    when(_requestContext.getHeaderString("X-User-Email")).thenReturn("header@example.com");
    String validJwt = createJwtToken("user123", Map.of("email", "jwt@example.com"));
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + validJwt);

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("header@example.com");
  }

  @Test
  public void testFallbackChainNoHeaderFallsBackToJwtClaim()
      throws Exception {
    _auditConfig.setUseridHeader("X-User-Email");
    _auditConfig.setUseridJwtClaimName("email");

    when(_requestContext.getHeaderString("X-User-Email")).thenReturn(null);
    String validJwt = createJwtToken("user123", Map.of("email", "jwt@example.com"));
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + validJwt);

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("jwt@example.com");
  }

  @Test
  public void testFallbackChainNoHeaderNoClaimFallsBackToSubject()
      throws Exception {
    _auditConfig.setUseridHeader("X-User-Email");
    _auditConfig.setUseridJwtClaimName("email");

    when(_requestContext.getHeaderString("X-User-Email")).thenReturn(null);
    String validJwt = createJwtToken("user123", new HashMap<>());
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + validJwt);

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("user123");
  }

  @Test
  public void testNoAuthenticationPresentReturnsNull() {
    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNull();
  }

  @Test
  public void testBlankConfigurationUsesDefaults()
      throws Exception {
    String validJwt = createJwtToken("user123", new HashMap<>());
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + validJwt);

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("user123");
  }

  @Test
  public void testNonBearerAuthorizationHeaderReturnsNull() {
    when(_requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)).thenReturn("Basic dXNlcjpwYXNz");

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNull();
  }

  @Test
  public void testWhitespaceInHeaderValueTrimmed() {
    _auditConfig.setUseridHeader("X-User-Email");
    when(_requestContext.getHeaderString("X-User-Email")).thenReturn("  user@example.com  ");

    AuditEvent.UserIdentity result = _auditIdentityResolver.resolveIdentity(_requestContext);

    assertThat(result).isNotNull();
    assertThat(result.getPrincipal()).isEqualTo("  user@example.com  ");
  }

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
