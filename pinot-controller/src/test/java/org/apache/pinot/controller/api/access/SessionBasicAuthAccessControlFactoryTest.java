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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


/**
 * Unit tests for {@link SessionBasicAuthAccessControlFactory}.
 *
 * <p>Covers:
 * <ul>
 *   <li>{@code init()} and {@code create()} lifecycle</li>
 *   <li>{@code protectAnnotatedOnly()} → false</li>
 *   <li>{@code getAuthWorkflowInfo()} → SESSION workflow</li>
 *   <li>{@code hasAccess(AccessType, HttpHeaders, String)} — valid/invalid/null credentials</li>
 *   <li>{@code hasAccess(String, AccessType, HttpHeaders, String)} — table-level authorization</li>
 *   <li>{@code hasAccess(HttpHeaders, TargetType)} — identity check</li>
 * </ul>
 */
public class SessionBasicAuthAccessControlFactoryTest {

  // admin:verysecret → Base64 "YWRtaW46dmVyeXNlY3JldA==" → normalized (no =) → "YWRtaW46dmVyeXNlY3JldA"
  private static final String TOKEN_ADMIN = "Basic YWRtaW46dmVyeXNlY3JldA";
  // user:secret → Base64 "dXNlcjpzZWNyZXQ=" → normalized → "dXNlcjpzZWNyZXQ"
  private static final String TOKEN_USER = "Basic dXNlcjpzZWNyZXQ";
  private static final String TOKEN_INVALID = "Basic aW52YWxpZA"; // "invalid" in base64

  private static final String ALLOWED_TABLE = "tableA";
  private static final String DISALLOWED_TABLE = "tableC";

  private AccessControl _accessControl;

  @BeforeClass
  public void setUp() {
    Map<String, Object> config = new HashMap<>();
    config.put("controller.admin.access.control.principals", "admin,user");
    config.put("controller.admin.access.control.principals.admin.password", "verysecret");
    config.put("controller.admin.access.control.principals.user.password", "secret");
    // user is restricted to tableA and tableB with read permission only
    config.put("controller.admin.access.control.principals.user.tables", "tableA,tableB");
    config.put("controller.admin.access.control.principals.user.permissions", "read");

    SessionBasicAuthAccessControlFactory factory = new SessionBasicAuthAccessControlFactory();
    factory.init(new PinotConfiguration(config));
    _accessControl = factory.create();
  }

  // ---------------------------------------------------------------------------
  // Factory lifecycle
  // ---------------------------------------------------------------------------

  @Test
  public void testCreateReturnsNonNull() {
    assertNotNull(_accessControl);
  }

  @Test
  public void testProtectAnnotatedOnlyReturnsFalse() {
    assertFalse(_accessControl.protectAnnotatedOnly());
  }

  // ---------------------------------------------------------------------------
  // Workflow
  // ---------------------------------------------------------------------------

  @Test
  public void testGetAuthWorkflowInfoReturnsSessionWorkflow() {
    AccessControl.AuthWorkflowInfo info = _accessControl.getAuthWorkflowInfo();
    assertNotNull(info);
    assertEquals(info.getWorkflow(), AccessControl.WORKFLOW_SESSION);
  }

  // ---------------------------------------------------------------------------
  // hasAccess(AccessType, HttpHeaders, String) — identity-only check
  // ---------------------------------------------------------------------------

  @Test
  public void testHasAccessIdentityOnlyValidAdminReturnsTrue() {
    HttpHeaders headers = mockHeaders(TOKEN_ADMIN);
    assertTrue(_accessControl.hasAccess(AccessType.READ, headers, "/api/tables"));
  }

  @Test
  public void testHasAccessIdentityOnlyValidUserReturnsTrue() {
    HttpHeaders headers = mockHeaders(TOKEN_USER);
    assertTrue(_accessControl.hasAccess(AccessType.READ, headers, "/api/tables"));
  }

  @Test(expectedExceptions = NotAuthorizedException.class)
  public void testHasAccessIdentityOnlyInvalidTokenThrowsNotAuthorized() {
    HttpHeaders headers = mockHeaders(TOKEN_INVALID);
    _accessControl.hasAccess(AccessType.READ, headers, "/api/tables");
  }

  @Test(expectedExceptions = NotAuthorizedException.class)
  public void testHasAccessIdentityOnlyNullHeadersThrowsNotAuthorized() {
    _accessControl.hasAccess(AccessType.READ, null, "/api/tables");
  }

  @Test(expectedExceptions = NotAuthorizedException.class)
  public void testHasAccessIdentityOnlyNoAuthHeaderThrowsNotAuthorized() {
    HttpHeaders headers = mockHeaders(null);
    _accessControl.hasAccess(AccessType.READ, headers, "/api/tables");
  }

  // ---------------------------------------------------------------------------
  // hasAccess(String, AccessType, HttpHeaders, String) — table-level check
  // ---------------------------------------------------------------------------

  @Test
  public void testHasAccessTableAdminAnyTableReturnsTrue() {
    HttpHeaders headers = mockHeaders(TOKEN_ADMIN);
    // Admin has no table restriction — all tables allowed
    assertTrue(_accessControl.hasAccess(ALLOWED_TABLE, AccessType.READ, headers, "/api/tables"));
    assertTrue(_accessControl.hasAccess(DISALLOWED_TABLE, AccessType.READ, headers, "/api/tables"));
    assertTrue(_accessControl.hasAccess("anyRandomTable", AccessType.DELETE, headers, "/api/tables"));
  }

  @Test
  public void testHasAccessTableUserAllowedTableReturnsTrue() {
    HttpHeaders headers = mockHeaders(TOKEN_USER);
    assertTrue(_accessControl.hasAccess(ALLOWED_TABLE, AccessType.READ, headers, "/api/tables"));
  }

  @Test
  public void testHasAccessTableUserSecondAllowedTableReturnsTrue() {
    HttpHeaders headers = mockHeaders(TOKEN_USER);
    assertTrue(_accessControl.hasAccess("tableB", AccessType.READ, headers, "/api/tables"));
  }

  @Test
  public void testHasAccessTableUserDisallowedTableReturnsFalse() {
    HttpHeaders headers = mockHeaders(TOKEN_USER);
    assertFalse(_accessControl.hasAccess(DISALLOWED_TABLE, AccessType.READ, headers, "/api/tables"));
  }

  @Test
  public void testHasAccessTableUserDisallowedPermissionReturnsFalse() {
    HttpHeaders headers = mockHeaders(TOKEN_USER);
    // user has read-only permission; CREATE should be denied
    assertFalse(_accessControl.hasAccess(ALLOWED_TABLE, AccessType.CREATE, headers, "/api/tables"));
  }

  @Test
  public void testHasAccessTableInvalidTokenReturnsFalse() {
    HttpHeaders headers = mockHeaders(TOKEN_INVALID);
    assertFalse(_accessControl.hasAccess(ALLOWED_TABLE, AccessType.READ, headers, "/api/tables"));
  }

  // ---------------------------------------------------------------------------
  // hasAccess(HttpHeaders, TargetType) — identity check via TargetType
  // ---------------------------------------------------------------------------

  @Test
  public void testHasAccessTargetTypeValidAdminReturnsTrue() {
    HttpHeaders headers = mockHeaders(TOKEN_ADMIN);
    assertTrue(_accessControl.hasAccess(headers, TargetType.TABLE));
  }

  @Test
  public void testHasAccessTargetTypeValidUserReturnsTrue() {
    HttpHeaders headers = mockHeaders(TOKEN_USER);
    assertTrue(_accessControl.hasAccess(headers, TargetType.TABLE));
  }

  @Test
  public void testHasAccessTargetTypeInvalidTokenReturnsFalse() {
    HttpHeaders headers = mockHeaders(TOKEN_INVALID);
    assertFalse(_accessControl.hasAccess(headers, TargetType.TABLE));
  }

  @Test
  public void testHasAccessTargetTypeNullHeadersReturnsFalse() {
    assertFalse(_accessControl.hasAccess((HttpHeaders) null, TargetType.TABLE));
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static HttpHeaders mockHeaders(String authToken) {
    HttpHeaders headers = mock(HttpHeaders.class);
    List<String> authValues = authToken != null ? List.of(authToken) : List.of();
    when(headers.getRequestHeader("Authorization")).thenReturn(authValues);
    return headers;
  }
}
