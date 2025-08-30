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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Unit tests for {@link AuditUrlPathFilter}.
 * Tests URL path filtering using PathMatcher with glob patterns.
 */
public class AuditUrlPathFilterTest {

  private AuditUrlPathFilter _filter;

  @BeforeMethod
  public void setUp() {
    _filter = new AuditUrlPathFilter();
  }

  // ===== Basic Input Validation Tests =====

  @Test
  public void testNullUrlPath() {
    assertThat(_filter.isExcluded(null, "health")).isFalse();
  }

  @Test
  public void testEmptyUrlPath() {
    assertThat(_filter.isExcluded("", "health")).isFalse();
    assertThat(_filter.isExcluded("   ", "health")).isFalse();
  }

  @Test
  public void testNullExcludePatterns() {
    assertThat(_filter.isExcluded("api/users", null)).isFalse();
  }

  @Test
  public void testEmptyExcludePatterns() {
    assertThat(_filter.isExcluded("api/users", "")).isFalse();
    assertThat(_filter.isExcluded("api/users", "   ")).isFalse();
  }

  @Test
  public void testBothParametersBlank() {
    assertThat(_filter.isExcluded(null, null)).isFalse();
    assertThat(_filter.isExcluded("", "")).isFalse();
  }

  // ===== Exact Match Pattern Tests =====

  @Test
  public void testExactMatchSingleSegment() {
    assertThat(_filter.isExcluded("health", "health")).isTrue();
    assertThat(_filter.isExcluded("metrics", "metrics")).isTrue();
  }

  @Test
  public void testExactMatchMultipleSegments() {
    assertThat(_filter.isExcluded("api/v1/users", "api/v1/users")).isTrue();
    assertThat(_filter.isExcluded("admin/config/settings", "admin/config/settings")).isTrue();
  }

  @Test
  public void testExactMatchCaseSensitive() {
    assertThat(_filter.isExcluded("health", "Health")).isFalse();
    assertThat(_filter.isExcluded("API/users", "api/users")).isFalse();
  }

  @Test
  public void testExactMatchNoMatch() {
    assertThat(_filter.isExcluded("healthcheck", "health")).isFalse();
    assertThat(_filter.isExcluded("api/v2/users", "api/v1/users")).isFalse();
  }

  // ===== Single Wildcard (*) Pattern Tests =====

  @Test
  public void testSingleWildcardWithinSegment() {
    // Pattern should match direct children only
    assertThat(_filter.isExcluded("api/users", "api/*")).isTrue();
    assertThat(_filter.isExcluded("api/groups", "api/*")).isTrue();

    // Should not match nested paths
    assertThat(_filter.isExcluded("api/v1/users", "api/*")).isFalse();
    assertThat(_filter.isExcluded("api", "api/*")).isFalse();
  }

  @Test
  public void testSingleWildcardPrefix() {
    assertThat(_filter.isExcluded("healthcheck", "*check")).isTrue();
    assertThat(_filter.isExcluded("admincheck", "*check")).isTrue();
    assertThat(_filter.isExcluded("check", "*check")).isTrue();
    assertThat(_filter.isExcluded("checking", "*check")).isFalse();
  }

  @Test
  public void testSingleWildcardSuffix() {
    assertThat(_filter.isExcluded("health", "health*")).isTrue();
    assertThat(_filter.isExcluded("healthcheck", "health*")).isTrue();
    assertThat(_filter.isExcluded("healthy", "health*")).isTrue();
    assertThat(_filter.isExcluded("heal", "health*")).isFalse();
  }

  @Test
  public void testSingleWildcardMiddle() {
    assertThat(_filter.isExcluded("health", "h*th")).isTrue();
    assertThat(_filter.isExcluded("hearth", "h*th")).isTrue();
    assertThat(_filter.isExcluded("hth", "h*th")).isTrue();
    assertThat(_filter.isExcluded("heat", "h*th")).isFalse();
  }

  // ===== Double Wildcard (**) Pattern Tests =====

  @Test
  public void testDoubleWildcardAcrossSegments() {
    String pattern = "api/**";
    assertThat(_filter.isExcluded("api/users", pattern)).isTrue();
    assertThat(_filter.isExcluded("api/v1/users", pattern)).isTrue();
    assertThat(_filter.isExcluded("api/v1/v2/users", pattern)).isTrue();
    assertThat(_filter.isExcluded("api/deeply/nested/path/users", pattern)).isTrue();

    // Should not match if not under api
    assertThat(_filter.isExcluded("admin/users", pattern)).isFalse();
    assertThat(_filter.isExcluded("api", pattern)).isFalse(); // api itself doesn't match api/**
  }

  @Test
  public void testDoubleWildcardInMiddle() {
    String pattern = "api/**/users";
    // ** requires at least one directory, so api/users won't match
    assertThat(_filter.isExcluded("api/users", pattern)).isFalse();
    assertThat(_filter.isExcluded("api/v1/users", pattern)).isTrue();
    assertThat(_filter.isExcluded("api/v1/v2/users", pattern)).isTrue();

    // Should not match if doesn't end with users
    assertThat(_filter.isExcluded("api/v1/groups", pattern)).isFalse();
    assertThat(_filter.isExcluded("api/v1/users/123", pattern)).isFalse();
  }

  @Test
  public void testDoubleWildcardAtStart() {
    String pattern = "**/users";
    // ** requires at least one directory, so "users" alone won't match
    assertThat(_filter.isExcluded("users", pattern)).isFalse();
    assertThat(_filter.isExcluded("api/users", pattern)).isTrue();
    assertThat(_filter.isExcluded("api/v1/users", pattern)).isTrue();
    assertThat(_filter.isExcluded("deeply/nested/path/users", pattern)).isTrue();

    assertThat(_filter.isExcluded("users/123", pattern)).isFalse();
  }

  // ===== Single Character (?) Pattern Tests =====

  @Test
  public void testSingleCharacterMatch() {
    assertThat(_filter.isExcluded("api/v1", "api/v?")).isTrue();
    assertThat(_filter.isExcluded("api/v2", "api/v?")).isTrue();
    assertThat(_filter.isExcluded("api/vX", "api/v?")).isTrue();

    assertThat(_filter.isExcluded("api/v10", "api/v?")).isFalse(); // Two characters
    assertThat(_filter.isExcluded("api/v", "api/v?")).isFalse(); // No character
  }

  @Test
  public void testMultipleSingleCharacters() {
    assertThat(_filter.isExcluded("api/123", "api/???")).isTrue();
    assertThat(_filter.isExcluded("api/abc", "api/???")).isTrue();

    assertThat(_filter.isExcluded("api/12", "api/???")).isFalse();
    assertThat(_filter.isExcluded("api/1234", "api/???")).isFalse();
  }

  // ===== Character Set Pattern Tests =====

  @Test
  public void testCharacterSetMatch() {
    String pattern = "api/v[123]/users";
    assertThat(_filter.isExcluded("api/v1/users", pattern)).isTrue();
    assertThat(_filter.isExcluded("api/v2/users", pattern)).isTrue();
    assertThat(_filter.isExcluded("api/v3/users", pattern)).isTrue();

    assertThat(_filter.isExcluded("api/v4/users", pattern)).isFalse();
    assertThat(_filter.isExcluded("api/v0/users", pattern)).isFalse();
  }

  @Test
  public void testCharacterRangeMatch() {
    String pattern = "api/v[0-9]/users";
    assertThat(_filter.isExcluded("api/v0/users", pattern)).isTrue();
    assertThat(_filter.isExcluded("api/v5/users", pattern)).isTrue();
    assertThat(_filter.isExcluded("api/v9/users", pattern)).isTrue();

    assertThat(_filter.isExcluded("api/va/users", pattern)).isFalse();
    assertThat(_filter.isExcluded("api/v10/users", pattern)).isFalse(); // Two digits
  }

  @Test
  public void testNegatedCharacterSet() {
    String pattern = "api/v[!0]/users";
    assertThat(_filter.isExcluded("api/v1/users", pattern)).isTrue();
    assertThat(_filter.isExcluded("api/v2/users", pattern)).isTrue();
    assertThat(_filter.isExcluded("api/va/users", pattern)).isTrue();

    assertThat(_filter.isExcluded("api/v0/users", pattern)).isFalse();
  }

  // ===== Grouping Pattern Tests =====
  // Note: Grouping patterns with commas like {users,groups} are not supported
  // because comma is used as the delimiter for multiple patterns

  // ===== Multiple Pattern Tests =====

  @Test
  public void testMultiplePatternsCommaSeparated() {
    String patterns = "health,api/*,admin/**";

    // First pattern
    assertThat(_filter.isExcluded("health", patterns)).isTrue();

    // Second pattern
    assertThat(_filter.isExcluded("api/users", patterns)).isTrue();
    assertThat(_filter.isExcluded("api/v1/users", patterns)).isFalse(); // api/* doesn't match nested

    // Third pattern
    assertThat(_filter.isExcluded("admin/config", patterns)).isTrue();
    assertThat(_filter.isExcluded("admin/config/settings", patterns)).isTrue();

    // None match
    assertThat(_filter.isExcluded("metrics", patterns)).isFalse();
  }

  @Test
  public void testMultiplePatternsWithSpaces() {
    String patterns = " health , api/* , admin/** ";

    assertThat(_filter.isExcluded("health", patterns)).isTrue();
    assertThat(_filter.isExcluded("api/users", patterns)).isTrue();
    assertThat(_filter.isExcluded("admin/config/settings", patterns)).isTrue();
  }

  @Test
  public void testMultiplePatternsEmptyElements() {
    String patterns = "health,,api/*,,";

    assertThat(_filter.isExcluded("health", patterns)).isTrue();
    assertThat(_filter.isExcluded("api/users", patterns)).isTrue();
    assertThat(_filter.isExcluded("metrics", patterns)).isFalse();
  }

  // ===== Regex Pattern Tests =====

  @Test
  public void testExplicitRegexPattern() {
    String pattern = "regex:api/v[0-9]+/.*";
    assertThat(_filter.isExcluded("api/v1/users", pattern)).isTrue();
    assertThat(_filter.isExcluded("api/v123/anything", pattern)).isTrue();

    assertThat(_filter.isExcluded("api/va/users", pattern)).isFalse();
    assertThat(_filter.isExcluded("api/v/users", pattern)).isFalse();
  }

  @Test
  public void testMixedGlobAndRegex() {
    String patterns = "glob:health,regex:api/v[0-9]+/.*,admin/**";

    assertThat(_filter.isExcluded("health", patterns)).isTrue();
    assertThat(_filter.isExcluded("api/v1/users", patterns)).isTrue();
    assertThat(_filter.isExcluded("admin/config", patterns)).isTrue();
  }

  // ===== Edge Cases and Error Handling =====

  @Test
  public void testInvalidGlobPattern() {
    // Unclosed bracket should be skipped
    String pattern = "api/v[123/users";
    assertThat(_filter.isExcluded("api/v1/users", pattern)).isFalse();

    // Multiple invalid patterns with one valid
    String patterns = "api/v[123,health,{unclosed";
    assertThat(_filter.isExcluded("health", patterns)).isTrue();
    assertThat(_filter.isExcluded("api/v1", patterns)).isFalse();
  }

  @Test
  public void testSpecialCharactersInPath() {
    assertThat(_filter.isExcluded("api/user@example.com", "api/*")).isTrue();
    assertThat(_filter.isExcluded("api/user+test", "api/*")).isTrue();
    assertThat(_filter.isExcluded("api/user%20name", "api/*")).isTrue();
  }

  @Test
  public void testPathsWithoutLeadingSlash() {
    // Jersey paths don't have leading slashes
    assertThat(_filter.isExcluded("api/users", "api/*")).isTrue();
    assertThat(_filter.isExcluded("health", "health")).isTrue();

    // Pattern with leading slash won't match
    assertThat(_filter.isExcluded("api/users", "/api/*")).isFalse();
  }

  // ===== Real-World Scenario Tests =====

  @Test
  public void testHealthEndpointExclusion() {
    String patterns = "health,healthcheck,ping,ready,live";

    assertThat(_filter.isExcluded("health", patterns)).isTrue();
    assertThat(_filter.isExcluded("healthcheck", patterns)).isTrue();
    assertThat(_filter.isExcluded("ping", patterns)).isTrue();
    assertThat(_filter.isExcluded("ready", patterns)).isTrue();
    assertThat(_filter.isExcluded("live", patterns)).isTrue();

    assertThat(_filter.isExcluded("api/health", patterns)).isFalse();
  }

  @Test
  public void testAPIVersioningExclusion() {
    // Exclude all v1 endpoints
    String pattern = "api/v1/**";

    assertThat(_filter.isExcluded("api/v1/users", pattern)).isTrue();
    assertThat(_filter.isExcluded("api/v1/users/123", pattern)).isTrue();
    assertThat(_filter.isExcluded("api/v1/groups/456/members", pattern)).isTrue();

    assertThat(_filter.isExcluded("api/v2/users", pattern)).isFalse();
  }

  @Test
  public void testAdminEndpointExclusion() {
    String pattern = "admin/**";

    assertThat(_filter.isExcluded("admin/config", pattern)).isTrue();
    assertThat(_filter.isExcluded("admin/users/list", pattern)).isTrue();
    assertThat(_filter.isExcluded("admin/system/restart", pattern)).isTrue();

    assertThat(_filter.isExcluded("api/admin", pattern)).isFalse();
  }

  @Test
  public void testStaticResourceExclusion() {
    String patterns = "static/**,assets/**,*.css,*.js,*.png,*.jpg";

    assertThat(_filter.isExcluded("static/css/main.css", patterns)).isTrue();
    assertThat(_filter.isExcluded("assets/images/logo.png", patterns)).isTrue();
    assertThat(_filter.isExcluded("script.js", patterns)).isTrue();
    assertThat(_filter.isExcluded("styles.css", patterns)).isTrue();
    assertThat(_filter.isExcluded("image.png", patterns)).isTrue();

    assertThat(_filter.isExcluded("api/data.json", patterns)).isFalse();
  }

  @Test
  public void testComplexRealWorldPatterns() {
    // Exclude health checks, static resources, and specific API versions
    String patterns = "health*,ping,static/**,assets/**,api/v[0-2]/**,admin/users/*,admin/groups/*";

    // Health checks
    assertThat(_filter.isExcluded("health", patterns)).isTrue();
    assertThat(_filter.isExcluded("healthcheck", patterns)).isTrue();
    assertThat(_filter.isExcluded("ping", patterns)).isTrue();

    // Static resources
    assertThat(_filter.isExcluded("static/js/app.js", patterns)).isTrue();
    assertThat(_filter.isExcluded("assets/css/main.css", patterns)).isTrue();

    // API versions
    assertThat(_filter.isExcluded("api/v0/users", patterns)).isTrue();
    assertThat(_filter.isExcluded("api/v1/users/123", patterns)).isTrue();
    assertThat(_filter.isExcluded("api/v2/groups", patterns)).isTrue();
    assertThat(_filter.isExcluded("api/v3/users", patterns)).isFalse();

    // Admin endpoints
    assertThat(_filter.isExcluded("admin/users/list", patterns)).isTrue();
    assertThat(_filter.isExcluded("admin/groups/create", patterns)).isTrue();
    assertThat(_filter.isExcluded("admin/settings/update", patterns)).isFalse();
  }

  // ===== Performance Edge Cases =====

  @Test
  public void testManyPatternsPerformance() {
    // Create a pattern string with 100 patterns
    StringBuilder patterns = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      if (i > 0) {
        patterns.append(",");
      }
      patterns.append("endpoint").append(i);
    }

    // Should match some patterns
    assertThat(_filter.isExcluded("endpoint50", patterns.toString())).isTrue();
    assertThat(_filter.isExcluded("endpoint99", patterns.toString())).isTrue();

    // Should not match
    assertThat(_filter.isExcluded("endpoint100", patterns.toString())).isFalse();
    assertThat(_filter.isExcluded("other", patterns.toString())).isFalse();
  }

  @Test
  public void testComplexPatternPerformance() {
    // Very complex pattern with multiple features
    String patterns = "api/**/users/create/*,api/**/users/update/*,api/**/users/delete/*,"
        + "admin/**/settings/[a-z]*/config/**";

    assertThat(_filter.isExcluded("api/v1/v2/v3/users/create/batch", patterns)).isTrue();
    assertThat(_filter.isExcluded("admin/system/core/settings/app/config/database/connection", patterns)).isTrue();
  }
}
