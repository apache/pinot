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
import static org.mockito.Mockito.mock;


/**
 * Unit tests for {@link AuditUrlPathFilter}.
 * Tests the filter's delegation to PathMatcher and its input handling logic.
 */
public class AuditUrlPathFilterTest {

  private AuditUrlPathFilter _filter;

  @BeforeMethod
  public void setUp() {
    _filter = new AuditUrlPathFilter(mock(AuditConfigManager.class));
  }

  // ===== Input Validation Tests =====

  @Test
  public void testNullUrlPath() {
    assertThat(_filter.matches(null, "health")).isFalse();
  }

  @Test
  public void testEmptyUrlPath() {
    assertThat(_filter.matches("", "health")).isFalse();
    assertThat(_filter.matches("   ", "health")).isFalse();
  }

  @Test
  public void testNullExcludePatterns() {
    assertThat(_filter.matches("api/users", null)).isFalse();
  }

  @Test
  public void testEmptyExcludePatterns() {
    assertThat(_filter.matches("api/users", "")).isFalse();
    assertThat(_filter.matches("api/users", "   ")).isFalse();
  }

  @Test
  public void testBothParametersBlank() {
    assertThat(_filter.matches(null, null)).isFalse();
    assertThat(_filter.matches("", "")).isFalse();
  }

  // ===== Multiple Pattern Processing Tests =====

  @Test
  public void testMultiplePatternsCommaSeparated() {
    String patterns = "health,api/users,admin";

    assertThat(_filter.matches("health", patterns)).isTrue();
    assertThat(_filter.matches("api/users", patterns)).isTrue();
    assertThat(_filter.matches("admin", patterns)).isTrue();
    assertThat(_filter.matches("metrics", patterns)).isFalse();
  }

  @Test
  public void testMultiplePatternsWithTrimmingAndEmptyElements() {
    String patterns = " health , , api/users , , ";

    assertThat(_filter.matches("health", patterns)).isTrue();
    assertThat(_filter.matches("api/users", patterns)).isTrue();
    assertThat(_filter.matches("metrics", patterns)).isFalse();
  }

  @Test
  public void testAnyPatternMatchesReturnsTrue() {
    String patterns = "nonexistent1,health,nonexistent2";

    assertThat(_filter.matches("health", patterns)).isTrue();
    assertThat(_filter.matches("nonexistent1", patterns)).isTrue();
    assertThat(_filter.matches("other", patterns)).isFalse();
  }

  // ===== Prefix Handling Tests =====

  @Test
  public void testAutomaticGlobPrefixAddition() {
    assertThat(_filter.matches("health", "health")).isTrue();
    assertThat(_filter.matches("api/users", "api/*")).isTrue();
  }

  @Test
  public void testExplicitGlobPrefix() {
    assertThat(_filter.matches("health", "glob:health")).isTrue();
    assertThat(_filter.matches("api/users", "glob:api/*")).isTrue();
  }

  @Test
  public void testExplicitRegexPrefix() {
    String pattern = "regex:api/v[0-9]+/.*";
    assertThat(_filter.matches("api/v1/users", pattern)).isTrue();
    assertThat(_filter.matches("api/v123/anything", pattern)).isTrue();
    assertThat(_filter.matches("api/va/users", pattern)).isFalse();
  }

  @Test
  public void testMixedPrefixes() {
    String patterns = "glob:health,regex:api/v[0-9]+/.*,admin";

    assertThat(_filter.matches("health", patterns)).isTrue();
    assertThat(_filter.matches("api/v1/users", patterns)).isTrue();
    assertThat(_filter.matches("admin", patterns)).isTrue();
  }

  // ===== Error Handling Tests =====

  @Test
  public void testInvalidPatternIsSkipped() {
    String patterns = "api/v[123,health,{unclosed";

    assertThat(_filter.matches("health", patterns)).isTrue();
    assertThat(_filter.matches("api/v1", patterns)).isFalse();
  }

  @Test
  public void testInvalidPathHandling() {
    String invalidPath = "path\0with\0nulls";
    assertThat(_filter.matches(invalidPath, "health")).isFalse();
  }

  @Test
  public void testAllInvalidPatternsReturnFalse() {
    String patterns = "[unclosed,{unclosed,\\invalid";

    assertThat(_filter.matches("anything", patterns)).isFalse();
  }

  @Test
  public void testBasicIntegrationWithPathMatcher() {
    String patterns = "health,api/*,admin/**";

    assertThat(_filter.matches("health", patterns)).isTrue();
    assertThat(_filter.matches("api/users", patterns)).isTrue();
    assertThat(_filter.matches("api/v1/users", patterns)).isFalse();
    assertThat(_filter.matches("admin/config", patterns)).isTrue();
    assertThat(_filter.matches("admin/config/settings", patterns)).isTrue();
    assertThat(_filter.matches("metrics", patterns)).isFalse();
  }
}
