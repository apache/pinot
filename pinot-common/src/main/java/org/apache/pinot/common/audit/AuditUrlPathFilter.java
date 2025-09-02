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

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Arrays;
import javax.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * URL path filter utility that uses Java's PathMatcher with glob and regex patterns
 * to determine if a URL path should be excluded from processing.
 *
 * This class provides powerful pattern matching capabilities including:
 * - Wildcards: * (within path segment), ** (across path segments), ? (single character)
 * - Character sets: [abc], [a-z], [!abc]
 * - Grouping: {api,v1,v2}
 * - Regular expressions with regex: prefix
 *
 * Glob pattern examples:
 * - health - exact match
 * - api/* - matches api/users but not api/v1/users
 * - api/** - matches api/users and api/v1/users
 * - api/v[12]/users - matches api/v1/users and api/v2/users
 * - api/{users,groups}/list - matches api/users/list and api/groups/list
 *
 * Regex pattern examples (must be prefixed with "regex:"):
 * - regex:api/v[0-9]+/users - matches api/v1/users, api/v2/users, api/v123/users
 * - regex:^health(check)?$ - matches only "health" or "healthcheck"
 * - regex:.*\.(jpg|png|gif)$ - matches paths ending with image extensions
 * - regex:api/(user|group|role)/[0-9]+ - matches api/user/123, api/group/456
 * - regex:^(ping|status|healthz?)$ - matches ping, status, health, or healthz
 */
@Singleton
public class AuditUrlPathFilter {
  private static final Logger LOG = LoggerFactory.getLogger(AuditUrlPathFilter.class);
  private static final String PREFIX_GLOB = "glob:";
  private static final String PREFIX_REGEX = "regex:";

  private static boolean matches(Path path, String pattern) {
    try {
      String globPattern = pattern;
      if (!globPattern.startsWith(PREFIX_GLOB) && !globPattern.startsWith(PREFIX_REGEX)) {
        globPattern = PREFIX_GLOB + globPattern;
      }

      PathMatcher matcher = FileSystems.getDefault().getPathMatcher(globPattern);
      if (matcher.matches(path)) {
        return true;
      }
    } catch (Exception e) {
      LOG.warn("Invalid pattern '{}', skipping", pattern, e);
    }
    return false;
  }

  /**
   * Checks if the given URL path should be excluded based on the provided patterns.
   *
   * @param urlPath The URL path to check (e.g., "api/v1/users")
   * @param excludePatterns Comma-separated list of glob patterns
   * @return true if the path matches any exclude pattern, false otherwise
   */
  public boolean isExcluded(String urlPath, String excludePatterns) {
    if (StringUtils.isBlank(urlPath) || StringUtils.isBlank(excludePatterns)) {
      return false;
    }

    try {
      Path path = Paths.get(urlPath);
      String[] patterns = excludePatterns.split(",");

      if (Arrays.stream(patterns)
          .map(String::trim)
          .filter(StringUtils::isNotBlank)
          .anyMatch(p -> matches(path, p))) {
        return true;
      }
    } catch (Exception e) {
      LOG.warn("Error checking URL path '{}' against exclude patterns", urlPath, e);
    }

    return false;
  }
}
