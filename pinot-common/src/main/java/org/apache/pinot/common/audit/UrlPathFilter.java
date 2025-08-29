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

import com.google.common.annotations.VisibleForTesting;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import javax.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * URL path filter utility that uses Java's PathMatcher with glob patterns
 * to determine if a URL path should be excluded from processing.
 *
 * This class provides powerful pattern matching capabilities including:
 * - Wildcards: * (within path segment), ** (across path segments), ? (single character)
 * - Character sets: [abc], [a-z], [!abc]
 * - Grouping: {api,v1,v2}
 *
 * Examples:
 * - /health - exact match
 * - /api/* - matches /api/users but not /api/v1/users
 * - /api/** - matches /api/users and /api/v1/users
 * - /api/v[12]/users - matches /api/v1/users and /api/v2/users
 * - /api/{users,groups}/list - matches /api/users/list and /api/groups/list
 */
@Singleton
public class UrlPathFilter {
  private static final Logger LOG = LoggerFactory.getLogger(UrlPathFilter.class);

  /**
   * Checks if the given URL path should be excluded based on the provided patterns.
   *
   * @param urlPath The URL path to check (e.g., "/api/v1/users")
   * @param excludePatterns Comma-separated list of glob patterns
   * @return true if the path matches any exclude pattern, false otherwise
   */
  public boolean isExcluded(String urlPath, String excludePatterns) {
    if (StringUtils.isBlank(urlPath) || StringUtils.isBlank(excludePatterns)) {
      return false;
    }

    try {
      Path path = normalizePath(urlPath);
      String[] patterns = excludePatterns.split(",");

      for (String pattern : patterns) {
        String trimmedPattern = pattern.trim();
        if (StringUtils.isNotBlank(trimmedPattern)) {
          try {
            String globPattern = trimmedPattern;
            if (!globPattern.startsWith("glob:") && !globPattern.startsWith("regex:")) {
              globPattern = "glob:" + globPattern;
            }

            PathMatcher matcher = FileSystems.getDefault().getPathMatcher(globPattern);
            if (matcher.matches(path)) {
              return true;
            }
          } catch (Exception e) {
            LOG.error("Invalid pattern '{}', skipping", trimmedPattern, e);
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Error checking URL path '{}' against exclude patterns", urlPath, e);
    }

    return false;
  }

  /**
   * Normalizes a URL path for consistent matching.
   * Ensures the path starts with / and uses forward slashes consistently.
   */
  @VisibleForTesting
  static Path normalizePath(String urlPath) {
    String normalized = urlPath;

    if (!normalized.startsWith("/")) {
      normalized = "/" + normalized;
    }

    normalized = normalized.replace('\\', '/');

    while (normalized.contains("//")) {
      normalized = normalized.replace("//", "/");
    }

    if (normalized.length() > 1 && normalized.endsWith("/")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }

    return Paths.get(normalized);
  }
}
