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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

  private List<PathMatcher> _excludeMatchers = Collections.emptyList();
  private String _patternString = "";

  /**
   * Updates the exclude patterns used for filtering.
   *
   * @param excludePatterns Comma-separated list of glob patterns
   */
  public void updateExcludePatterns(String excludePatterns) {
    if (StringUtils.isBlank(excludePatterns)) {
      _excludeMatchers = Collections.emptyList();
      _patternString = "";
      LOG.info("Cleared URL exclude patterns");
      return;
    }

    if (excludePatterns.equals(_patternString)) {
      return;
    }

    List<PathMatcher> matchers = new ArrayList<>();
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
          matchers.add(matcher);
          LOG.debug("Added URL exclude pattern: {}", trimmedPattern);
        } catch (Exception e) {
          LOG.error("Invalid pattern '{}', skipping", trimmedPattern, e);
        }
      }
    }

    _excludeMatchers = Collections.unmodifiableList(matchers);
    _patternString = excludePatterns;
    LOG.info("Updated URL exclude patterns with {} patterns", matchers.size());
  }

  /**
   * Checks if the given URL path should be excluded based on configured patterns.
   *
   * @param urlPath The URL path to check (e.g., "/api/v1/users")
   * @return true if the path matches any exclude pattern, false otherwise
   */
  public boolean isExcluded(String urlPath) {
    if (StringUtils.isBlank(urlPath) || _excludeMatchers.isEmpty()) {
      return false;
    }

    try {
      Path path = normalizePath(urlPath);

      for (PathMatcher matcher : _excludeMatchers) {
        if (matcher.matches(path)) {
          LOG.trace("URL path '{}' matched exclude pattern", urlPath);
          return true;
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

  /**
   * Gets the current pattern string for debugging/logging purposes.
   */
  public String getPatternString() {
    return _patternString;
  }

  /**
   * Gets the number of active exclude patterns.
   */
  public int getPatternCount() {
    return _excludeMatchers.size();
  }
}
