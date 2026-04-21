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
package org.apache.pinot.common.version;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;


/**
 * Utility methods for parsing and comparing Pinot version strings.
 *
 * <p>Supported formats:
 * <ul>
 *   <li>{@code 1.2.3}          — release</li>
 *   <li>{@code 1.2.3-SNAPSHOT} — snapshot pre-release</li>
 *   <li>{@code 1.2.3-rc1}      — release candidate</li>
 *   <li>{@code UNKNOWN}        — version could not be determined (non-Maven build or missing file)</li>
 * </ul>
 *
 * <p>Comparison ordering (highest to lowest):
 * {@code 1.3.0 > 1.2.3 > 1.2.3-rc2 > 1.2.3-rc1 > 1.2.3-SNAPSHOT > 1.2.2 > UNKNOWN}
 *
 * <p>{@code UNKNOWN} is always considered less than any parseable version so that unknown
 * instances do not artificially raise the cluster's apparent minimum version.
 */
public final class VersionParsingUtils {

  // Pre-release type ranks: higher = newer
  private static final int RANK_SNAPSHOT = 0;
  private static final int RANK_RC = 1;
  private static final int RANK_RELEASE = 2;

  // Matches strict `major.minor.patch` optionally followed by `-SNAPSHOT` or `-rc<N>`.
  // The suffix must immediately follow the numeric patch component — arbitrary intermediate
  // qualifiers (e.g. `1.2.3-foo-rc10`) are rejected to avoid ambiguous parses.
  private static final Pattern VERSION_PATTERN =
      Pattern.compile("^(\\d+)\\.(\\d+)\\.(\\d+)(?:-(SNAPSHOT|rc(\\d+)))?$");

  private VersionParsingUtils() {
  }

  /**
   * Parses a raw version string into a {@link ParsedVersion}.
   *
   * @param rawVersion the version string to parse; may be {@code null}
   * @return a parsed version, or {@code null} if the string is null, blank, {@code UNKNOWN},
   *         the Maven placeholder {@code ${project.version}}, or cannot be parsed
   */
  @Nullable
  public static ParsedVersion parse(@Nullable String rawVersion) {
    if (rawVersion == null || rawVersion.isBlank()) {
      return null;
    }
    if (PinotVersion.UNKNOWN.equals(rawVersion) || "${project.version}".equals(rawVersion)) {
      return null;
    }

    Matcher m = VERSION_PATTERN.matcher(rawVersion.trim());
    if (!m.matches()) {
      return null;
    }
    try {
      int major = Integer.parseInt(m.group(1));
      int minor = Integer.parseInt(m.group(2));
      int patch = Integer.parseInt(m.group(3));
      String suffix = m.group(4);
      boolean isSnapshot = false;
      int preReleaseRank = RANK_RELEASE;
      int preReleaseNum = 0;
      if (suffix != null) {
        if ("SNAPSHOT".equals(suffix)) {
          isSnapshot = true;
          preReleaseRank = RANK_SNAPSHOT;
        } else {
          // -rc<N>
          preReleaseRank = RANK_RC;
          preReleaseNum = Integer.parseInt(m.group(5));
        }
      }
      return new ParsedVersion(major, minor, patch, isSnapshot, preReleaseRank, preReleaseNum, rawVersion);
    } catch (NumberFormatException ignored) {
      return null;
    }
  }

  /**
   * Compares two raw version strings.
   *
   * <ul>
   *   <li>Returns a negative value if {@code v1 < v2}</li>
   *   <li>Returns {@code 0} if equal</li>
   *   <li>Returns a positive value if {@code v1 > v2}</li>
   *   <li>Two unparseable / UNKNOWN versions are considered equal</li>
   *   <li>An UNKNOWN version is always less than any parseable version</li>
   * </ul>
   */
  public static int compare(@Nullable String v1, @Nullable String v2) {
    ParsedVersion pv1 = parse(v1);
    ParsedVersion pv2 = parse(v2);
    if (pv1 == null && pv2 == null) {
      return 0;
    }
    if (pv1 == null) {
      return -1;
    }
    if (pv2 == null) {
      return 1;
    }
    return pv1.compareTo(pv2);
  }

  /**
   * Returns {@code true} if the version string is parseable (i.e. not null, blank, UNKNOWN, or
   * otherwise unrecognized).
   */
  public static boolean isKnown(@Nullable String rawVersion) {
    return parse(rawVersion) != null;
  }

  /**
   * Returns {@code true} if the version is a SNAPSHOT build.
   * Returns {@code false} for null, UNKNOWN, or non-snapshot versions.
   */
  public static boolean isSnapshot(@Nullable String rawVersion) {
    ParsedVersion pv = parse(rawVersion);
    return pv != null && pv.isSnapshot();
  }
}
