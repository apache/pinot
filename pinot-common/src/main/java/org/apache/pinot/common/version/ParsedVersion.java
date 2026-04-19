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


/**
 * Immutable representation of a parsed Pinot version string.
 *
 * <p>Instances are obtained via {@link VersionParsingUtils#parse(String)}.
 */
public final class ParsedVersion implements Comparable<ParsedVersion> {

  private final int _major;
  private final int _minor;
  private final int _patch;
  private final boolean _isSnapshot;
  // Higher rank = newer pre-release type. SNAPSHOT=0, RC=1, RELEASE=2.
  private final int _preReleaseRank;
  // Discriminator within the same pre-release type (e.g. rc2 > rc1).
  private final int _preReleaseNum;
  private final String _rawVersion;

  ParsedVersion(int major, int minor, int patch, boolean isSnapshot,
      int preReleaseRank, int preReleaseNum, String rawVersion) {
    _major = major;
    _minor = minor;
    _patch = patch;
    _isSnapshot = isSnapshot;
    _preReleaseRank = preReleaseRank;
    _preReleaseNum = preReleaseNum;
    _rawVersion = rawVersion;
  }

  public int getMajor() {
    return _major;
  }

  public int getMinor() {
    return _minor;
  }

  public int getPatch() {
    return _patch;
  }

  public boolean isSnapshot() {
    return _isSnapshot;
  }

  public String getRawVersion() {
    return _rawVersion;
  }

  /**
   * Returns {@code true} if this version is at least {@code other}
   * (i.e. {@code this >= other}).
   */
  public boolean isAtLeast(ParsedVersion other) {
    return compareTo(other) >= 0;
  }

  @Override
  public int compareTo(ParsedVersion other) {
    int cmp = Integer.compare(_major, other._major);
    if (cmp != 0) {
      return cmp;
    }
    cmp = Integer.compare(_minor, other._minor);
    if (cmp != 0) {
      return cmp;
    }
    cmp = Integer.compare(_patch, other._patch);
    if (cmp != 0) {
      return cmp;
    }
    // Within the same base version: release > rc > snapshot
    cmp = Integer.compare(_preReleaseRank, other._preReleaseRank);
    if (cmp != 0) {
      return cmp;
    }
    return Integer.compare(_preReleaseNum, other._preReleaseNum);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ParsedVersion)) {
      return false;
    }
    return compareTo((ParsedVersion) o) == 0;
  }

  @Override
  public int hashCode() {
    int result = _major;
    result = 31 * result + _minor;
    result = 31 * result + _patch;
    result = 31 * result + _preReleaseRank;
    result = 31 * result + _preReleaseNum;
    return result;
  }

  @Override
  public String toString() {
    return _rawVersion;
  }
}
