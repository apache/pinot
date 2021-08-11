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
package org.apache.pinot.broker.routing.segmentselector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * The SelectedSegments has a strict (Sha256 for now) hash for all the segments in it.
 * It can be used to quickly compare whether the two segment sets are identical.
 * It will be helpful for high QPS queries for tables with lots of segments
 */
public class SelectedSegments {
  private final Set<String> _segments;
  private final String _segmentHash;
  private final boolean _hasHash;
  public static final String EMPTY_HASH = "";

  /**
   * Note that computing Hash involves sorting, so it is O(N*LogN) complexity
   * @param segments the segments
   * @param computeHash whether we compute the hash for quick comparison.
   *                    Note that computing Hash involves sorting, so it is O(N*LogN) complexity
   */
  public SelectedSegments(Set<String> segments, boolean computeHash){
      this(segments, computeHash ? computeHash(segments) : EMPTY_HASH);
  }

  public SelectedSegments(Set<String> segments, String segmentHash) {
    _segments = segments;
    _segmentHash = segmentHash;
    _hasHash = !segmentHash.equals(EMPTY_HASH);
  }

  /**
   * Gets the segment set
   * @return the set of all segments
   */
  public Set<String> getSegments() {
    return _segments;
  }

  /**
   * Gets the checksum of all the segments in it. Mostly used for quick comparison of sets
   * @return The string representation of hash
   */
  public String getSegmentHash() {
    return _segmentHash;
  }

  /**
   * Gets whether the hash is valid and usable
   * @return a flag that whether the hash is valid
   */
  public boolean hasHash() {
    return _hasHash;
  }

  /**
   * Compute the hash checksum for all segments
   * @param segments the set for all segments
   * @return string digest of byte arrays
   */
  public static String computeHash(Set<String> segments) {
    SortedSet<String> sorted = new TreeSet<>(segments);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
    for (String segment: sorted) {
      outputStream.write(segment.getBytes());
    }
    } catch (IOException ex) {
      throw new RuntimeException("Byte Output Stream should not throw exceptions", ex);
    }
    return DigestUtils.sha256Hex(outputStream.toByteArray());
  }

}
