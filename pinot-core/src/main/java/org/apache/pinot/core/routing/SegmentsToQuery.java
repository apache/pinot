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
package org.apache.pinot.core.routing;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;


/// Wrapper class around the list of segments and optional segments that need to be queried on a particular server.
///
/// Optional segments are new segments that are not online in the external view yet, but might already be loaded on the
/// server. The server queries the ones it has loaded and skips the others without reporting them as missing, while a
/// segment of the other list that the server cannot find is reported as missing.
public class SegmentsToQuery {
  private final List<String> _segments;
  private final List<String> _optionalSegments;

  /// Constructor for SegmentsToQuery without optional segments.
  ///
  /// @param segments List of segments assigned to the server.
  public SegmentsToQuery(List<String> segments) {
    this(segments, List.of());
  }

  /// Constructor for SegmentsToQuery.
  ///
  /// @param segments List of segments assigned to the server.
  /// @param optionalSegments List of optional segments assigned to the server, `null` when there is none.
  public SegmentsToQuery(List<String> segments, @Nullable List<String> optionalSegments) {
    _segments = segments;
    _optionalSegments = optionalSegments != null ? optionalSegments : List.of();
  }

  /// Gets the list of segments assigned to the server.
  ///
  /// @return List of segments.
  public List<String> getSegments() {
    return _segments;
  }

  /// Gets the list of optional segments assigned to the server.
  ///
  /// @return List of optional segments.
  public List<String> getOptionalSegments() {
    return _optionalSegments;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SegmentsToQuery)) {
      return false;
    }
    SegmentsToQuery that = (SegmentsToQuery) o;
    return _segments.equals(that._segments) && _optionalSegments.equals(that._optionalSegments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_segments, _optionalSegments);
  }

  @Override
  public String toString() {
    return "SegmentsToQuery{segments=" + _segments + ", optionalSegments=" + _optionalSegments + '}';
  }
}
