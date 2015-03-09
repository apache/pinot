/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.transport.common;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * A segmentId set can be one or more segments. Used to aggregate segments
 * which have a common feature or to employ a function on them.
 */
public class SegmentIdSet {
  // Set of segments that belong to this group
  private final Set<SegmentId> _idSet;

  public SegmentIdSet() {
    _idSet = new HashSet<SegmentId>();
  }

  public void addSegment(SegmentId segment) {
    _idSet.add(segment);
  }

  public void addSegments(Set<SegmentId> segments) {
    _idSet.addAll(segments);
  }

  public void removeSegment(SegmentId segment) {
    _idSet.remove(segment);
  }

  /**
   * Return a segment that is a member of this group.
   * @return
   */
  public SegmentId getOneSegment() {
    if (_idSet.isEmpty()) {
      return null;
    }

    return _idSet.iterator().next();
  }

  public Set<SegmentId> getSegments() {
    return _idSet;
  }

  public List<String> getSegmentsNameList() {
    List<String> result = new ArrayList<String>();
    for (SegmentId s : _idSet) {
      result.add(s.getSegmentId());
    }
    return result;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_idSet == null) ? 0 : _idSet.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SegmentIdSet other = (SegmentIdSet) obj;
    if (_idSet == null) {
      if (other._idSet != null)
        return false;
    } else if (!_idSet.equals(other._idSet))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "SegmentIdSet [_idSet=" + _idSet + "]";
  }
}
