/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

/**
 * Represent one segment in the system identified by a "string" type identifier
 *
 * TODO: need to evaluate if we really need this class
 */
public class SegmentId {

  public static final SegmentId INVALID_segment = new SegmentId("INVALID_SEGMENT");

  public final String _id;

  /**
   * Construct a segmentId object with the given id.
   * @param id segmentId number
   */
  public SegmentId(String id) {
    _id = id;
  }

  /**
   * Returns the segment id
   * @return segment id
   */
  public String getSegmentId() {
    return _id;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_id == null) ? 0 : _id.hashCode());
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
    SegmentId other = (SegmentId) obj;
    if (_id == null) {
      if (other._id != null)
        return false;
    } else if (!_id.equals(other._id))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "SegmentId [_id=" + _id + "]";
  }

}
