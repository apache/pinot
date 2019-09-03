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
package org.apache.pinot.common.lineage;

import java.util.List;
import java.util.Set;


/**
 * Class to represent segment group
 */
public class SegmentGroup {

  private String _groupId;
  private int _groupLevel;
  private SegmentGroup _parentGroup;
  private List<SegmentGroup> _childrenGroups;
  private Set<String> _segments;

  public String getGroupId() {
    return _groupId;
  }

  public void setGroupId(String groupId) {
    _groupId = groupId;
  }

  public SegmentGroup getParentGroup() {
    return _parentGroup;
  }

  public void setParentGroup(SegmentGroup parentGroup) {
    _parentGroup = parentGroup;
  }

  public List<SegmentGroup> getChildrenGroups() {
    return _childrenGroups;
  }

  public void setChildrenGroups(List<SegmentGroup> childrenGroups) {
    _childrenGroups = childrenGroups;
  }

  public Set<String> getSegments() {
    return _segments;
  }

  public void setSegments(Set<String> segments) {
    _segments = segments;
  }

  public int getGroupLevel() {
    return _groupLevel;
  }

  public void setGroupLevel(int groupLevel) {
    _groupLevel = groupLevel;
  }
}
