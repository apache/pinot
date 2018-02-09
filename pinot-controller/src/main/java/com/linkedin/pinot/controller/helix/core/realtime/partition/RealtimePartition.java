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

package com.linkedin.pinot.controller.helix.core.realtime.partition;

import com.linkedin.pinot.common.utils.EqualityUtils;
import java.util.List;


/**
 * Class to represent a stream partition for a realtime table
 */
public class RealtimePartition {

  private String _partitionNum;
  private List<String> _instanceNames;

  public RealtimePartition(String partitionNum, List<String> instanceNames) {
    _partitionNum = partitionNum;
    _instanceNames = instanceNames;
  }

  public String getPartitionNum() {
    return _partitionNum;
  }

  public List<String> getInstanceNames() {
    return _instanceNames;
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    RealtimePartition that = (RealtimePartition) o;

    return EqualityUtils.isEqual(_partitionNum, that._partitionNum) && EqualityUtils.isEqual(_instanceNames,
        that._instanceNames);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_partitionNum);
    result = EqualityUtils.hashCodeOf(result, _instanceNames);
    return result;
  }
}
