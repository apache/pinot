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
package org.apache.pinot.tsdb.m3ql.time;

import java.util.HashSet;
import java.util.Set;


public class QueryTimeBoundaryConstraints {
  private final Set<Long> _divisors = new HashSet<>();
  private long _leftExtensionSeconds = 0;
  private long _rightExtensionSeconds = 0;
  private boolean _leftAligned = false;

  public Set<Long> getDivisors() {
    return _divisors;
  }

  public long getLeftExtensionSeconds() {
    return _leftExtensionSeconds;
  }

  public void setLeftExtensionSeconds(long leftExtensionSeconds) {
    _leftExtensionSeconds = leftExtensionSeconds;
  }

  public long getRightExtensionSeconds() {
    return _rightExtensionSeconds;
  }

  public void setRightExtensionSeconds(long rightExtensionSeconds) {
    _rightExtensionSeconds = rightExtensionSeconds;
  }

  public boolean isLeftAligned() {
    return _leftAligned;
  }

  public void setLeftAligned(boolean leftAligned) {
    _leftAligned = leftAligned;
  }

  public static QueryTimeBoundaryConstraints merge(QueryTimeBoundaryConstraints left,
      QueryTimeBoundaryConstraints right) {
    QueryTimeBoundaryConstraints merged = new QueryTimeBoundaryConstraints();
    merged._divisors.addAll(left._divisors);
    merged._divisors.addAll(right._divisors);
    merged._leftExtensionSeconds = Math.max(left._leftExtensionSeconds, right._leftExtensionSeconds);
    merged._rightExtensionSeconds = Math.max(left._rightExtensionSeconds, right._rightExtensionSeconds);
    if (left._leftAligned != right._leftAligned) {
      throw new IllegalArgumentException(String.format("Cannot merge constraints with different alignments. "
              + "Alignment from plan node on the left and right are %s and %s respectively",
          left._leftAligned, right._leftAligned));
    }
    merged._leftAligned = left._leftAligned;
    return merged;
  }
}
