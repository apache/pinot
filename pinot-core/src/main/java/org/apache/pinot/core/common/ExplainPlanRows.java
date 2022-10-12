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
package org.apache.pinot.core.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Class to hold all the rows for a given Explain Plan
 */
public class ExplainPlanRows implements Comparable<ExplainPlanRows> {
  public static final String ALL_SEGMENTS_PRUNED_ON_SERVER = "ALL_SEGMENTS_PRUNED_ON_SERVER";
  public static final String PLAN_START = "PLAN_START(numSegmentsForThisPlan:";
  public static final String PLAN_START_FORMAT = PLAN_START + "%d" + ")";
  public static final int PLAN_START_IDS = -1;

  private final List<ExplainPlanRowData> _explainPlanRowData;
  private boolean _hasEmptyFilter;
  private boolean _hasMatchAllFilter;
  private boolean _hasNoMatchingSegment;
  private int _numSegmentsMatchingThisPlan;

  public ExplainPlanRows() {
    _explainPlanRowData = new ArrayList<>();
    _hasEmptyFilter = false;
    _hasMatchAllFilter = false;
    _hasNoMatchingSegment = false;
    _numSegmentsMatchingThisPlan = 0;
  }

  public List<ExplainPlanRowData> getExplainPlanRowData() {
    return _explainPlanRowData;
  }

  public void appendExplainPlanRowData(ExplainPlanRowData explainPlanRowData) {
    _explainPlanRowData.add(explainPlanRowData);
  }

  public boolean isHasEmptyFilter() {
    return _hasEmptyFilter;
  }

  public void setHasEmptyFilter(boolean hasEmptyFilter) {
    _hasEmptyFilter = hasEmptyFilter;
  }

  public boolean isHasMatchAllFilter() {
    return _hasMatchAllFilter;
  }

  public void setHasMatchAllFilter(boolean hasMatchAllFilter) {
    _hasMatchAllFilter = hasMatchAllFilter;
  }

  public boolean isHasNoMatchingSegment() {
    return _hasNoMatchingSegment;
  }

  public void setHasNoMatchingSegment(boolean hasNoMatchingSegment) {
    _hasNoMatchingSegment = hasNoMatchingSegment;
  }

  public int getNumSegmentsMatchingThisPlan() {
    return _numSegmentsMatchingThisPlan;
  }

  public void incrementNumSegmentsMatchingThisPlan() {
    _numSegmentsMatchingThisPlan++;
  }

  public void setNumSegmentsMatchingThisPlan(int numSegmentsMatchingThisPlan) {
    _numSegmentsMatchingThisPlan = numSegmentsMatchingThisPlan;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExplainPlanRows that = (ExplainPlanRows) o;
    return _hasEmptyFilter == that._hasEmptyFilter && _hasMatchAllFilter == that._hasMatchAllFilter
        && _hasNoMatchingSegment == that._hasNoMatchingSegment
        && Objects.equals(_explainPlanRowData, that._explainPlanRowData);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_explainPlanRowData, _hasEmptyFilter, _hasMatchAllFilter, _hasNoMatchingSegment);
  }

  @Override
  public int compareTo(ExplainPlanRows o) {
    int thisHashCode = this.hashCode();
    int otherHashCode = o.hashCode();
    return Integer.compare(thisHashCode, otherHashCode);
  }
}
