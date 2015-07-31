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
package com.linkedin.pinot.core.startree;

import com.google.common.base.Objects;

import java.util.*;

public class StarTreeTableGroupByStats {
  private Map<Integer, Integer> rawCounts;
  private Map<Integer, Integer> uniqueCounts;
  private Map<Integer, Integer> minRecordIds;

  public StarTreeTableGroupByStats() {
    this.rawCounts = new HashMap<Integer, Integer>();
    this.uniqueCounts = new HashMap<Integer, Integer>();
    this.minRecordIds = new HashMap<Integer, Integer>();
  }

  public void setValue(Integer value, Integer rawCount, Integer uniqueCount, Integer minRecordId) {
    rawCounts.put(value, rawCount);
    uniqueCounts.put(value, uniqueCount);
    minRecordIds.put(value, minRecordId);
  }

  public void incrementRawCount(Integer value) {
    incrementCount(rawCounts, value);
  }

  public void incrementUniqueCount(Integer value) {
    incrementCount(uniqueCounts, value);
  }

  public void updateMinRecordId(Integer value, Integer recordId) {
    Integer existing = minRecordIds.get(value);
    if (existing == null || recordId < existing) {
      minRecordIds.put(value, recordId);
    }
  }

  private void incrementCount(Map<Integer, Integer> counts, Integer key) {
    Integer existing = counts.get(key);
    if (existing == null) {
      existing = 0;
    }
    counts.put(key, existing + 1);
  }

  public Set<Integer> getValues() {
    return uniqueCounts.keySet();
  }

  public Integer getRawCount(Integer value) {
    return rawCounts.get(value);
  }

  public Integer getUniqueCount(Integer value) {
    return uniqueCounts.get(value);
  }

  public Integer getMinRecordId(Integer value) {
    return minRecordIds.get(value);
  }

  @Override
  public String toString() {
    Objects.ToStringHelper helper = Objects.toStringHelper(this);
    for (Integer value : getValues()) {
      List<Integer> stats = new ArrayList<Integer>();
      stats.add(getRawCount(value));
      stats.add(getUniqueCount(value));
      stats.add(getMinRecordId(value));
      helper.add(String.valueOf(value), stats);
    }
    return helper.toString();
  }
}
