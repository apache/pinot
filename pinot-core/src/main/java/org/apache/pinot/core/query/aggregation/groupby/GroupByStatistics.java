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
package org.apache.pinot.core.query.aggregation.groupby;

/**
 * Statistics recorded during the execution of group-by operator.
 */
public class GroupByStatistics {
  private long _numGroupsIgnored;

  public GroupByStatistics() {
  }

  public GroupByStatistics(long numGroupsIgnored) {
    _numGroupsIgnored = numGroupsIgnored;
  }

  public long getNumGroupsIgnored() {
    return _numGroupsIgnored;
  }

  /**
   * Merge another group-by statistics into the current one.
   *
   * @param statsToMerge statistics to merge.
   */
  public void merge(GroupByStatistics statsToMerge) {
    _numGroupsIgnored += statsToMerge._numGroupsIgnored;
  }

  @Override
  public String toString() {
    return "GroupBy Statistics:" + "\n  numGroupsIgnored: " + _numGroupsIgnored;
  }
}

