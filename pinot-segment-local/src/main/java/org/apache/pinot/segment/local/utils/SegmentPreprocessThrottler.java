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
package org.apache.pinot.segment.local.utils;

import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;


/**
 * Contains all the segment preprocess throttlers used to control the total index rebuilds that can happen on a given
 * Pinot server. For now this class supports index rebuild throttling at the following levels:
 * - All index throttling
 * - StarTree index throttling
 * Code paths that do no need to rebuild the index or which don't happen on the server need not utilize this throttler.
 * The throttlers passed in for now cannot be 'null', instead for code paths that do not need throttling, this object
 * itself will be passed in as 'null'.
 */
public class SegmentPreprocessThrottler implements PinotClusterConfigChangeListener {

  private final SegmentAllIndexPreprocessThrottler _segmentAllIndexPreprocessThrottler;
  private final SegmentStarTreePreprocessThrottler _segmentStarTreePreprocessThrottler;

  /**
   * Constructor for SegmentPreprocessThrottler
   * @param segmentAllIndexPreprocessThrottler segment preprocess throttler to use for all indexes
   * @param segmentStarTreePreprocessThrottler segment preprocess throttler to use for StarTree index
   */
  public SegmentPreprocessThrottler(SegmentAllIndexPreprocessThrottler segmentAllIndexPreprocessThrottler,
      SegmentStarTreePreprocessThrottler segmentStarTreePreprocessThrottler) {
    _segmentAllIndexPreprocessThrottler = segmentAllIndexPreprocessThrottler;
    _segmentStarTreePreprocessThrottler = segmentStarTreePreprocessThrottler;
  }

  public SegmentAllIndexPreprocessThrottler getSegmentAllIndexPreprocessThrottler() {
    return _segmentAllIndexPreprocessThrottler;
  }

  public SegmentStarTreePreprocessThrottler getSegmentStarTreePreprocessThrottler() {
    return _segmentStarTreePreprocessThrottler;
  }

  public void startServingQueries() {
    _segmentAllIndexPreprocessThrottler.startServingQueries();
    _segmentStarTreePreprocessThrottler.startServingQueries();
  }

  @Override
  public synchronized void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    _segmentAllIndexPreprocessThrottler.onChange(changedConfigs, clusterConfigs);
    _segmentStarTreePreprocessThrottler.onChange(changedConfigs, clusterConfigs);
  }
}
