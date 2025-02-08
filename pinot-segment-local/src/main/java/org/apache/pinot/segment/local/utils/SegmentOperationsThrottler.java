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
 * Contains all the segment operation throttlers used to control the total parallel segment related operations that can
 * happen on a given Pinot server. For now this class supports download throttling and index rebuild throttling at the
 * following levels:
 * - All index rebuild throttling
 * - StarTree index rebuild throttling
 * - Server level segment download throttling (this is taken after the table level download semaphore is taken)
 * Code paths that do not need to download or rebuild the index or which don't happen on the server need not utilize
 * this throttler. The throttlers passed in for now cannot be 'null', instead for code paths that do not need
 * throttling, this object itself will be passed in as 'null'.
 */
public class SegmentOperationsThrottler implements PinotClusterConfigChangeListener {

  private final SegmentAllIndexPreprocessThrottler _segmentAllIndexPreprocessThrottler;
  private final SegmentStarTreePreprocessThrottler _segmentStarTreePreprocessThrottler;
  private final SegmentDownloadThrottler _segmentDownloadThrottler;

  /**
   * Constructor for SegmentOperationsThrottler
   * @param segmentAllIndexPreprocessThrottler segment preprocess throttler to use for all indexes
   * @param segmentStarTreePreprocessThrottler segment preprocess throttler to use for StarTree index
   * @param segmentDownloadThrottler segment download throttler to throttle download at server level
   */
  public SegmentOperationsThrottler(SegmentAllIndexPreprocessThrottler segmentAllIndexPreprocessThrottler,
      SegmentStarTreePreprocessThrottler segmentStarTreePreprocessThrottler,
      SegmentDownloadThrottler segmentDownloadThrottler) {
    _segmentAllIndexPreprocessThrottler = segmentAllIndexPreprocessThrottler;
    _segmentStarTreePreprocessThrottler = segmentStarTreePreprocessThrottler;
    _segmentDownloadThrottler = segmentDownloadThrottler;
  }

  public SegmentAllIndexPreprocessThrottler getSegmentAllIndexPreprocessThrottler() {
    return _segmentAllIndexPreprocessThrottler;
  }

  public SegmentStarTreePreprocessThrottler getSegmentStarTreePreprocessThrottler() {
    return _segmentStarTreePreprocessThrottler;
  }

  public SegmentDownloadThrottler getSegmentDownloadThrottler() {
    return _segmentDownloadThrottler;
  }

  public void startServingQueries() {
    _segmentAllIndexPreprocessThrottler.startServingQueries();
    _segmentStarTreePreprocessThrottler.startServingQueries();
    _segmentDownloadThrottler.startServingQueries();
  }

  @Override
  public synchronized void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    _segmentAllIndexPreprocessThrottler.onChange(changedConfigs, clusterConfigs);
    _segmentStarTreePreprocessThrottler.onChange(changedConfigs, clusterConfigs);
    _segmentDownloadThrottler.onChange(changedConfigs, clusterConfigs);
  }
}
