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
package org.apache.pinot.segment.local.data.manager;

import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;


public class TableDataManagerParams {
  private boolean _isSegmentDownloadUntarStreamed;
  private long _segmentDownloadUntarRateLimit;
  private int _maxParallelSegmentDownloads;

  public TableDataManagerParams(int maxParallelSegmentDownloads, boolean isSegmentDownloadUntarStreamed,
      long segmentDownloadUntarRateLimit) {
    _maxParallelSegmentDownloads = maxParallelSegmentDownloads;
    _isSegmentDownloadUntarStreamed = isSegmentDownloadUntarStreamed;
    _segmentDownloadUntarRateLimit = segmentDownloadUntarRateLimit;
  }

  public TableDataManagerParams(InstanceDataManagerConfig instanceDataManagerConfig) {
    _maxParallelSegmentDownloads = instanceDataManagerConfig.getMaxParallelSegmentDownloads();
    _isSegmentDownloadUntarStreamed = instanceDataManagerConfig.isSegmentDownloadUntarStreamed();
    _segmentDownloadUntarRateLimit = instanceDataManagerConfig.getSegmentDownloadUntarRateLimit();
  }

  public boolean getSegmentDownloadUntarStreamed() {
    return _isSegmentDownloadUntarStreamed;
  }

  public long getSegmentDownloadUntarRateLimit() {
    return _segmentDownloadUntarRateLimit;
  }

  public void setSegmentDownloadUntarStreamed(boolean segmentDownloadUntarStreamed) {
    _isSegmentDownloadUntarStreamed = segmentDownloadUntarStreamed;
  }

  public void setSegmentDownloadUntarRateLimit(long segmentDownloadUntarRateLimit) {
    _segmentDownloadUntarRateLimit = segmentDownloadUntarRateLimit;
  }

  public int getMaxParallelSegmentDownloads() {
    return _maxParallelSegmentDownloads;
  }

  public void setMaxParallelSegmentDownloads(int maxParallelSegmentDownloads) {
    _maxParallelSegmentDownloads = maxParallelSegmentDownloads;
  }
}
