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
  private boolean _isStreamSegmentDownloadUntar; // whether to turn on stream segment download-untar
  private long _streamSegmentDownloadUntarRateLimitBytesPerSec; // the per segment rate limit for stream download-untar
  private int _maxParallelSegmentDownloads; // max number of segment download in parallel per table
  private boolean _isRetrySegmentDownloadUntarFailure; // whether to retry segment untar failures
  private int _segmentDownloadUntarRetryCount;
  private int _segmentDownloadUntarRetryWaitMs;
  private int _segmentDownloadUntarRetryDelayScaleFactor;

  public TableDataManagerParams(int maxParallelSegmentDownloads, boolean isStreamSegmentDownloadUntar,
      long streamSegmentDownloadUntarRateLimitBytesPerSec) {
    _maxParallelSegmentDownloads = maxParallelSegmentDownloads;
    _isStreamSegmentDownloadUntar = isStreamSegmentDownloadUntar;
    _streamSegmentDownloadUntarRateLimitBytesPerSec = streamSegmentDownloadUntarRateLimitBytesPerSec;
  }

  public TableDataManagerParams(InstanceDataManagerConfig instanceDataManagerConfig) {
    _maxParallelSegmentDownloads = instanceDataManagerConfig.getMaxParallelSegmentDownloads();
    _isStreamSegmentDownloadUntar = instanceDataManagerConfig.isStreamSegmentDownloadUntar();
    _isRetrySegmentDownloadUntarFailure = instanceDataManagerConfig.isRetrySegmentDownloadUntarFailure();
    _segmentDownloadUntarRetryCount = instanceDataManagerConfig.getSegmentDownloadUntarRetryCount();
    _segmentDownloadUntarRetryWaitMs = instanceDataManagerConfig.getSegmentDownloadUntarRetryWaitMs();
    _segmentDownloadUntarRetryDelayScaleFactor =
        instanceDataManagerConfig.getSegmentDownloadUntarRetryDelayScaleFactor();
    _streamSegmentDownloadUntarRateLimitBytesPerSec =
        instanceDataManagerConfig.getStreamSegmentDownloadUntarRateLimit();
  }

  public TableDataManagerParams(int maxParallelSegmentDownloads, boolean isStreamSegmentDownloadUntar,
      boolean isRetrySegmentDownloadUntarFailure, int segmentDownloadUntarRetryCount,
      int segmentDownloadUntarRetryWaitMs, int segmentDownloadUntarRetryDelayScaleFactor,
      long streamSegmentDownloadUntarRateLimitBytesPerSec) {
    _maxParallelSegmentDownloads = maxParallelSegmentDownloads;
    _isStreamSegmentDownloadUntar = isStreamSegmentDownloadUntar;
    _isRetrySegmentDownloadUntarFailure = isRetrySegmentDownloadUntarFailure;
    _segmentDownloadUntarRetryCount = segmentDownloadUntarRetryCount;
    _segmentDownloadUntarRetryWaitMs = segmentDownloadUntarRetryWaitMs;
    _segmentDownloadUntarRetryDelayScaleFactor = segmentDownloadUntarRetryDelayScaleFactor;
    _streamSegmentDownloadUntarRateLimitBytesPerSec = streamSegmentDownloadUntarRateLimitBytesPerSec;
  }

  public boolean isStreamSegmentDownloadUntar() {
    return _isStreamSegmentDownloadUntar;
  }

  public long getStreamSegmentDownloadUntarRateLimitBytesPerSec() {
    return _streamSegmentDownloadUntarRateLimitBytesPerSec;
  }

  public void setStreamSegmentDownloadUntar(boolean streamSegmentDownloadUntar) {
    _isStreamSegmentDownloadUntar = streamSegmentDownloadUntar;
  }

  public void setStreamSegmentDownloadUntarRateLimitBytesPerSec(long streamSegmentDownloadUntarRateLimitBytesPerSec) {
    _streamSegmentDownloadUntarRateLimitBytesPerSec = streamSegmentDownloadUntarRateLimitBytesPerSec;
  }

  public boolean isRetrySegmentDownloadUntarFailure() {
    return _isRetrySegmentDownloadUntarFailure;
  }

  public void setRetrySegmentDownloadUntarFailure(boolean retrySegmentDownloadUntarFailure) {
    _isRetrySegmentDownloadUntarFailure = retrySegmentDownloadUntarFailure;
  }

  public int getSegmentDownloadUntarRetryCount() {
    return _segmentDownloadUntarRetryCount;
  }

  public void setSegmentDownloadUntarRetryCount(int retryCount) {
    _segmentDownloadUntarRetryCount = retryCount;
  }

  public int getSegmentDownloadUntarRetryWaitMs() {
    return _segmentDownloadUntarRetryWaitMs;
  }

  public void setSegmentDownloadUntarRetryWaitMs(int retryWaitMs) {
    _segmentDownloadUntarRetryWaitMs = retryWaitMs;
  }

  public int getSegmentDownloadUntarRetryDelayScaleFactor() {
    return _segmentDownloadUntarRetryDelayScaleFactor;
  }

  public void setSegmentDownloadUntarRetryDelayScaleFactor(int retryDelayScaleFactor) {
    _segmentDownloadUntarRetryDelayScaleFactor = retryDelayScaleFactor;
  }

  public int getMaxParallelSegmentDownloads() {
    return _maxParallelSegmentDownloads;
  }

  public void setMaxParallelSegmentDownloads(int maxParallelSegmentDownloads) {
    _maxParallelSegmentDownloads = maxParallelSegmentDownloads;
  }
}
