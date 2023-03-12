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
  private int _retryCount;
  private int _retryWaitMs;
  private int _retryDelayScaleFactor;

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
    _retryCount = instanceDataManagerConfig.getRetryCount();
    _retryWaitMs = instanceDataManagerConfig.getRetryWaitMs();
    _retryDelayScaleFactor = instanceDataManagerConfig.getRetryDelayScaleFactor();
    _streamSegmentDownloadUntarRateLimitBytesPerSec =
        instanceDataManagerConfig.getStreamSegmentDownloadUntarRateLimit();
  }

  public TableDataManagerParams(int maxParallelSegmentDownloads, boolean isStreamSegmentDownloadUntar,
      boolean isRetrySegmentDownloadUntarFailure, int retryCount, int retryWaitMs, int retryDelayScaleFactor,
      long streamSegmentDownloadUntarRateLimitBytesPerSec) {
    _maxParallelSegmentDownloads = maxParallelSegmentDownloads;
    _isStreamSegmentDownloadUntar = isStreamSegmentDownloadUntar;
    _isRetrySegmentDownloadUntarFailure = isRetrySegmentDownloadUntarFailure;
    _retryCount = retryCount;
    _retryWaitMs = retryWaitMs;
    _retryDelayScaleFactor = retryDelayScaleFactor;
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

  public int getRetryCount() {
    return _retryCount;
  }

  public void setRetryCount(int retryCount) {
    _retryCount = retryCount;
  }

  public int getRetryWaitMs() {
    return _retryWaitMs;
  }

  public void setRetryWaitMs(int retryWaitMs) {
    _retryWaitMs = retryWaitMs;
  }

  public int getRetryDelayScaleFactor() {
    return _retryDelayScaleFactor;
  }

  public void setRetryDelayScaleFactor(int retryDelayScaleFactor) {
    _retryDelayScaleFactor = retryDelayScaleFactor;
  }

  public int getMaxParallelSegmentDownloads() {
    return _maxParallelSegmentDownloads;
  }

  public void setMaxParallelSegmentDownloads(int maxParallelSegmentDownloads) {
    _maxParallelSegmentDownloads = maxParallelSegmentDownloads;
  }
}
