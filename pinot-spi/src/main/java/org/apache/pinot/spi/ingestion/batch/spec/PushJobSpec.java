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
package org.apache.pinot.spi.ingestion.batch.spec;

import java.io.Serializable;


/**
 * PushJobSpec defines segment push job related configuration
 */
public class PushJobSpec implements Serializable {

  /**
   * number of attempts for push job, default is 1, which means no retry.
   */
  private int _pushAttempts = 1;

  /**
   * push job parallelism, default is 1.
   */
  private int _pushParallelism = 1;

  /**
   * retry wait Ms, default to 1 second.
   */
  private long _pushRetryIntervalMillis = 1000;

  /**
   * Used in SegmentUriPushJobRunner, which is used to composite the segment uri to send to pinot controller.
   * The URI sends to controller is in the format ${segmentUriPrefix}${segmentPath}${segmentUriSuffix}
   */
  private String _segmentUriPrefix;
  private String _segmentUriSuffix;

  public String getSegmentUriPrefix() {
    return _segmentUriPrefix;
  }

  /**
   * Used in SegmentUriPushJobRunner, which is used to composite the segment uri to send to pinot controller.
   * The URI sends to controller is in the format ${segmentUriPrefix}${segmentPath}${segmentUriSuffix}
   * @param segmentUriPrefix
   */
  public void setSegmentUriPrefix(String segmentUriPrefix) {
    _segmentUriPrefix = segmentUriPrefix;
  }

  public String getSegmentUriSuffix() {
    return _segmentUriSuffix;
  }

  /**
   * Used in SegmentUriPushJobRunner, which is used to composite the segment uri to send to pinot controller.
   * The URI sends to controller is in the format ${segmentUriPrefix}${segmentPath}${segmentUriSuffix}
   * @param segmentUriSuffix
   */
  public void setSegmentUriSuffix(String segmentUriSuffix) {
    _segmentUriSuffix = segmentUriSuffix;
  }

  public int getPushAttempts() {
    return _pushAttempts;
  }

  /**
   * number of attempts for push job, default is 1, which means no retry.
   * @param pushAttempts
   */
  public void setPushAttempts(int pushAttempts) {
    _pushAttempts = pushAttempts;
  }

  public long getPushRetryIntervalMillis() {
    return _pushRetryIntervalMillis;
  }

  /**
   * retry wait Ms, default to 1 second.
   * @param pushRetryIntervalMillis
   */
  public void setPushRetryIntervalMillis(long pushRetryIntervalMillis) {
    _pushRetryIntervalMillis = pushRetryIntervalMillis;
  }

  public int getPushParallelism() {
    return _pushParallelism;
  }

  public void setPushParallelism(int pushParallelism) {
    _pushParallelism = pushParallelism;
  }
}
