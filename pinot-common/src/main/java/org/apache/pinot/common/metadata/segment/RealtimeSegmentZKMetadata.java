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
package org.apache.pinot.common.metadata.segment;

import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.CommonConstants.Segment.SegmentType;

import static org.apache.pinot.spi.utils.EqualityUtils.hashCodeOf;
import static org.apache.pinot.spi.utils.EqualityUtils.isEqual;
import static org.apache.pinot.spi.utils.EqualityUtils.isNullOrNotSameClass;
import static org.apache.pinot.spi.utils.EqualityUtils.isSameReference;


public class RealtimeSegmentZKMetadata extends SegmentZKMetadata {

  private Status _status = null;
  private int _sizeThresholdToFlushSegment = -1;
  private String _timeThresholdToFlushSegment = null; // store as period string for readability

  public RealtimeSegmentZKMetadata() {
    setSegmentType(SegmentType.REALTIME);
  }

  public RealtimeSegmentZKMetadata(ZNRecord znRecord) {
    super(znRecord);
    setSegmentType(SegmentType.REALTIME);
    _status = Status.valueOf(znRecord.getSimpleField(CommonConstants.Segment.Realtime.STATUS));
    _sizeThresholdToFlushSegment = znRecord.getIntField(CommonConstants.Segment.FLUSH_THRESHOLD_SIZE, -1);
    String flushThresholdTime = znRecord.getSimpleField(CommonConstants.Segment.FLUSH_THRESHOLD_TIME);
    if (flushThresholdTime != null && !flushThresholdTime.equals(NULL)) {
      _timeThresholdToFlushSegment = znRecord.getSimpleField(CommonConstants.Segment.FLUSH_THRESHOLD_TIME);
    }
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    String newline = "\n";
    result.append(this.getClass().getName());
    result.append(" Object {");
    result.append(newline);
    result.append("  " + super.getClass().getName() + " : " + super.toString());
    result.append(newline);
    result.append("  " + CommonConstants.Segment.Realtime.STATUS + " : " + _status);
    result.append(newline);
    result.append("}");
    return result.toString();
  }

  @Override
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = super.toZNRecord();
    znRecord.setSimpleField(CommonConstants.Segment.Realtime.STATUS, _status.toString());
    znRecord.setLongField(CommonConstants.Segment.FLUSH_THRESHOLD_SIZE, _sizeThresholdToFlushSegment);
    znRecord.setSimpleField(CommonConstants.Segment.FLUSH_THRESHOLD_TIME, _timeThresholdToFlushSegment);
    return znRecord;
  }

  @Override
  public boolean equals(Object segmentMetadata) {
    if (isSameReference(this, segmentMetadata)) {
      return true;
    }

    if (isNullOrNotSameClass(this, segmentMetadata)) {
      return false;
    }

    RealtimeSegmentZKMetadata metadata = (RealtimeSegmentZKMetadata) segmentMetadata;
    return super.equals(metadata) && isEqual(_status, metadata._status)
        && isEqual(_sizeThresholdToFlushSegment, metadata._sizeThresholdToFlushSegment)
        && isEqual(_timeThresholdToFlushSegment, metadata._timeThresholdToFlushSegment);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = hashCodeOf(result, _status);
    result = hashCodeOf(result, _sizeThresholdToFlushSegment);
    result = hashCodeOf(result, _timeThresholdToFlushSegment);
    return result;
  }

  @Override
  public Map<String, String> toMap() {
    Map<String, String> configMap = super.toMap();
    configMap.put(CommonConstants.Segment.Realtime.STATUS, _status.toString());
    configMap.put(CommonConstants.Segment.SEGMENT_TYPE, SegmentType.REALTIME.toString());
    configMap.put(CommonConstants.Segment.FLUSH_THRESHOLD_SIZE, Integer.toString(_sizeThresholdToFlushSegment));
    configMap.put(CommonConstants.Segment.FLUSH_THRESHOLD_TIME, _timeThresholdToFlushSegment);
    return configMap;
  }

  public Status getStatus() {
    return _status;
  }

  public void setStatus(Status status) {
    _status = status;
  }

  public void setSizeThresholdToFlushSegment(int sizeThresholdToFlushSegment) {
    _sizeThresholdToFlushSegment = sizeThresholdToFlushSegment;
  }

  public int getSizeThresholdToFlushSegment() {
    return _sizeThresholdToFlushSegment;
  }

  /**
   * Gets the time threshold as a  period string
   * @return
   */
  public String getTimeThresholdToFlushSegment() {
    return _timeThresholdToFlushSegment;
  }

  /**
   * Sets the time threshold to the given period string
   * @param timeThresholdPeriodString
   */
  public void setTimeThresholdToFlushSegment(String timeThresholdPeriodString) {
    _timeThresholdToFlushSegment = timeThresholdPeriodString;
  }
}
