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
import org.apache.pinot.spi.utils.CommonConstants.Segment.SegmentType;

import static org.apache.pinot.spi.utils.EqualityUtils.hashCodeOf;
import static org.apache.pinot.spi.utils.EqualityUtils.isEqual;
import static org.apache.pinot.spi.utils.EqualityUtils.isNullOrNotSameClass;
import static org.apache.pinot.spi.utils.EqualityUtils.isSameReference;


public class OfflineSegmentZKMetadata extends SegmentZKMetadata {

  private String _downloadUrl = null;
  private long _pushTime = Long.MIN_VALUE;
  private long _refreshTime = Long.MIN_VALUE;

  public OfflineSegmentZKMetadata() {
    setSegmentType(SegmentType.OFFLINE);
  }

  public OfflineSegmentZKMetadata(ZNRecord znRecord) {
    super(znRecord);
    setSegmentType(SegmentType.OFFLINE);
    _downloadUrl = znRecord.getSimpleField(CommonConstants.Segment.Offline.DOWNLOAD_URL);
    _pushTime = znRecord.getLongField(CommonConstants.Segment.Offline.PUSH_TIME, Long.MIN_VALUE);
    _refreshTime = znRecord.getLongField(CommonConstants.Segment.Offline.REFRESH_TIME, Long.MIN_VALUE);
  }

  public String getDownloadUrl() {
    return _downloadUrl;
  }

  public void setDownloadUrl(String downloadUrl) {
    _downloadUrl = downloadUrl;
  }

  public long getPushTime() {
    return _pushTime;
  }

  public void setPushTime(long pushTime) {
    _pushTime = pushTime;
  }

  public long getRefreshTime() {
    return _refreshTime;
  }

  public void setRefreshTime(long currentTimeMillis) {
    _refreshTime = currentTimeMillis;
  }

  @Override
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = super.toZNRecord();
    znRecord.setSimpleField(CommonConstants.Segment.Offline.DOWNLOAD_URL, _downloadUrl);
    znRecord.setLongField(CommonConstants.Segment.Offline.PUSH_TIME, _pushTime);
    znRecord.setLongField(CommonConstants.Segment.Offline.REFRESH_TIME, _refreshTime);
    return znRecord;
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
    result.append("  " + CommonConstants.Segment.Offline.DOWNLOAD_URL + " : " + _downloadUrl);
    result.append(newline);
    result.append("  " + CommonConstants.Segment.Offline.PUSH_TIME + " : " + _pushTime);
    result.append(newline);
    result.append("  " + CommonConstants.Segment.Offline.REFRESH_TIME + " : " + _refreshTime);
    result.append(newline);
    result.append("}");
    return result.toString();
  }

  @Override
  public boolean equals(Object segmentMetadata) {
    if (isSameReference(this, segmentMetadata)) {
      return true;
    }

    if (isNullOrNotSameClass(this, segmentMetadata)) {
      return false;
    }

    OfflineSegmentZKMetadata metadata = (OfflineSegmentZKMetadata) segmentMetadata;
    return super.equals(metadata) && isEqual(_pushTime, metadata._pushTime) && isEqual(_refreshTime,
        metadata._refreshTime) && isEqual(_downloadUrl, metadata._downloadUrl);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = hashCodeOf(result, _downloadUrl);
    result = hashCodeOf(result, _pushTime);
    result = hashCodeOf(result, _refreshTime);
    return result;
  }

  @Override
  public Map<String, String> toMap() {
    Map<String, String> configMap = super.toMap();
    configMap.put(CommonConstants.Segment.Offline.DOWNLOAD_URL, _downloadUrl);
    configMap.put(CommonConstants.Segment.Offline.PUSH_TIME, Long.toString(_pushTime));
    configMap.put(CommonConstants.Segment.Offline.REFRESH_TIME, Long.toString(_refreshTime));
    configMap.put(CommonConstants.Segment.SEGMENT_TYPE, SegmentType.OFFLINE.toString());
    return configMap;
  }
}
