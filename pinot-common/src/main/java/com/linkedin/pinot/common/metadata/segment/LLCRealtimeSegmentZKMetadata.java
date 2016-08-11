/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.common.metadata.segment;

import java.util.Map;
import org.apache.helix.ZNRecord;
import static com.linkedin.pinot.common.utils.EqualityUtils.hashCodeOf;
import static com.linkedin.pinot.common.utils.EqualityUtils.isEqual;
import static com.linkedin.pinot.common.utils.EqualityUtils.isNullOrNotSameClass;
import static com.linkedin.pinot.common.utils.EqualityUtils.isSameReference;


public class LLCRealtimeSegmentZKMetadata extends RealtimeSegmentZKMetadata {
  private static final String START_OFFSET = "segment.realtime.startOffset";
  private static final String END_OFFSET = "segment.realtime.endOffset";
  private static final String NUM_REPLICAS = "segment.realtime.numReplicas";
  public static final String DOWNLOAD_URL = "segment.realtime.download.url";

  private long _startOffset;
  private long _endOffset;
  private int _numReplicas;

  private String _downloadUrl = null;

  public LLCRealtimeSegmentZKMetadata() {
    super();
  }

  public LLCRealtimeSegmentZKMetadata(ZNRecord znRecord) {
    super(znRecord);
    _startOffset = Long.valueOf(znRecord.getSimpleField(START_OFFSET));
    _numReplicas = Integer.valueOf(znRecord.getSimpleField(NUM_REPLICAS));
    _endOffset = Long.valueOf(znRecord.getSimpleField(END_OFFSET));
    _downloadUrl = znRecord.getSimpleField(DOWNLOAD_URL);
  }

  public long getStartOffset() {
    return _startOffset;
  }

  public long getEndOffset() {
    return _endOffset;
  }

  public int getNumReplicas() {
    return _numReplicas;
  }

  public void setStartOffset(long startOffset) {
    _startOffset = startOffset;
  }

  public void setEndOffset(long endOffset) {
    _endOffset = endOffset;
  }

  public void setNumReplicas(int numReplicas) {
    _numReplicas = numReplicas;
  }

  public String getDownloadUrl() {
    return _downloadUrl;
  }

  public void setDownloadUrl(String downloadUrl) {
    _downloadUrl = downloadUrl;
  }

  @Override
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = super.toZNRecord();
    znRecord.setLongField(START_OFFSET, _startOffset);
    znRecord.setLongField(END_OFFSET, _endOffset);
    znRecord.setIntField(NUM_REPLICAS, _numReplicas);
    znRecord.setSimpleField(DOWNLOAD_URL, _downloadUrl);
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
    result.append("  " + START_OFFSET + " : " + _startOffset + ",");
    result.append(newline);
    result.append("  " + DOWNLOAD_URL + " : " + _downloadUrl + ",");
    result.append(newline);
    result.append("  " + END_OFFSET + " : " + _endOffset);
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

    LLCRealtimeSegmentZKMetadata metadata = (LLCRealtimeSegmentZKMetadata) segmentMetadata;
    return super.equals(metadata) &&
        isEqual(_startOffset, metadata._startOffset) &&
        isEqual(_endOffset, metadata._endOffset) &&
        isEqual(_downloadUrl, metadata._downloadUrl) &&
        isEqual(_numReplicas, metadata._numReplicas);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result =  hashCodeOf(result, _startOffset);
    result =  hashCodeOf(result, _endOffset);
    result =  hashCodeOf(result, _numReplicas);
    result = hashCodeOf(result, _downloadUrl);
    return result;
  }

  @Override
  public Map<String, String> toMap() {
    Map<String, String> configMap = super.toMap();
    configMap.put(START_OFFSET, Long.toString(_startOffset));
    configMap.put(END_OFFSET, Long.toString(_endOffset));
    configMap.put(NUM_REPLICAS, Integer.toString(_numReplicas));
    configMap.put(DOWNLOAD_URL, _downloadUrl);

    return configMap;
  }
}
