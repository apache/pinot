/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import org.apache.helix.ZNRecord;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;


public class RealtimeSegmentZKMetadata extends SegmentZKMetadata {

  private Status _status = null;

  public RealtimeSegmentZKMetadata() {
    setSegmentType(SegmentType.REALTIME);
  }

  public RealtimeSegmentZKMetadata(ZNRecord znRecord) {
    super(znRecord);
    setSegmentType(SegmentType.REALTIME);
    _status = Status.valueOf(znRecord.getSimpleField(CommonConstants.Segment.Realtime.STATUS));
  }

  public Status getStatus() {
    return _status;
  }

  public void setStatus(Status status) {
    _status = status;
  }

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

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = super.toZNRecord();
    znRecord.setSimpleField(CommonConstants.Segment.Realtime.STATUS, _status.toString());
    return znRecord;
  }

  public boolean equals(RealtimeSegmentZKMetadata segmentMetadata) {
    if (!super.equals(segmentMetadata)) {
      return false;
    }
    if (getStatus() != segmentMetadata.getStatus()) {
      return false;
    }
    return true;
  }
}
