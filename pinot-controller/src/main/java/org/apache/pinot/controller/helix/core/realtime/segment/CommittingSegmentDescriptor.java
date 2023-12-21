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
package org.apache.pinot.controller.helix.core.realtime.segment;

import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;


/**
 * Class to hold properties of the committing segment
 */
public class CommittingSegmentDescriptor {
  private String _segmentName;
  private long _segmentSizeBytes;
  private String _segmentLocation;
  private String _nextOffset;
  private SegmentMetadataImpl _segmentMetadata;
  private String _stopReason;

  public static CommittingSegmentDescriptor fromSegmentCompletionReqParams(
      SegmentCompletionProtocol.Request.Params reqParams) {
    CommittingSegmentDescriptor committingSegmentDescriptor =
        new CommittingSegmentDescriptor(reqParams.getSegmentName(), reqParams.getStreamPartitionMsgOffset(),
            reqParams.getSegmentSizeBytes());
    committingSegmentDescriptor.setSegmentLocation(reqParams.getSegmentLocation());
    committingSegmentDescriptor.setStopReason(reqParams.getReason());
    return committingSegmentDescriptor;
  }

  public static CommittingSegmentDescriptor fromSegmentCompletionReqParamsAndMetadata(
      SegmentCompletionProtocol.Request.Params reqParams, SegmentMetadataImpl metadata) {
    CommittingSegmentDescriptor committingSegmentDescriptor = fromSegmentCompletionReqParams(reqParams);
    committingSegmentDescriptor.setSegmentMetadata(metadata);
    return committingSegmentDescriptor;
  }

  public CommittingSegmentDescriptor(String segmentName, String nextOffset, long segmentSizeBytes) {
    _segmentName = segmentName;
    _nextOffset = nextOffset;
    _segmentSizeBytes = segmentSizeBytes;
  }

  public CommittingSegmentDescriptor(String segmentName, String nextOffset, long segmentSizeBytes,
      String segmentLocation) {
    this(segmentName, nextOffset, segmentSizeBytes);
    _segmentLocation = segmentLocation;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public void setSegmentName(String segmentName) {
    _segmentName = segmentName;
  }

  public long getSegmentSizeBytes() {
    return _segmentSizeBytes;
  }

  public void setSegmentSizeBytes(long segmentSizeBytes) {
    _segmentSizeBytes = segmentSizeBytes;
  }

  public String getSegmentLocation() {
    return _segmentLocation;
  }

  public void setSegmentLocation(String segmentLocation) {
    _segmentLocation = segmentLocation;
  }

  public String getNextOffset() {
    return _nextOffset;
  }

  public SegmentMetadataImpl getSegmentMetadata() {
    return _segmentMetadata;
  }

  public void setSegmentMetadata(SegmentMetadataImpl segmentMetadata) {
    _segmentMetadata = segmentMetadata;
  }

  public String getStopReason() {
    return _stopReason;
  }

  public void setStopReason(String stopReason) {
    _stopReason = stopReason;
  }
}
