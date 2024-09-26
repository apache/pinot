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
package org.apache.pinot.controller.helix.core.realtime;

import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.controller.helix.core.realtime.segment.CommittingSegmentDescriptor;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;


public interface SegmentCompletionFSM {
  boolean isDone();

  SegmentCompletionProtocol.Response segmentConsumed(String instanceId, StreamPartitionMsgOffset offset,
      String stopReason);

  SegmentCompletionProtocol.Response segmentCommitStart(String instanceId, StreamPartitionMsgOffset offset);

  SegmentCompletionProtocol.Response stoppedConsuming(String instanceId, StreamPartitionMsgOffset offset,
      String reason);

  SegmentCompletionProtocol.Response extendBuildTime(String instanceId, StreamPartitionMsgOffset offset,
      int extTimeSec);

  SegmentCompletionProtocol.Response segmentCommitEnd(SegmentCompletionProtocol.Request.Params reqParams,
      boolean success, boolean isSplitCommit, CommittingSegmentDescriptor committingSegmentDescriptor);
}
