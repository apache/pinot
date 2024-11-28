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


/**
 * Interface for managing the state machine transitions related to segment completion
 * in a real-time table within Apache Pinot.
 *
 * The segment completion process is crucial for handling real-time ingestion, ensuring
 * that segments are correctly built, committed, or discarded based on the server's state
 * and the protocol defined for low-level consumer (LLC) tables. This interface abstracts
 * the controller-side logic for responding to server events during the lifecycle of a
 * segment's completion.
 */
public interface SegmentCompletionFSM {

  /**
   * Checks whether the segment completion process has completed.
   *
   * The process is considered complete when the segment has been either successfully
   * committed or marked as aborted. This method helps determine if the FSM can be
   * removed from memory.
   *
   * @return {@code true} if the FSM has reached a terminal state, {@code false} otherwise.
   */
  boolean isDone();

  /**
   * Processes the event where a server indicates it has consumed up to a specified offset.
   *
   * This is typically triggered when a server finishes consuming data for a segment due
   * to reaching a row limit, an end-of-partition signal, or another stopping condition.
   * The FSM evaluates the reported offset and determines the next state or action for
   * the server, such as holding, catching up, or committing the segment.
   *
   * @param instanceId The ID of the server instance reporting consumption.
   * @param offset The offset up to which the server has consumed.
   * @param stopReason The reason the server stopped consuming (e.g., row limit or end of partition).
   * @return A response indicating the next action for the server (e.g., HOLD, CATCHUP, or COMMIT).
   */
  SegmentCompletionProtocol.Response segmentConsumed(String instanceId, StreamPartitionMsgOffset offset,
      String stopReason);

  /**
   * Processes the start of a segment commit from a server.
   *
   * This occurs when a server signals its intention to commit a segment it has built.
   * The FSM verifies whether the server is eligible to commit based on its previous
   * state and the reported offset, and transitions to a committing state if appropriate.
   *
   * @param instanceId The ID of the server instance attempting to commit.
   * @param offset The offset being committed by the server.
   * @return A response indicating the next action for the server (e.g., CONTINUE or FAILED).
   */
  SegmentCompletionProtocol.Response segmentCommitStart(String instanceId, StreamPartitionMsgOffset offset);

  /**
   * Handles the event where a server indicates it has stopped consuming.
   *
   * This is triggered when a server cannot continue consuming for a segment, potentially
   * due to resource constraints, errors, or a manual stop. The FSM updates its state
   * and determines whether the server can participate in subsequent actions for the segment.
   *
   * @param instanceId The ID of the server instance reporting the stopped consumption.
   * @param offset The offset at which the server stopped consuming.
   * @param reason The reason for stopping consumption (e.g., resource constraints or errors).
   * @return A response indicating the next action for the server (e.g., PROCESSED or FAILED).
   */
  SegmentCompletionProtocol.Response stoppedConsuming(String instanceId, StreamPartitionMsgOffset offset,
      String reason);

  /**
   * Handles a request to extend the time allowed for segment building.
   *
   * If a server requires more time to build a segment, it can request an extension.
   * The FSM evaluates the request in the context of the current state and the protocol's
   * constraints, and either grants or denies the extension.
   *
   * @param instanceId The ID of the server instance requesting an extension.
   * @param offset The offset at which the server is currently consuming.
   * @param extTimeSec The additional time (in seconds) requested for segment building.
   * @return A response indicating whether the extension was accepted or denied.
   */
  SegmentCompletionProtocol.Response extendBuildTime(String instanceId, StreamPartitionMsgOffset offset,
      int extTimeSec);

  /**
   * Processes the end of a segment commit from a server.
   *
   * This method is triggered when a server has completed uploading the segment and
   * signals the end of the commit process. The FSM validates the commit, updates metadata,
   * and finalizes the segment's state. Depending on the outcome, the segment is either
   * successfully committed or the FSM transitions to an error state.
   *
   * @param reqParams The parameters of the commit request.
   * @param success {@code true} if the commit was successful, {@code false} otherwise.
   * @param isSplitCommit {@code true} if the commit uses split commit protocol, {@code false} otherwise.
   * @param committingSegmentDescriptor Metadata about the segment being committed.
   * @return A response indicating whether the commit was successful or failed.
   */
  SegmentCompletionProtocol.Response segmentCommitEnd(SegmentCompletionProtocol.Request.Params reqParams,
      boolean success, boolean isSplitCommit, CommittingSegmentDescriptor committingSegmentDescriptor);
}
