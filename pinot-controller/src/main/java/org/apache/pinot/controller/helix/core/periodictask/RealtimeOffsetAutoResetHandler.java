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
package org.apache.pinot.controller.helix.core.periodictask;

import java.util.Collection;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.stream.StreamConfig;


public interface RealtimeOffsetAutoResetHandler {

  /**
   * Initialize the handler with the PinotLLCRealtimeSegmentManager and PinotHelixResourceManager.
   * This is called once in constructor.
   */
  public abstract void init(
      PinotLLCRealtimeSegmentManager llcRealtimeSegmentManager, PinotHelixResourceManager pinotHelixResourceManager);

  /**
   * Trigger the job to backfill the skipped interval due to offset auto reset.
   * It is expected to backfill the [fromOffset, toOffset) interval.
   * @return if successfully started the backfill job
   */
  public abstract boolean triggerBackfillJob(
      String tableNameWithType, StreamConfig streamConfig, String topicName, int partitionId, long fromOffset,
      long toOffset);

  /**
   * Ensure all topics under the table are being backfilled. It is the caller's responsibility to figure out what
   * topic set it should check.
   */
  public abstract void ensureBackfillJobsRunning(String tableNameWithType, Collection<String> topicNames);

  /**
   * Return the Collection of completed and cleaned up topicNames from the input.
   */
  public abstract Collection<String> cleanupCompletedBackfillJobs(
      String tableNameWithType, Collection<String> topicNames);

  public abstract void close();
}
