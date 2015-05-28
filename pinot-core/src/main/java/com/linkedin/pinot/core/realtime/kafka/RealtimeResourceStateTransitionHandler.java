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
package com.linkedin.pinot.core.realtime.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;

import com.linkedin.pinot.core.realtime.RealtimeIndexingConfig;

/**
 * Handles the state transition for realtime segments Overall flow <br>
 * Read the metadata for this segment, start a listener on the segment metadata. <br>
 * the metadata must have the start offset <br>
 * start the consumer for that particular offset <br>
 * when the per segment pre set threshold(index size,count, time) is reached, <br>
 * pause the consumption and update the offset in the segment metadata -- <br>
 * the controller will compare the offsets among all the replicas and chose the
 * max offset as the end of this segment <br>
 * it writes the end offset to segment metadata <br>
 * the replicas listen to this update and consumes from Kafka until that offset,
 * flushes the segment to disk and reload the segment from disk (note on disk
 * format is different from in memory format) and frees up the memory <br>
 */
public class RealtimeResourceStateTransitionHandler {
  /**
   * maintains one realtime segment manager per segment
   */
  Map<String, KafkaRealtimeSegmentDataManager> realtimeSegmentManagerMap;
  private HelixManager manager;
  private HelixAdmin helixAdmin;

  public RealtimeResourceStateTransitionHandler(HelixManager manager) {
    this.manager = manager;
    helixAdmin = manager.getClusterManagmentTool();
    realtimeSegmentManagerMap = new HashMap<String, KafkaRealtimeSegmentDataManager>();
  }

  /**
   * @param resourceName
   * @param segmentName
   */
  public void goOnline(String resourceName, String segmentName) {
    KafkaRealtimeSegmentDataManager manager = createManagerIfAbsent(resourceName, segmentName);
    RealtimeIndexingConfig realtimeIndexingConfig =
        getRealtimeIndexingConfig(resourceName, segmentName);
    manager.init(realtimeIndexingConfig);
    manager.start();
  }

  public void goOffline(String resourceName, String segmentName) {
    KafkaRealtimeSegmentDataManager manager = createManagerIfAbsent(resourceName, segmentName);
    RealtimeIndexingConfig realtimeIndexingConfig =
        getRealtimeIndexingConfig(resourceName, segmentName);
    manager.init(realtimeIndexingConfig);
    manager.shutdown();

  }

  private RealtimeIndexingConfig getRealtimeIndexingConfig(String resourceName, String segmentName) {

    return null;
  }

  private KafkaRealtimeSegmentDataManager createManagerIfAbsent(String resourceName,
      String segmentName) {
    return null;
  }
}
