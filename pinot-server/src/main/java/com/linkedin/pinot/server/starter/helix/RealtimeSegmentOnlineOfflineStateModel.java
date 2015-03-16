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
package com.linkedin.pinot.server.starter.helix;

import com.linkedin.pinot.common.utils.ZkUtils;

import org.apache.helix.AccessOption;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.SegmentNameBuilder;


@StateModelInfo(states = "{'OFFLINE','ONLINE', 'DROPPED'}", initialState = "OFFLINE")
public class RealtimeSegmentOnlineOfflineStateModel extends StateModel {
  private static final Logger logger = Logger.getLogger(RealtimeSegmentOnlineOfflineStateModel.class);

  private final String clusterName;

  public RealtimeSegmentOnlineOfflineStateModel(String helixClusterName) {
    this.clusterName = helixClusterName;
  }

  @Transition(from = "OFFLINE", to = "ONLINE")
  public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {

    String segmentId = message.getPartitionName();

    ZkHelixPropertyStore<ZNRecord> propertyStore = ZkUtils.getZkPropertyStore(context.getManager(), clusterName);

    RealtimeSegmentZKMetadata metadata =
        new RealtimeSegmentZKMetadata(propertyStore.get(
            "/" + SegmentNameBuilder.Realtime.extractResourceName(segmentId) + "/" + segmentId, null,
            AccessOption.PERSISTENT));
    /*
     * TODO :
     *
     * 1. extract Resource metadata
     * 2. extract instance metadata
     * 3. extract segment metadata
     * 4. create property store
     *
     * */
  }
}
