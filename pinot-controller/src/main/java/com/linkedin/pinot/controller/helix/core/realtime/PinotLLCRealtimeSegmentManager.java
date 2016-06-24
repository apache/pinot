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

package com.linkedin.pinot.controller.helix.core.realtime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;


public class PinotLLCRealtimeSegmentManager {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotLLCRealtimeSegmentManager.class);
  private static final String KAFKA_PARTITIONS_PATH = "KAFKA_PARTITIONS";

  private static PinotLLCRealtimeSegmentManager INSTANCE = null;

  private final HelixAdmin _helixAdmin;
  private final HelixManager _helixManager;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final PinotHelixResourceManager _helixResourceManager;
  private final String _clusterName;

  public static synchronized void create(HelixAdmin helixAdmin, HelixManager helixManager, ZkHelixPropertyStore propertyStore, PinotHelixResourceManager helixResourceManager) {
    if (INSTANCE != null) {
      throw new RuntimeException("Instance already created");
    }
    INSTANCE = new PinotLLCRealtimeSegmentManager(helixAdmin, helixManager, propertyStore, helixResourceManager);
  }

  private String makeKafkaPartitionPath(String realtimeTableName) {
    return KAFKA_PARTITIONS_PATH + "/" + realtimeTableName;
  }

  private PinotLLCRealtimeSegmentManager(HelixAdmin helixAdmin, HelixManager helixManager, ZkHelixPropertyStore propertyStore, PinotHelixResourceManager helixResourceManager) {
    _helixAdmin = helixAdmin;
    _helixManager = helixManager;
    _propertyStore = propertyStore;
    _helixResourceManager = helixResourceManager;
    _clusterName = _helixManager.getClusterName();
  }

  public static PinotLLCRealtimeSegmentManager getInstance() {
    if (INSTANCE == null) {
      throw new RuntimeException("Not yet created");
    }
    return INSTANCE;
  }

  /*
   * Use helix balancer to balance the kafka partitions amongst the realtime nodes (for a table).
   * The topic name is being used as a dummy helix resource name. We do not read or write to zk in this
   * method.
   */
  public ZNRecord assignKafkaPartitions(final String topicName, int nPartitions, final List<String> instanceNames, int nReplicas) {
    final String resourceName = topicName;

    List<String> partitions = new ArrayList<>(nPartitions);
    for (int i = 0; i < nPartitions; i++) {
      partitions.add(Integer.toString(i));
    }

    LinkedHashMap<String, Integer> states = new LinkedHashMap<>(2);
    states.put("OFFLINE", 0);
    states.put("ONLINE", nReplicas);

    AutoRebalanceStrategy strategy = new AutoRebalanceStrategy(resourceName, partitions, states);
    ZNRecord znRecord = strategy.computePartitionAssignment(instanceNames, new HashMap<String, Map<String, String>>(0), instanceNames);
    znRecord.setMapFields(new HashMap<String, Map<String, String>>(0));
    return znRecord;
  }

  public void writeKafkaPartitionAssignemnt(final String realtimeTableName, ZNRecord znRecord) {
    final String path = makeKafkaPartitionPath(realtimeTableName);
    _propertyStore.set(path, znRecord, AccessOption.PERSISTENT);
  }
}
