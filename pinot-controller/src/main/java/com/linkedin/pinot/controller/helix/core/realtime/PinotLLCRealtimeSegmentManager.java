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
import java.util.List;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Function;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;


public class PinotLLCRealtimeSegmentManager {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotLLCRealtimeSegmentManager.class);
  private static final String KAFKA_PARTITIONS_PATH = "KAFKA_PARTITIONS";

  private static PinotLLCRealtimeSegmentManager INSTANCE = null;

  private final HelixAdmin _helixAdmin;
  private final HelixManager _helixManager;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final PinotHelixResourceManager _helixResourceManager;
  private final String _clusterName;

  public static synchronized void create(HelixAdmin helixAdmin, String clusterName, HelixManager helixManager, ZkHelixPropertyStore propertyStore, PinotHelixResourceManager helixResourceManager) {
    if (INSTANCE != null) {
      throw new RuntimeException("Instance already created");
    }
    INSTANCE = new PinotLLCRealtimeSegmentManager(helixAdmin, clusterName, helixManager, propertyStore, helixResourceManager);
  }

  private String makeKafkaPartitionPath(String realtimeTableName) {
    return KAFKA_PARTITIONS_PATH + "/" + realtimeTableName;
  }

  protected PinotLLCRealtimeSegmentManager(HelixAdmin helixAdmin, String clusterName, HelixManager helixManager, ZkHelixPropertyStore propertyStore, PinotHelixResourceManager helixResourceManager) {
    _helixAdmin = helixAdmin;
    _helixManager = helixManager;
    _propertyStore = propertyStore;
    _helixResourceManager = helixResourceManager;
    _clusterName = clusterName;
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
  public void setupHelixEntries(final String topicName, final String realtimeTableName, int nPartitions,
      final List<String> instanceNames, int nReplicas, long startOffset) {
    if (nReplicas > instanceNames.size()) {
      throw new RuntimeException("Replicas requested(" + nReplicas + ") cannot fit within number of instances(" +
          instanceNames.size() + ") for table " + realtimeTableName + " topic " + topicName);
    }
    /*
     Cannot do this because of https://issues.apache.org/jira/browse/HELIX-631
    {
      final String resourceName = topicName;

      List<String> partitions = new ArrayList<>(nPartitions);
      for (int i = 0; i < nPartitions; i++) {
        partitions.add(Integer.toString(i));
      }

      LinkedHashMap<String, Integer> states = new LinkedHashMap<>(2);
      states.put("OFFLINE", 0);
      states.put("ONLINE", nReplicas);

      AutoRebalanceStrategy strategy = new AutoRebalanceStrategy(resourceName, partitions, states);
      znRecord = strategy.computePartitionAssignment(instanceNames, new HashMap<String, Map<String, String>>(0), instanceNames);
      znRecord.setMapFields(new HashMap<String, Map<String, String>>(0));
    }
    */
    ZNRecord znRecord = new ZNRecord(topicName);
    int serverId = 0;
    for (int p = 0; p < nPartitions; p++) {
      List<String> instances = new ArrayList<>(nReplicas);
      for (int r = 0; r < nReplicas; r++) {
        instances.add(instanceNames.get(serverId++));
        if (serverId == instanceNames.size()) {
          serverId = 0;
        }
      }
      znRecord.setListField(Integer.toString(p), instances);
    }
    writeKafkaPartitionAssignemnt(realtimeTableName, znRecord);
    setupInitialSegments(realtimeTableName, znRecord, startOffset);
  }

  protected void writeKafkaPartitionAssignemnt(final String realtimeTableName, ZNRecord znRecord) {
    final String path = makeKafkaPartitionPath(realtimeTableName);
    _propertyStore.set(path, znRecord, AccessOption.PERSISTENT);
  }

  protected void setupInitialSegments(String realtimeTableName, ZNRecord partitionAssignment, long startOffset) {
    // Map of segment names to the server-instances that hold the segment.
    final Map<String, List<String>> idealStateEntries = new HashMap<String, List<String>>(4);
    final Map<String, List<String>> partitionMap = partitionAssignment.getListFields();
    final int nPartitions = partitionMap.size();

    // Create one segment entry in PROPERTYSTORE for each kafka partition.
    // Any of these may already be there, so bail out clean if they are already present.
    List<String> paths = new ArrayList<>(nPartitions);
    List<ZNRecord> records = new ArrayList<>(nPartitions);
    final long now = System.currentTimeMillis();
    final int seqNum = 0; // Initial seq number for the segments
    for (int i = 0; i < nPartitions; i++) {
      final List instances = partitionMap.get(Integer.toString(i));
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);
      LLCSegmentName llcSegmentName = new LLCSegmentName(rawTableName, i, seqNum, now);
      final String segName = llcSegmentName.getSegmentName();

      metadata.setCreationTime(now);
      metadata.setStartOffset(startOffset);
      metadata.setNumReplicas(instances.size());
      metadata.setTableName(realtimeTableName);
      metadata.setSegmentName(segName);
      metadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);

      ZNRecord record = metadata.toZNRecord();
      final String znodePath = PinotRealtimeSegmentManager.getSegmentsPath() + "/" + realtimeTableName + "/" + segName;
      paths.add(znodePath);
      records.add(record);
      idealStateEntries.put(segName, instances);
    }

    updateHelixIdealState(realtimeTableName, idealStateEntries, paths, records);
  }

  // Marking this method protected so it is easier to test
  protected void updateHelixIdealState(String realtimeTableName, final Map<String, List<String>> idealStateEntries,
      List<String> paths, List<ZNRecord> records) {
    _propertyStore.createChildren(paths, records, AccessOption.PERSISTENT);
    LOGGER.info("Added {} segments to propertyStore for table {}", paths.size(), realtimeTableName);

    HelixHelper.updateIdealState(_helixManager, realtimeTableName, new Function<IdealState, IdealState>() {
      @Override
      public IdealState apply(IdealState idealState) {
        return addOrUpdateRealtimeSegmentInIdealState(idealState, idealStateEntries);
      }
    }, RetryPolicies.exponentialBackoffRetryPolicy(5, 500L, 2.0f));
    LOGGER.info("Updated IDEALSTATE for {} segments for table {}", paths.size(), realtimeTableName);
  }

  private static IdealState addOrUpdateRealtimeSegmentInIdealState(IdealState state, Map<String, List<String>> isEntryMap) {
    for (Map.Entry<String, List<String>> entry : isEntryMap.entrySet()) {
      final String segmentId = entry.getKey();
      final Map<String, String> stateMap = state.getInstanceStateMap(segmentId);
      // if the segment name already exists, clear it.
      if (stateMap != null) {
        stateMap.clear();
      }
      for (String instanceName : entry.getValue()) {
        state.setPartitionState(segmentId, instanceName, PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE);
      }
    }
    return state;
  }
}
