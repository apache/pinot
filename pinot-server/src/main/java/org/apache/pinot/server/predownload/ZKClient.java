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
package org.apache.pinot.server.predownload;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.store.zk.AutoFallbackPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is to manage the ZK connection and operations. It will be used to fetch the segment
 * metadata from ZK and prepare for the downloading.
 */
public class ZKClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZKClient.class);
  private static final long ZK_CONNECTION_TIMEOUT_MS = 30000L;
  private final String _clusterName;
  private final String _instanceName;
  private final String _zkAddress;

  @SuppressWarnings("NullAway.Init")
  private RealmAwareZkClient _zkClient;

  private boolean _started;

  public ZKClient(String zkAddress, String clusterName, String instanceName) {
    _clusterName = clusterName;
    _instanceName = instanceName;
    _zkAddress = zkAddress;
    _started = false;
  }

  public void start() {
    RealmAwareZkClient.RealmAwareZkClientConfig config = new RealmAwareZkClient.RealmAwareZkClientConfig();
    config.setConnectInitTimeout(ZK_CONNECTION_TIMEOUT_MS);
    config.setZkSerializer(new ZNRecordSerializer());
    _zkClient = SharedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(_zkAddress), config.createHelixZkClientConfig());
    _zkClient.waitUntilConnected(ZK_CONNECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    _started = true;
  }

  public void close() {
    if (_zkClient != null) {
      _zkClient.close();
      _started = false;
    }
  }

  public boolean isStarted() {
    return _started;
  }

  public HelixDataAccessor getDataAccessor() {
    HelixDataAccessor accessor = new ZKHelixDataAccessor(_clusterName, new ZkBaseDataAccessor(_zkClient));
    return accessor;
  }

  public InstanceConfig getInstanceConfig(HelixDataAccessor accessor) {
    String instanceConfigPath = PropertyPathBuilder.instanceConfig(_clusterName, _instanceName);
    if (!_zkClient.exists(instanceConfigPath)) {
      throw new HelixException("instance " + _instanceName + " does not exist in cluster " + _clusterName);
    }
    org.apache.helix.PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    return accessor.getProperty(keyBuilder.instanceConfig(_instanceName));
  }

  /**
   * Get the segments of the instance from ZK. Only fetch segments that are in ONLINE state from
   * instance current state.
   *
   * @return List of segment info with table name and segment name.
   */
  public List<SegmentInfo> getSegmentsOfInstance(HelixDataAccessor accessor) {
    List<SegmentInfo> segmentInfos = new ArrayList<>();
    org.apache.helix.PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(_instanceName));
    if (liveInstance == null) {
      String instanceConfigPath = PropertyPathBuilder.instanceConfig(_clusterName, _instanceName);
      PredownloadCompleteReason reason =
          _zkClient.exists(instanceConfigPath) ? PredownloadCompleteReason.INSTANCE_NOT_ALIVE
              : PredownloadCompleteReason.INSTANCE_NON_EXISTENT;
      StatusRecorder.predownloadComplete(reason, _clusterName, _instanceName, "");
    } else {
      String sessionId = liveInstance.getEphemeralOwner();
      List<CurrentState> instanceCurrentStates =
          accessor.getChildValues(keyBuilder.currentStates(_instanceName, sessionId), true);
      if (instanceCurrentStates == null || instanceCurrentStates.isEmpty()) {
        return segmentInfos;
      }
      for (CurrentState currentState : instanceCurrentStates) {
        String tableNameWithType = currentState.getResourceName();
        Map<String, String> partitionStateMap = currentState.getPartitionStateMap();
        for (Map.Entry<String, String> entry : partitionStateMap.entrySet()) {
          String segmentName = entry.getKey();
          String state = entry.getValue();
          if (state.equals(CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE)) {
            segmentInfos.add(new SegmentInfo(tableNameWithType, segmentName));
            LOGGER.info("Added segment {} to the predownload list", segmentName);
          } else {
            LOGGER.info("Not add segment {} to the predownload list because it is in state {}", segmentName, state);
          }
        }
      }
    }
    return segmentInfos;
  }

  /**
   * Update the segment deepstore metadata from ZK.
   *
   * @param segmentInfoList List of segment info with table name and segment name.
   * @param tableInfoMap Map of table name to table info to be filled with table config.
   */
  public void updateSegmentMetadata(List<SegmentInfo> segmentInfoList, Map<String, TableInfo> tableInfoMap,
      InstanceDataManagerConfig instanceDataManagerConfig) {
    // fallback path comes from ZKHelixManager.class getHelixPropertyStore method
    ZkHelixPropertyStore<ZNRecord> propertyStore = new AutoFallbackPropertyStore<>(new ZkBaseDataAccessor<>(_zkClient),
        PropertyPathBuilder.propertyStore(_clusterName), String.format("/%s/%s", _clusterName, "HELIX_PROPERTYSTORE"));
    for (SegmentInfo segmentInfo : segmentInfoList) {
      tableInfoMap.computeIfAbsent(segmentInfo.getTableNameWithType(), name -> {
        TableConfig tableConfig = ZKMetadataProvider.getTableConfig(propertyStore, segmentInfo.getTableNameWithType());
        if (tableConfig == null) {
          LOGGER.warn("Cannot predownload segment {} because not able to get its table config from ZK",
              segmentInfo.getSegmentName());
          return null;
        }
        Schema schema = ZKMetadataProvider.getTableSchema(propertyStore, tableConfig);
        return new TableInfo(name, tableConfig, schema, instanceDataManagerConfig);
      });
      SegmentZKMetadata segmentZKMetadata =
          ZKMetadataProvider.getSegmentZKMetadata(propertyStore, segmentInfo.getTableNameWithType(),
              segmentInfo.getSegmentName());
      if (segmentZKMetadata == null) {
        LOGGER.warn("Cannot predownload segment {} because not able to get its metadata from ZK",
            segmentInfo.getSegmentName());
        continue;
      }
      segmentInfo.updateSegmentInfo(segmentZKMetadata);
      LOGGER.info("Update segment {} with download path {} and CRC number {}", segmentInfo.getSegmentName(),
          segmentInfo.getDownloadUrl(), segmentInfo.getCrc());
    }
  }
}
