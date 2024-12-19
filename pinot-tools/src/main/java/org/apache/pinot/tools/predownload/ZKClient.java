package org.apache.pinot.tools.predownload;

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
  private final String clusterName;
  private final String instanceName;
  private final String zkAddress;

  @SuppressWarnings("NullAway.Init")
  private RealmAwareZkClient zkClient;

  private boolean started;

  public ZKClient(String zkAddress, String clusterName, String instanceName) {
    this.clusterName = clusterName;
    this.instanceName = instanceName;
    this.zkAddress = zkAddress;
    this.started = false;
  }

  public void start() {
    RealmAwareZkClient.RealmAwareZkClientConfig config = new RealmAwareZkClient.RealmAwareZkClientConfig();
    config.setConnectInitTimeout(ZK_CONNECTION_TIMEOUT_MS);
    config.setZkSerializer(new ZNRecordSerializer());
    zkClient = SharedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddress), config.createHelixZkClientConfig());
    zkClient.waitUntilConnected(ZK_CONNECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    started = true;
  }

  public void close() {
    if (zkClient != null) {
      zkClient.close();
      started = false;
    }
  }

  public boolean isStarted() {
    return started;
  }

  public HelixDataAccessor getDataAccessor() {
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(zkClient));
    return accessor;
  }

  public InstanceConfig getInstanceConfig(HelixDataAccessor accessor) {
    String instanceConfigPath = PropertyPathBuilder.instanceConfig(clusterName, instanceName);
    if (!zkClient.exists(instanceConfigPath)) {
      throw new HelixException("instance " + instanceName + " does not exist in cluster " + clusterName);
    }
    org.apache.helix.PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    return accessor.getProperty(keyBuilder.instanceConfig(instanceName));
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
    LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName));
    if (liveInstance == null) {
      String instanceConfigPath = PropertyPathBuilder.instanceConfig(clusterName, instanceName);
      PredownloadCompleteReason reason =
          zkClient.exists(instanceConfigPath) ? PredownloadCompleteReason.INSTANCE_NOT_ALIVE
              : PredownloadCompleteReason.INSTANCE_NON_EXISTENT;
      StatusRecorder.predownloadComplete(reason, clusterName, instanceName, "");
    } else {
      String sessionId = liveInstance.getEphemeralOwner();
      List<CurrentState> instanceCurrentStates =
          accessor.getChildValues(keyBuilder.currentStates(instanceName, sessionId), true);
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
    ZkHelixPropertyStore<ZNRecord> propertyStore = new AutoFallbackPropertyStore<>(new ZkBaseDataAccessor<>(zkClient),
        PropertyPathBuilder.propertyStore(clusterName), String.format("/%s/%s", clusterName, "HELIX_PROPERTYSTORE"));
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
