package com.linkedin.pinot.common.metadata;

import java.util.List;

import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.OfflineDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.RealtimeDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.StringUtil;


public class ZKMetadataProvider {
  private static String CONFIG_RESOURCE_PATH = "/CONFIGS/RESOURCE";
  private static String CONFIG_INSTANCE_PATH = "/CONFIGS/PARTICIPANT";

  public static void setRealtimeResourceZKMetadata(RealtimeDataResourceZKMetadata realtimeDataResource, ZkClient zkClient) {
    ZNRecord znRecord = realtimeDataResource.toZNRecord();
    ZKUtil.createOrReplace(zkClient, getResourceConfigZNRecordPath(znRecord.getId()), znRecord, true);
  }

  public static RealtimeDataResourceZKMetadata getRealtimeResourceZKMetadata(ZkClient zkClient, String resourceName) {
    String realtimeResourceName = null;
    if (resourceName.endsWith(CommonConstants.Broker.DataResource.REALTIME_RESOURCE_SUFFIX)) {
      realtimeResourceName = resourceName;
    } else {
      realtimeResourceName = BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName);
    }
    List<ZNRecord> znRecords = ZKUtil.getChildren(zkClient, CONFIG_RESOURCE_PATH);
    for (ZNRecord znRecord : znRecords) {
      if (znRecord.getId().equals(realtimeResourceName)) {
        return new RealtimeDataResourceZKMetadata(znRecord);
      }
    }
    return null;
  }

  public static void setOfflineResourceZKMetadata(OfflineDataResourceZKMetadata offlineDataResource, ZkClient zkClient) {
    ZNRecord znRecord = offlineDataResource.toZNRecord();
    ZKUtil.createOrReplace(zkClient, getResourceConfigZNRecordPath(znRecord.getId()), znRecord, true);
  }

  public static OfflineDataResourceZKMetadata getOfflineResourceZKMetadata(ZkClient zkClient, String resourceName) {
    String offlineResourceName = null;
    if (resourceName.endsWith(CommonConstants.Broker.DataResource.OFFLINE_RESOURCE_SUFFIX)) {
      offlineResourceName = resourceName;
    } else {
      offlineResourceName = BrokerRequestUtils.getOfflineResourceNameForResource(resourceName);
    }
    List<ZNRecord> znRecords = ZKUtil.getChildren(zkClient, CONFIG_RESOURCE_PATH);
    for (ZNRecord znRecord : znRecords) {
      if (znRecord.getId().equals(offlineResourceName)) {
        return new OfflineDataResourceZKMetadata(znRecord);
      }
    }
    return null;
  }

  private static String getResourceConfigZNRecordPath(String resourceName) {
    return CONFIG_RESOURCE_PATH + "/" + resourceName;
  }

  public static void setInstanceZKMetadata(InstanceZKMetadata instanceZKMetadata, ZkClient zkClient) {
    // Always merge configs.
    ZKUtil.createOrUpdate(zkClient, getInstanceConfigZNRecordPath(instanceZKMetadata.getId()), instanceZKMetadata.toZNRecord(), true, true);
  }

  private static String getInstanceConfigZNRecordPath(String instanceName) {
    return CONFIG_INSTANCE_PATH + "/" + instanceName;
  }

  public static InstanceZKMetadata getInstanceZKMetadata(ZkClient zkClient, String instanceId) {
    List<ZNRecord> znRecords = ZKUtil.getChildren(zkClient, CONFIG_INSTANCE_PATH);
    for (ZNRecord znRecord : znRecords) {
      if (znRecord.getId().equals(instanceId)) {
        return new InstanceZKMetadata(znRecord);
      }
    }
    return null;
  }

  public static OfflineSegmentZKMetadata getOfflineSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, String resourceName, String segmentName) {
    if (!resourceName.endsWith(CommonConstants.Broker.DataResource.OFFLINE_RESOURCE_SUFFIX)) {
      resourceName = BrokerRequestUtils.getOfflineResourceNameForResource(resourceName);
    }
    return new OfflineSegmentZKMetadata(propertyStore.get(constructPropertyStorePathForSegment(resourceName, segmentName), null, AccessOption.PERSISTENT));
  }

  public static void setOfflineSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, OfflineSegmentZKMetadata offlineSegmentZKMetadata) {
    propertyStore.set(constructPropertyStorePathForSegment(
        BrokerRequestUtils.getOfflineResourceNameForResource(offlineSegmentZKMetadata.getResourceName()), offlineSegmentZKMetadata.getSegmentName()),
        offlineSegmentZKMetadata.toZNRecord(), AccessOption.PERSISTENT);
  }

  public static RealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, String resourceName, String segmentName) {
    if (!resourceName.endsWith(CommonConstants.Broker.DataResource.REALTIME_RESOURCE_SUFFIX)) {
      resourceName = BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName);
    }
    return new RealtimeSegmentZKMetadata(propertyStore.get(
        constructPropertyStorePathForSegment(resourceName, segmentName), null, AccessOption.PERSISTENT));
  }

  public static void setRealtimeSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, RealtimeSegmentZKMetadata realtimeSegmentZKMetadata) {
    propertyStore.set(constructPropertyStorePathForSegment(
        BrokerRequestUtils.getRealtimeResourceNameForResource(realtimeSegmentZKMetadata.getResourceName()), realtimeSegmentZKMetadata.getSegmentName()),
        realtimeSegmentZKMetadata.toZNRecord(), AccessOption.PERSISTENT);
  }

  private static String constructPropertyStorePathForSegment(String resourceName, String segmentName) {
    return "/" + StringUtil.join("/", resourceName, segmentName);
  }

  private static String constructPropertyStorePathForResource(String resourceName) {
    return "/" + resourceName;
  }
}
