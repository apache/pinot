package com.linkedin.pinot.common.metadata;

import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
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
  private static String PROPERTYSTORE_SEGMENTS_PREFIX = "SEGMENTS";
  private static String PROPERTYSTORE_RESOURCE_CONFIGS_PREFIX = "/CONFIGS/RESOURCE";
  private static String PROPERTYSTORE_INSTANCE_CONFIGS_PREFIX = "/CONFIGS/INSTANCE";

  public static void setRealtimeResourceZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, RealtimeDataResourceZKMetadata realtimeDataResource) {
    ZNRecord znRecord = realtimeDataResource.toZNRecord();
    propertyStore.set(StringUtil.join("/", PROPERTYSTORE_RESOURCE_CONFIGS_PREFIX, realtimeDataResource.getResourceName()), znRecord, AccessOption.PERSISTENT);
  }

  public static RealtimeDataResourceZKMetadata getRealtimeResourceZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, String resourceName) {
    String realtimeResourceName = null;
    if (resourceName.endsWith(CommonConstants.Broker.DataResource.REALTIME_RESOURCE_SUFFIX)) {
      realtimeResourceName = resourceName;
    } else {
      realtimeResourceName = BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName);
    }
    ZNRecord znRecord = propertyStore.get(StringUtil.join("/", PROPERTYSTORE_RESOURCE_CONFIGS_PREFIX, realtimeResourceName), null, AccessOption.PERSISTENT);
    if (znRecord == null) {
      return null;
    }
    return new RealtimeDataResourceZKMetadata(znRecord);
  }

  public static void setOfflineResourceZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, OfflineDataResourceZKMetadata offlineDataResource) {
    ZNRecord znRecord = offlineDataResource.toZNRecord();
    propertyStore.set(StringUtil.join("/", PROPERTYSTORE_RESOURCE_CONFIGS_PREFIX, offlineDataResource.getResourceName()), znRecord, AccessOption.PERSISTENT);
  }

  public static OfflineDataResourceZKMetadata getOfflineResourceZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, String resourceName) {
    String offlineResourceName = null;
    if (resourceName.endsWith(CommonConstants.Broker.DataResource.OFFLINE_RESOURCE_SUFFIX)) {
      offlineResourceName = resourceName;
    } else {
      offlineResourceName = BrokerRequestUtils.getOfflineResourceNameForResource(resourceName);
    }
    ZNRecord znRecord = propertyStore.get(StringUtil.join("/", PROPERTYSTORE_RESOURCE_CONFIGS_PREFIX, offlineResourceName), null, AccessOption.PERSISTENT);
    if (znRecord == null) {
      return null;
    }
    return new OfflineDataResourceZKMetadata(znRecord);
  }

  public static void setInstanceZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, InstanceZKMetadata instanceZKMetadata) {
    ZNRecord znRecord = instanceZKMetadata.toZNRecord();
    propertyStore.set(StringUtil.join("/", PROPERTYSTORE_INSTANCE_CONFIGS_PREFIX, instanceZKMetadata.getInstanceName()), znRecord, AccessOption.PERSISTENT);
  }

  public static InstanceZKMetadata getInstanceZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, String instanceId) {
    ZNRecord znRecord = propertyStore.get(StringUtil.join("/", PROPERTYSTORE_INSTANCE_CONFIGS_PREFIX, instanceId), null, AccessOption.PERSISTENT);
    if (znRecord == null) {
      return null;
    }
    return new InstanceZKMetadata(znRecord);
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
    return "/" + StringUtil.join("/", PROPERTYSTORE_SEGMENTS_PREFIX, resourceName, segmentName);
  }

}
