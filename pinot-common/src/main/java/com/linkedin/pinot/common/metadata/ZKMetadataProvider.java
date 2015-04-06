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
package com.linkedin.pinot.common.metadata;

import java.util.ArrayList;
import java.util.List;

import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.OfflineDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.RealtimeDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.StringUtil;


public class ZKMetadataProvider {
  private static String PROPERTYSTORE_SEGMENTS_PREFIX = "/SEGMENTS";
  private static String PROPERTYSTORE_RESOURCE_CONFIGS_PREFIX = "/CONFIGS/RESOURCE";
  private static String PROPERTYSTORE_INSTANCE_CONFIGS_PREFIX = "/CONFIGS/INSTANCE";

  public static void setRealtimeResourceZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, RealtimeDataResourceZKMetadata realtimeDataResource) {
    ZNRecord znRecord = realtimeDataResource.toZNRecord();
    String realtimeResourceName = BrokerRequestUtils.getRealtimeResourceNameForResource(realtimeDataResource.getResourceName());
    propertyStore.set(constructPropertyStorePathForResourceConfig(realtimeResourceName), znRecord, AccessOption.PERSISTENT);
  }

  public static RealtimeDataResourceZKMetadata getRealtimeResourceZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, String resourceName) {
    String realtimeResourceName = BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName);
    ZNRecord znRecord = propertyStore.get(constructPropertyStorePathForResourceConfig(realtimeResourceName), null, AccessOption.PERSISTENT);
    if (znRecord == null) {
      return null;
    }
    return new RealtimeDataResourceZKMetadata(znRecord);
  }

  public static void setOfflineResourceZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, OfflineDataResourceZKMetadata offlineDataResource) {
    ZNRecord znRecord = offlineDataResource.toZNRecord();
    String offlineResourceName = BrokerRequestUtils.getOfflineResourceNameForResource(offlineDataResource.getResourceName());
    propertyStore.set(constructPropertyStorePathForResourceConfig(offlineResourceName), znRecord, AccessOption.PERSISTENT);
  }

  public static OfflineDataResourceZKMetadata getOfflineResourceZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, String resourceName) {
    String offlineResourceName = BrokerRequestUtils.getOfflineResourceNameForResource(resourceName);
    ZNRecord znRecord = propertyStore.get(constructPropertyStorePathForResourceConfig(offlineResourceName), null, AccessOption.PERSISTENT);
    if (znRecord == null) {
      return null;
    }
    return new OfflineDataResourceZKMetadata(znRecord);
  }

  public static void setInstanceZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, InstanceZKMetadata instanceZKMetadata) {
    ZNRecord znRecord = instanceZKMetadata.toZNRecord();
    propertyStore.set(StringUtil.join("/", PROPERTYSTORE_INSTANCE_CONFIGS_PREFIX, instanceZKMetadata.getId()), znRecord, AccessOption.PERSISTENT);
  }

  public static InstanceZKMetadata getInstanceZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, String instanceId) {
    ZNRecord znRecord = propertyStore.get(StringUtil.join("/", PROPERTYSTORE_INSTANCE_CONFIGS_PREFIX, instanceId), null, AccessOption.PERSISTENT);
    if (znRecord == null) {
      return null;
    }
    return new InstanceZKMetadata(znRecord);
  }

  public static OfflineSegmentZKMetadata getOfflineSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, String resourceName, String segmentName) {
    resourceName = BrokerRequestUtils.getOfflineResourceNameForResource(resourceName);
    return new OfflineSegmentZKMetadata(propertyStore.get(constructPropertyStorePathForSegment(resourceName, segmentName), null, AccessOption.PERSISTENT));
  }

  public static void setOfflineSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, OfflineSegmentZKMetadata offlineSegmentZKMetadata) {
    propertyStore.set(constructPropertyStorePathForSegment(
        BrokerRequestUtils.getOfflineResourceNameForResource(offlineSegmentZKMetadata.getResourceName()), offlineSegmentZKMetadata.getSegmentName()),
        offlineSegmentZKMetadata.toZNRecord(), AccessOption.PERSISTENT);
  }

  public static RealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, String resourceName, String segmentName) {
    resourceName = BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName);
    return new RealtimeSegmentZKMetadata(propertyStore.get(
        constructPropertyStorePathForSegment(resourceName, segmentName), null, AccessOption.PERSISTENT));
  }

  public static void setRealtimeSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, RealtimeSegmentZKMetadata realtimeSegmentZKMetadata) {
    propertyStore.set(constructPropertyStorePathForSegment(
        BrokerRequestUtils.getRealtimeResourceNameForResource(realtimeSegmentZKMetadata.getResourceName()), realtimeSegmentZKMetadata.getSegmentName()),
        realtimeSegmentZKMetadata.toZNRecord(), AccessOption.PERSISTENT);
  }

  public static String constructPropertyStorePathForSegment(String resourceName, String segmentName) {
    return StringUtil.join("/", PROPERTYSTORE_SEGMENTS_PREFIX, resourceName, segmentName);
  }

  public static String constructPropertyStorePathForResource(String resourceName) {
    return StringUtil.join("/", PROPERTYSTORE_SEGMENTS_PREFIX, resourceName);
  }

  public static String constructPropertyStorePathForResourceConfig(String resourceName) {
    return StringUtil.join("/", PROPERTYSTORE_RESOURCE_CONFIGS_PREFIX, resourceName);
  }

  public static List<OfflineSegmentZKMetadata> getOfflineResourceZKMetadataListForResource(ZkHelixPropertyStore<ZNRecord> propertyStore, String resourceName) {
    List<OfflineSegmentZKMetadata> resultList = new ArrayList<OfflineSegmentZKMetadata>();
    if (propertyStore == null) {
      return resultList;
    }
    String offlineResourceName = BrokerRequestUtils.getOfflineResourceNameForResource(resourceName);
    if (propertyStore.exists(constructPropertyStorePathForResource(offlineResourceName), AccessOption.PERSISTENT)) {
      List<ZNRecord> znRecordList = propertyStore.getChildren(constructPropertyStorePathForResource(offlineResourceName), null, AccessOption.PERSISTENT);
      if (znRecordList != null) {
        for (ZNRecord record : znRecordList) {
          resultList.add(new OfflineSegmentZKMetadata(record));
        }
      }
    }
    return resultList;
  }

  public static List<RealtimeSegmentZKMetadata> getRealtimeResourceZKMetadataListForResource(ZkHelixPropertyStore<ZNRecord> propertyStore, String resourceName) {
    List<RealtimeSegmentZKMetadata> resultList = new ArrayList<RealtimeSegmentZKMetadata>();
    if (propertyStore == null) {
      return resultList;
    }
    String realtimeResourceName = BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName);
    if (propertyStore.exists(constructPropertyStorePathForResource(realtimeResourceName), AccessOption.PERSISTENT)) {
      List<ZNRecord> znRecordList = propertyStore.getChildren(constructPropertyStorePathForResource(realtimeResourceName), null, AccessOption.PERSISTENT);
      if (znRecordList != null) {
        for (ZNRecord record : znRecordList) {
          resultList.add(new RealtimeSegmentZKMetadata(record));
        }
      }
    }
    return resultList;
  }

  public static boolean isSegmentExisted(ZkHelixPropertyStore<ZNRecord> propertyStore, String resourceNameForResource, String segmentName) {
    return propertyStore.exists(constructPropertyStorePathForSegment(resourceNameForResource, segmentName), AccessOption.PERSISTENT);
  }

  public static void removeResourceSegmentsFromPropertyStore(ZkHelixPropertyStore<ZNRecord> propertyStore, String resourceName) {
    String propertyStorePath = constructPropertyStorePathForResource(resourceName);
    if (propertyStore.exists(propertyStorePath, AccessOption.PERSISTENT)) {
      propertyStore.remove(propertyStorePath, AccessOption.PERSISTENT);
    }
  }

  public static void removeResourceConfigFromPropertyStore(ZkHelixPropertyStore<ZNRecord> propertyStore, String resourceName) {
    String propertyStorePath = constructPropertyStorePathForResourceConfig(resourceName);
    if (propertyStore.exists(propertyStorePath, AccessOption.PERSISTENT)) {
      propertyStore.remove(propertyStorePath, AccessOption.PERSISTENT);
    }
  }

}
