package com.linkedin.pinot.common.metadata.instance;

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.ZNRecord;

import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.CommonConstants;


public class InstanceMetadata {

  private String _instanceName;

  // Only care about realtime resources for now
  private Map<String, String> _groupIdMap = new HashMap<String, String>();
  private Map<String, String> _partitionMap = new HashMap<String, String>();

  public InstanceMetadata() {
  }

  public InstanceMetadata(ZNRecord record) {
    _instanceName = record.getSimpleField(CommonConstants.Helix.Instance.INSTANCE_NAME);
    for (String key : record.getSimpleFields().keySet()) {
      if (key.endsWith(CommonConstants.Helix.Instance.GROUP_ID_SUFFIX)) {
        _groupIdMap.put(extactRealtimeResourceName(key), record.getSimpleField(key));
        continue;
      }
      if (key.endsWith(CommonConstants.Helix.Instance.PARTITION_SUFFIX)) {
        _partitionMap.put(extactRealtimeResourceName(key), record.getSimpleField(key));
        continue;
      }
    }
  }

  private String extactRealtimeResourceName(String key) {
    String resourceName = key.split(".")[0];
    if (resourceName.endsWith(CommonConstants.Broker.DataResource.REALTIME_RESOURCE_SUFFIX)) {
      return resourceName;
    } else {
      return BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName);
    }

  }

  public String getInstanceName() {
    return _instanceName;
  }

  public void setInstanceName(String instanceName) {
    _instanceName = instanceName;
  }

  public String getGroupId(String resourceName) {
    if (resourceName.endsWith(CommonConstants.Broker.DataResource.REALTIME_RESOURCE_SUFFIX)) {
      return _groupIdMap.get(resourceName);
    } else {
      return _groupIdMap.get(BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName));
    }
  }

  public void setGroupId(String resourceName, String groupId) {
    if (resourceName.endsWith(CommonConstants.Broker.DataResource.REALTIME_RESOURCE_SUFFIX)) {
      _groupIdMap.put(resourceName, groupId);
    } else {
      _groupIdMap.put(BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName), groupId);
    }
  }

  public String getPartition(String resourceName) {
    if (resourceName.endsWith(CommonConstants.Broker.DataResource.REALTIME_RESOURCE_SUFFIX)) {
      return _partitionMap.get(resourceName);
    } else {
      return _partitionMap.get(BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName));
    }
  }

  public void setPartition(String resourceName, String partition) {
    if (resourceName.endsWith(CommonConstants.Broker.DataResource.REALTIME_RESOURCE_SUFFIX)) {
      _partitionMap.put(resourceName, partition);
    } else {
      _partitionMap.put(BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName), partition);
    }
  }

  public ZNRecord toZnRecord() {
    ZNRecord znRecord = new ZNRecord(_instanceName);
    return znRecord;
  }

  public static InstanceMetadata fromZNRecord(ZNRecord record) {
    return new InstanceMetadata(record);
  }

}
