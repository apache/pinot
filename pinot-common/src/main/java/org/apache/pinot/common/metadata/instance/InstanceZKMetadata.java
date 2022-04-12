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
package org.apache.pinot.common.metadata.instance;

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadata;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

import static org.apache.pinot.spi.utils.EqualityUtils.hashCodeOf;
import static org.apache.pinot.spi.utils.EqualityUtils.isEqual;
import static org.apache.pinot.spi.utils.EqualityUtils.isNullOrNotSameClass;
import static org.apache.pinot.spi.utils.EqualityUtils.isSameReference;


public final class InstanceZKMetadata implements ZKMetadata {
  private static final String KAFKA_HIGH_LEVEL_CONSUMER_GROUP_MAP = "KAFKA_HLC_GROUP_MAP";
  private static final String KAFKA_HIGH_LEVEL_CONSUMER_PARTITION_MAP = "KAFKA_HLC_PARTITION_MAP";
  private String _id = null;
  private String _instanceName = null;
  private int _instancePort;
  private String _instanceType = null;

  // Only care about realtime resources for now
  private Map<String, String> _groupIdMap = new HashMap<String, String>();
  private Map<String, String> _partitionMap = new HashMap<String, String>();

  public InstanceZKMetadata() {
  }

  public InstanceZKMetadata(ZNRecord record) {
    _id = record.getId();
    setInstanceConfigFromId(_id);
    _groupIdMap.putAll(record.getMapField(KAFKA_HIGH_LEVEL_CONSUMER_GROUP_MAP));
    _partitionMap.putAll(record.getMapField(KAFKA_HIGH_LEVEL_CONSUMER_PARTITION_MAP));
  }

  private void setInstanceConfigFromId(String id) {
    String[] instanceConfigs = id.split("_");
    assert instanceConfigs.length == 3;
    setInstanceType(instanceConfigs[0]);
    setInstanceName(instanceConfigs[1]);
    setInstancePort(Integer.parseInt(instanceConfigs[2]));
  }

  public int getInstancePort() {
    return _instancePort;
  }

  public void setInstancePort(int instancePort) {
    _instancePort = instancePort;
  }

  public String getInstanceType() {
    return _instanceType;
  }

  public void setInstanceType(String instanceType) {
    _instanceType = instanceType;
  }

  public String getId() {
    if (_id == null) {
      _id = buildIdFromInstanceConfig();
    }
    return _id;
  }

  public void setId(String id) {
    _id = id;
  }

  public String getInstanceName() {
    return _instanceName;
  }

  public void setInstanceName(String instanceName) {
    _instanceName = instanceName;
  }

  public String getGroupId(String resourceName) {
    return _groupIdMap.get(TableNameBuilder.REALTIME.tableNameWithType(resourceName));
  }

  public void setGroupId(String resourceName, String groupId) {
    _groupIdMap.put(TableNameBuilder.REALTIME.tableNameWithType(resourceName), groupId);
  }

  public String getPartition(String resourceName) {
    return _partitionMap.get(TableNameBuilder.REALTIME.tableNameWithType(resourceName));
  }

  public void setPartition(String resourceName, String partition) {
    _partitionMap.put(TableNameBuilder.REALTIME.tableNameWithType(resourceName), partition);
  }

  public void removeResource(String resourceName) {
    _groupIdMap.remove(resourceName);
    _partitionMap.remove(resourceName);
  }

  @Override
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(getId());
    znRecord.setMapField(KAFKA_HIGH_LEVEL_CONSUMER_GROUP_MAP, _groupIdMap);
    znRecord.setMapField(KAFKA_HIGH_LEVEL_CONSUMER_PARTITION_MAP, _partitionMap);
    return znRecord;
  }

  private String buildIdFromInstanceConfig() {
    return StringUtil.join("_", _instanceType, _instanceName, _instancePort + "");
  }

  @Override
  public boolean equals(Object instanceMetadata) {
    if (isSameReference(this, instanceMetadata)) {
      return true;
    }

    if (isNullOrNotSameClass(this, instanceMetadata)) {
      return false;
    }

    InstanceZKMetadata metadata = (InstanceZKMetadata) instanceMetadata;
    return isEqual(_id, metadata._id) && isEqual(_instanceName, metadata._instanceName) && isEqual(_instanceType,
        metadata._instanceType) && isEqual(_instancePort, metadata._instancePort) && isEqual(_groupIdMap,
        metadata._groupIdMap) && isEqual(_partitionMap, metadata._partitionMap);
  }

  @Override
  public int hashCode() {
    int result = hashCodeOf(_id);
    result = hashCodeOf(result, _instanceName);
    result = hashCodeOf(result, _instancePort);
    result = hashCodeOf(result, _instanceType);
    result = hashCodeOf(result, _groupIdMap);
    result = hashCodeOf(result, _partitionMap);
    return result;
  }
}
