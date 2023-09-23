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
package org.apache.pinot.controller.helix.core.assignment.segment.strategy;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.controller.helix.core.assignment.segment.OfflineSegmentAssignment;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignment;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentFactory;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentTestUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.helix.model.InstanceConfig.InstanceConfigProperty.TAG_LIST;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertTrue;


public class AllServersSegmentAssignmentStrategyTest {
  private static final String INSTANCE_NAME_PREFIX = "instance_";
  private static final int NUM_INSTANCES = 10;
  private static final List<String> INSTANCES =
      SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, NUM_INSTANCES);
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_SERVER_TAG = "DefaultTenant_OFFLINE";
  private static final String REALTIME_SERVER_TAG = "DefaultTenant_REALTIME";
  private static final String BROKER_TAG = "DefaultTenant_Broker";
  private static final String SEGMENT_NAME = "segment1";

  private SegmentAssignment _segmentAssignment;
  private HelixManager _helixManager;
  private static final String INSTANCE_PARTITIONS_NAME =
      InstancePartitionsType.OFFLINE.getInstancePartitionsName(RAW_TABLE_NAME);
  private static final String COMPLETED_INSTANCE_NAME_PREFIX = "completedInstance_";
  private static final String COMPLETED_INSTANCE_PARTITIONS_NAME =
      InstancePartitionsType.COMPLETED.getInstancePartitionsName(RAW_TABLE_NAME);
  private static final List<String> COMPLETED_INSTANCES =
      SegmentAssignmentTestUtils.getNameList(COMPLETED_INSTANCE_NAME_PREFIX, NUM_INSTANCES);

  private Map<InstancePartitionsType, InstancePartitions> _instancePartitionsMap = new HashMap<>();

  @BeforeClass
  public void setup() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setIsDimTable(true).build();
    _helixManager = mock(HelixManager.class);
    InstancePartitions instancePartitions = new InstancePartitions(INSTANCE_PARTITIONS_NAME);
    instancePartitions.setInstances(0, 0, INSTANCES);

    InstancePartitions completedInstancePartitions = new InstancePartitions(COMPLETED_INSTANCE_PARTITIONS_NAME);
    completedInstancePartitions.setInstances(0, 0, COMPLETED_INSTANCES);

    _instancePartitionsMap.put(InstancePartitionsType.OFFLINE, instancePartitions);
    _instancePartitionsMap.put(InstancePartitionsType.COMPLETED, completedInstancePartitions);
    _segmentAssignment = SegmentAssignmentFactory.getSegmentAssignment(_helixManager, tableConfig, null);
  }

  @Test
  public void testFactory() {
    assertTrue(_segmentAssignment instanceof OfflineSegmentAssignment);
  }

  @Test
  public void testSegmentAssignmentAndRebalance() {
    List<HelixProperty> instanceConfigList = new ArrayList<>();
    for (String instance : INSTANCES) {
      ZNRecord znRecord = new ZNRecord(instance);
      znRecord.setListField(TAG_LIST.name(), ImmutableList.of(OFFLINE_SERVER_TAG, REALTIME_SERVER_TAG));
      instanceConfigList.add(new InstanceConfig(znRecord));
    }
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    PropertyKey.Builder builder = new PropertyKey.Builder("cluster");
    when(dataAccessor.keyBuilder()).thenReturn(builder);
    when(dataAccessor.getChildValues(builder.instanceConfigs(), true)).thenReturn(instanceConfigList);
    when(_helixManager.getHelixDataAccessor()).thenReturn(dataAccessor);

    List<String> instances = _segmentAssignment.assignSegment(SEGMENT_NAME, new TreeMap(), _instancePartitionsMap);
    assertEquals(instances.size(), NUM_INSTANCES);
    assertEqualsNoOrder(instances.toArray(), INSTANCES.toArray());

    // Remove one instance and rebalance table
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    Map<String, String> segment1Assginment = new TreeMap<>();
    instances.stream().forEach(instance -> segment1Assginment.put(instance, "ONLINE"));
    currentAssignment.put(SEGMENT_NAME, segment1Assginment);
    ZNRecord znRecord = new ZNRecord(instanceConfigList.get(0).getId());
    znRecord.setListField(TAG_LIST.name(), ImmutableList.of(BROKER_TAG));
    InstanceConfig newInstanceConfig = new InstanceConfig(znRecord);
    instanceConfigList.set(0, newInstanceConfig);
    when(dataAccessor.getChildValues(builder.instanceConfigs(), true)).thenReturn(instanceConfigList);

    Map<String, Map<String, String>> newAssignment =
        _segmentAssignment.rebalanceTable(currentAssignment, _instancePartitionsMap, null, null, null);
    assertEquals(newAssignment.get(SEGMENT_NAME).size(), NUM_INSTANCES - 1);
  }

  @Test
  public void testSegmentAssignmentToRealtimeHosts() {
    List<HelixProperty> instanceConfigList = new ArrayList<>();
    for (String instance : INSTANCES) {
      ZNRecord znRecord = new ZNRecord(instance);
      znRecord.setListField(TAG_LIST.name(), ImmutableList.of(REALTIME_SERVER_TAG));
      instanceConfigList.add(new InstanceConfig(znRecord));
    }
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    PropertyKey.Builder builder = new PropertyKey.Builder("cluster");
    when(dataAccessor.keyBuilder()).thenReturn(builder);
    when(dataAccessor.getChildValues(builder.instanceConfigs(), true)).thenReturn(instanceConfigList);
    when(_helixManager.getHelixDataAccessor()).thenReturn(dataAccessor);

    List<String> instances = _segmentAssignment.assignSegment(SEGMENT_NAME, new TreeMap(), _instancePartitionsMap);
    assertEquals(instances.size(), NUM_INSTANCES);
    assertEqualsNoOrder(instances.toArray(), INSTANCES.toArray());
  }
}
