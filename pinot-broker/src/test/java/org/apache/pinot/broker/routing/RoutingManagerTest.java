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
package org.apache.pinot.broker.routing;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class RoutingManagerTest {
  private static String TABLE_NAME = "testTable";
  private static String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
  private static String SEGMENT_NAME = "segment1";
  private static String INSTANCE_NAME = "host1";

  @Test
  public void testRoutingManager() throws JsonProcessingException {
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    RoutingManager routingManager = new RoutingManager(brokerMetrics);
    HelixManager helixManager = mock(HelixManager.class);
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    PropertyKey.Builder builder = new PropertyKey.Builder("testCluster");
    when(helixDataAccessor.keyBuilder()).thenReturn(builder);
    @SuppressWarnings("unchecked")
    BaseDataAccessor<ZNRecord> zkDataAccessor = mock(BaseDataAccessor.class);
    when(helixDataAccessor.getBaseDataAccessor()).thenReturn(zkDataAccessor);
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    when(propertyStore.get("/CONFIGS/TABLE/testTable_OFFLINE", null, AccessOption.PERSISTENT)).thenReturn(
        TableConfigUtils.toZNRecord(tableConfig));
    ZNRecord znRecord = mock(ZNRecord.class);
    Map<String, Map<String, String>> mapFields = new HashMap<>();
    Map<String, String> instanceToStateMap = new HashMap<>();
    instanceToStateMap.put(INSTANCE_NAME, SegmentOnlineOfflineStateModel.ERROR);
    mapFields.put(SEGMENT_NAME, instanceToStateMap);
    when(znRecord.getMapFields()).thenReturn(mapFields);
    znRecord.setMapFields(mapFields);
    when(zkDataAccessor.get(helixDataAccessor.keyBuilder().idealStates().getPath() + "/" + OFFLINE_TABLE_NAME, null,
        AccessOption.PERSISTENT)).thenReturn(znRecord);

    ZNRecord externalViewZNRecord = mock(ZNRecord.class);
    externalViewZNRecord.setMapFields(mapFields);
    when(externalViewZNRecord.getMapFields()).thenReturn(mapFields);
    when(zkDataAccessor.get(eq(helixDataAccessor.keyBuilder().externalViews().getPath() + "/" + OFFLINE_TABLE_NAME),
        any(), eq(AccessOption.PERSISTENT))).thenReturn(externalViewZNRecord);

    routingManager.init(helixManager);

    ExternalView externalView = mock(ExternalView.class);
    Set<String> partitionSet = new HashSet<>();
    partitionSet.add(SEGMENT_NAME);
    when(externalView.getPartitionSet()).thenReturn(partitionSet);
    Map<String, String> partitionMap = new HashMap<>();
    partitionMap.put(INSTANCE_NAME, SegmentOnlineOfflineStateModel.ERROR);
    when(externalView.getStateMap(anyString())).thenReturn(partitionMap);

    Set<String> noReplicasSegments = new HashSet<>();
    routingManager.fetchSegmentsInErrorState(externalView, noReplicasSegments);
    Assert.assertFalse(noReplicasSegments.isEmpty());

    BrokerRequest brokerRequest = new BrokerRequest();
    QuerySource querySource = new QuerySource();
    querySource.setTableName(OFFLINE_TABLE_NAME);
    brokerRequest.setQuerySource(querySource);

    // Constructs routing entry map
    routingManager.buildRouting(OFFLINE_TABLE_NAME);
    Assert.assertTrue(routingManager.containsNoReplicaSegments(brokerRequest));
  }
}
