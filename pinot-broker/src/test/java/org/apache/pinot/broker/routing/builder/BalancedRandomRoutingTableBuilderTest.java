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
package org.apache.pinot.broker.routing.builder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.routing.RoutingTableLookupRequest;
import org.apache.pinot.common.config.RoutingConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.core.transport.ServerInstance;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test that random routing tables are really random.
 */
public class BalancedRandomRoutingTableBuilderTest {

  @Test
  public void isRandom() {
    // Build dummy external view
    BalancedRandomRoutingTableBuilder routingTableBuilder = new BalancedRandomRoutingTableBuilder();

    ExternalView externalView = getDummyExternalView();

    // Create configs for above instances.
    List<InstanceConfig> instanceConfigList = getDummyInstanceConfigs();

    // Build routing table
    routingTableBuilder.computeOnExternalViewChange("dummy", externalView, instanceConfigList);
    List<Map<ServerInstance, List<String>>> routingTables = routingTableBuilder.getRoutingTables();

    // Check that at least two routing tables are different
    Iterator<Map<ServerInstance, List<String>>> routingTableIterator = routingTables.iterator();
    Map<ServerInstance, List<String>> previous = routingTableIterator.next();
    while (routingTableIterator.hasNext()) {
      Map<ServerInstance, List<String>> current = routingTableIterator.next();
      if (!current.equals(previous)) {
        return;
      }
    }

    Assert.fail("All routing tables are equal!");
  }

  @Test
  public void testDynamicRouting()
      throws Exception {
    String tableNameWithType = "testTable_OFFLINE";
    BalancedRandomRoutingTableBuilder routingTableBuilder = new BalancedRandomRoutingTableBuilder();

    TableConfig tableConfig = new TableConfig.Builder(TableType.OFFLINE).setTableName(tableNameWithType)
        .setRoutingConfig(
            new RoutingConfig(null, Collections.singletonMap(RoutingConfig.ENABLE_DYNAMIC_COMPUTING_KEY, "true")))
        .build();
    routingTableBuilder.init(new BaseConfiguration(), tableConfig, null, null);

    // Create external view
    ExternalView externalView = getDummyExternalView();

    // Create instance configs
    List<InstanceConfig> instanceConfigList = getDummyInstanceConfigs();

    // Build routing table
    routingTableBuilder.computeOnExternalViewChange("dummy", externalView, instanceConfigList);
    RoutingTableLookupRequest request = new RoutingTableLookupRequest(tableNameWithType);
    Map<ServerInstance, List<String>> routingTable = routingTableBuilder.getRoutingTable(request, null);

    Set<String> segmentsInRoutingTable = new HashSet<>();
    for (List<String> segments : routingTable.values()) {
      segmentsInRoutingTable.addAll(segments);
    }

    // Check that we picked all segments from the table
    Set<String> expectedSegments = externalView.getPartitionSet();
    Assert.assertEquals(segmentsInRoutingTable, expectedSegments);
  }

  private ExternalView getDummyExternalView() {
    ExternalView externalView = new ExternalView("dummy");
    externalView.setState("segment_1", "Server_1.2.3.4_1234", "ONLINE");
    externalView.setState("segment_1", "Server_1.2.3.5_2345", "ONLINE");
    externalView.setState("segment_1", "Server_1.2.3.6_3456", "ONLINE");
    externalView.setState("segment_2", "Server_1.2.3.4_1234", "ONLINE");
    externalView.setState("segment_2", "Server_1.2.3.5_2345", "ONLINE");
    externalView.setState("segment_2", "Server_1.2.3.6_3456", "ONLINE");
    externalView.setState("segment_3", "Server_1.2.3.4_1234", "ONLINE");
    externalView.setState("segment_3", "Server_1.2.3.5_2345", "ONLINE");
    externalView.setState("segment_3", "Server_1.2.3.6_3456", "ONLINE");
    return externalView;
  }

  private List<InstanceConfig> getDummyInstanceConfigs() {
    List<InstanceConfig> instanceConfigList = new ArrayList<>();
    instanceConfigList.add(new InstanceConfig("Server_1.2.3.4_1234"));
    instanceConfigList.add(new InstanceConfig("Server_1.2.3.5_2345"));
    instanceConfigList.add(new InstanceConfig("Server_1.2.3.6_3456"));
    return instanceConfigList;
  }
}
