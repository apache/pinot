/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.broker.routing;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableConfig.Builder;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import java.io.FileInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RandomRoutingTableTest {
  private static final int NUM_ROUNDS = 100;
  private static final int MIN_NUM_SEGMENTS_PER_SERVER = 28;
  private static final int MAX_NUM_SEGMENTS_PER_SERVER = 31;

  @Test
  public void testHelixExternalViewBasedRoutingTable() throws Exception {
    URL resourceUrl = getClass().getClassLoader().getResource("SampleExternalView.json");
    Assert.assertNotNull(resourceUrl);
    String fileName = resourceUrl.getFile();

    byte[] externalViewBytes = IOUtils.toByteArray(new FileInputStream(fileName));
    ExternalView externalView = new ExternalView((ZNRecord) new ZNRecordSerializer().deserialize(externalViewBytes));
    String tableName = externalView.getResourceName();
    List<InstanceConfig> instanceConfigs = getInstanceConfigs(externalView);
    int numSegmentsInEV = externalView.getPartitionSet().size();
    int numServersInEV = instanceConfigs.size();

    HelixExternalViewBasedRouting routing = new HelixExternalViewBasedRouting(null, null, new BaseConfiguration());
    routing.markDataResourceOnline(generateTableConfig(tableName), externalView, instanceConfigs);

    for (int i = 0; i < NUM_ROUNDS; i++) {
      Map<String, List<String>> routingTable = routing.getRoutingTable(new RoutingTableLookupRequest(tableName));
      Assert.assertEquals(routingTable.size(), numServersInEV);
      int numSegments = 0;
      for (List<String> segmentsForServer : routingTable.values()) {
        int numSegmentsForServer = segmentsForServer.size();
        Assert.assertTrue(
            numSegmentsForServer >= MIN_NUM_SEGMENTS_PER_SERVER && numSegmentsForServer <= MAX_NUM_SEGMENTS_PER_SERVER);
        numSegments += numSegmentsForServer;
      }
      Assert.assertEquals(numSegments, numSegmentsInEV);
    }
  }

  private List<InstanceConfig> getInstanceConfigs(ExternalView externalView) {
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    Set<String> instances = new HashSet<>();

    // Collect all unique instances
    for (String partitionName : externalView.getPartitionSet()) {
      for (String instance : externalView.getStateMap(partitionName).keySet()) {
        if (!instances.contains(instance)) {
          instanceConfigs.add(new InstanceConfig(instance));
          instances.add(instance);
        }
      }
    }

    return instanceConfigs;
  }

  private TableConfig generateTableConfig(String tableName) throws Exception {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    Builder builder = new TableConfig.Builder(tableType);
    builder.setTableName(tableName);
    return builder.build();
  }
}
