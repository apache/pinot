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
package com.linkedin.pinot.transport.common.routing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.HLCSegmentName;
import com.linkedin.pinot.routing.HelixExternalViewBasedRouting;
import com.linkedin.pinot.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.routing.builder.KafkaHighLevelConsumerBasedRoutingTableBuilder;
import com.linkedin.pinot.routing.builder.RandomRoutingTableBuilder;
import com.linkedin.pinot.routing.builder.RoutingTableBuilder;
import com.linkedin.pinot.transport.common.SegmentIdSet;


public class RoutingTableTest {
  public static final String ALL_PARTITIONS = "ALL";
  private static Logger LOGGER = org.slf4j.LoggerFactory.getLogger(RoutingTableTest.class);
  @Test
  public void testHelixExternalViewBasedRoutingTable() {
    RoutingTableBuilder routingStrategy = new RandomRoutingTableBuilder(100);
    HelixExternalViewBasedRouting routingTable = new HelixExternalViewBasedRouting(routingStrategy, null, null, null);
    ExternalView externalView = new ExternalView("testResource0_OFFLINE");
    externalView.setState("segment0", "dataServer_instance_0", "ONLINE");
    externalView.setState("segment0", "dataServer_instance_1", "ONLINE");
    externalView.setState("segment1", "dataServer_instance_1", "ONLINE");
    externalView.setState("segment1", "dataServer_instance_2", "ONLINE");
    externalView.setState("segment2", "dataServer_instance_2", "ONLINE");
    externalView.setState("segment2", "dataServer_instance_0", "ONLINE");
    List<InstanceConfig> instanceConfigs = generateInstanceConfigs("dataServer_instance", 0, 2);
    routingTable.markDataResourceOnline("testResource0_OFFLINE", externalView, instanceConfigs);
    ExternalView externalView1 = new ExternalView("testResource1_OFFLINE");
    externalView1.setState("segment10", "dataServer_instance_0", "ONLINE");
    externalView1.setState("segment11", "dataServer_instance_1", "ONLINE");
    externalView1.setState("segment12", "dataServer_instance_2", "ONLINE");
    routingTable.markDataResourceOnline("testResource1_OFFLINE", externalView1, instanceConfigs);
    ExternalView externalView2 = new ExternalView("testResource2_OFFLINE");
    externalView2.setState("segment20", "dataServer_instance_0", "ONLINE");
    externalView2.setState("segment21", "dataServer_instance_0", "ONLINE");
    externalView2.setState("segment22", "dataServer_instance_0", "ONLINE");
    externalView2.setState("segment20", "dataServer_instance_1", "ONLINE");
    externalView2.setState("segment21", "dataServer_instance_1", "ONLINE");
    externalView2.setState("segment22", "dataServer_instance_1", "ONLINE");
    externalView2.setState("segment20", "dataServer_instance_2", "ONLINE");
    externalView2.setState("segment21", "dataServer_instance_2", "ONLINE");
    externalView2.setState("segment22", "dataServer_instance_2", "ONLINE");
    routingTable.markDataResourceOnline("testResource2_OFFLINE", externalView2, instanceConfigs);

    for (int numRun = 0; numRun < 100; ++numRun) {
      assertResourceRequest(routingTable, "testResource0_OFFLINE", "[segment0, segment1, segment2]", 3);
    }
    for (int numRun = 0; numRun < 100; ++numRun) {
      assertResourceRequest(routingTable, "testResource1_OFFLINE", "[segment10, segment11, segment12]", 3);
    }
    for (int numRun = 0; numRun < 100; ++numRun) {
      assertResourceRequest(routingTable, "testResource2_OFFLINE", "[segment20, segment21, segment22]", 3);
    }
  }

  private void assertResourceRequest(HelixExternalViewBasedRouting routingTable, String resource,
      String expectedSegmentList, int expectedNumSegment) {
    RoutingTableLookupRequest request = new RoutingTableLookupRequest(resource);
    Map<ServerInstance, SegmentIdSet> serversMap = routingTable.findServers(request);
    List<String> selectedSegments = new ArrayList<String>();
    for (ServerInstance serverInstance : serversMap.keySet()) {
      LOGGER.trace(serverInstance.toString());
      SegmentIdSet segmentIdSet = serversMap.get(serverInstance);
      LOGGER.trace(segmentIdSet.toString());
      selectedSegments.addAll(segmentIdSet.getSegmentsNameList());
    }
    String[] selectedSegmentArray = selectedSegments.toArray(new String[0]);
    Arrays.sort(selectedSegmentArray);
    Assert.assertEquals(selectedSegments.size(), expectedNumSegment);
    Assert.assertEquals(Arrays.toString(selectedSegmentArray), expectedSegmentList);
    LOGGER.trace("********************************");
  }

  @Test
  public void testKafkaHighLevelConsumerBasedRoutingTable() {
    RoutingTableBuilder routingStrategy = new KafkaHighLevelConsumerBasedRoutingTableBuilder();
    final String group0 = "testResource0_REALTIME_1433316466991_0";
    final String group1 = "testResource1_REALTIME_1433316490099_1";
    final String group2 = "testResource2_REALTIME_1436589344583_1";

    HelixExternalViewBasedRouting routingTable = new HelixExternalViewBasedRouting(null, routingStrategy, null, null);
    ExternalView externalView = new ExternalView("testResource0_REALTIME");
    externalView.setState(new HLCSegmentName(group0, ALL_PARTITIONS, "0").getSegmentName(),
        "dataServer_instance_0", "ONLINE");
    externalView.setState(new HLCSegmentName(group0, ALL_PARTITIONS, "1").getSegmentName(),
        "dataServer_instance_1", "ONLINE");
    externalView.setState(new HLCSegmentName(group1, ALL_PARTITIONS, "2").getSegmentName(),
        "dataServer_instance_2", "ONLINE");
    externalView.setState(new HLCSegmentName(group1, ALL_PARTITIONS, "3").getSegmentName(),
        "dataServer_instance_3", "ONLINE");
    externalView.setState(new HLCSegmentName(group2, ALL_PARTITIONS, "4").getSegmentName(),
        "dataServer_instance_4", "ONLINE");
    externalView.setState(new HLCSegmentName(group2, ALL_PARTITIONS, "5").getSegmentName(),
        "dataServer_instance_5", "ONLINE");
    routingTable.markDataResourceOnline("testResource0_REALTIME", externalView,
        generateInstanceConfigs("dataServer_instance", 0, 5));
    ExternalView externalView1 = new ExternalView("testResource1_REALTIME");
    externalView1.setState(new HLCSegmentName(group0, ALL_PARTITIONS, "10").getSegmentName(),
        "dataServer_instance_10", "ONLINE");
    externalView1.setState(new HLCSegmentName(group0, ALL_PARTITIONS, "11").getSegmentName(),
        "dataServer_instance_11", "ONLINE");
    externalView1.setState(new HLCSegmentName(group0, ALL_PARTITIONS, "12").getSegmentName(),
        "dataServer_instance_12", "ONLINE");
    routingTable.markDataResourceOnline("testResource1_REALTIME", externalView1,
        generateInstanceConfigs("dataServer_instance", 10, 12));
    ExternalView externalView2 = new ExternalView("testResource2_REALTIME");
    externalView2.setState(new HLCSegmentName(group0, ALL_PARTITIONS, "20").getSegmentName(),
        "dataServer_instance_20", "ONLINE");
    externalView2.setState(new HLCSegmentName(group0, ALL_PARTITIONS, "21").getSegmentName(),
        "dataServer_instance_21", "ONLINE");
    externalView2.setState(new HLCSegmentName(group0, ALL_PARTITIONS, "22").getSegmentName(),
        "dataServer_instance_22", "ONLINE");
    externalView2.setState(new HLCSegmentName(group1, ALL_PARTITIONS, "23").getSegmentName(),
        "dataServer_instance_23", "ONLINE");
    externalView2.setState(new HLCSegmentName(group1, ALL_PARTITIONS, "24").getSegmentName(),
        "dataServer_instance_24", "ONLINE");
    externalView2.setState(new HLCSegmentName(group1, ALL_PARTITIONS, "25").getSegmentName(),
        "dataServer_instance_25", "ONLINE");
    externalView2.setState(new HLCSegmentName(group2, ALL_PARTITIONS, "26").getSegmentName(),
        "dataServer_instance_26", "ONLINE");
    externalView2.setState(new HLCSegmentName(group2, ALL_PARTITIONS, "27").getSegmentName(),
        "dataServer_instance_27", "ONLINE");
    externalView2.setState(new HLCSegmentName(group2, ALL_PARTITIONS, "28").getSegmentName(),
        "dataServer_instance_28", "ONLINE");
    routingTable.markDataResourceOnline("testResource2_REALTIME", externalView2,
        generateInstanceConfigs("dataServer_instance", 20, 28));

    for (int numRun = 0; numRun < 100; ++numRun) {
      assertResourceRequest(
          routingTable,
          "testResource0_REALTIME",
          new String[] { "[" + new HLCSegmentName(group0, ALL_PARTITIONS, "0").getSegmentName()
              + ", " + new HLCSegmentName(group0, ALL_PARTITIONS, "1").getSegmentName() + "]", "["
              + new HLCSegmentName(group1, ALL_PARTITIONS, "2").getSegmentName()
              + ", "
              + new HLCSegmentName(group1, ALL_PARTITIONS, "3").getSegmentName() + "]", "["
              + new HLCSegmentName(group2, ALL_PARTITIONS, "4").getSegmentName() + ", "
              + new HLCSegmentName(group2, ALL_PARTITIONS, "5").getSegmentName() + "]" }, 2);
    }
    for (int numRun = 0; numRun < 100; ++numRun) {
      assertResourceRequest(routingTable, "testResource1_REALTIME",
          new String[] { "[" + new HLCSegmentName(group0, ALL_PARTITIONS, "10").getSegmentName()
              + ", " + new HLCSegmentName(group0, ALL_PARTITIONS, "11").getSegmentName() + ", "
              + new HLCSegmentName(group0, ALL_PARTITIONS, "12").getSegmentName() + "]" }, 3);
    }
    for (int numRun = 0; numRun < 100; ++numRun) {
      assertResourceRequest(routingTable, "testResource2_REALTIME",
          new String[] { "[" + new HLCSegmentName(group0, ALL_PARTITIONS, "20").getSegmentName()
              + ", " + new HLCSegmentName(group0, ALL_PARTITIONS, "21").getSegmentName() + ", "
              + new HLCSegmentName(group0, ALL_PARTITIONS, "22").getSegmentName() + "]", "["
              + new HLCSegmentName(group1, ALL_PARTITIONS, "23").getSegmentName() + ", "
              + new HLCSegmentName(group1, ALL_PARTITIONS, "24").getSegmentName() + ", "
              + new HLCSegmentName(group1, ALL_PARTITIONS, "25").getSegmentName() + "]", "["
              + new HLCSegmentName(group2, ALL_PARTITIONS, "26").getSegmentName() + ", "
              + new HLCSegmentName(group2, ALL_PARTITIONS, "27").getSegmentName() + ", "
              + new HLCSegmentName(group2, ALL_PARTITIONS, "28").getSegmentName() + "]" }, 3);
    }
  }

  private void assertResourceRequest(HelixExternalViewBasedRouting routingTable, String resource,
      String[] expectedSegmentLists, int expectedNumSegment) {
    RoutingTableLookupRequest request = new RoutingTableLookupRequest(resource);
    Map<ServerInstance, SegmentIdSet> serversMap = routingTable.findServers(request);
    List<String> selectedSegments = new ArrayList<String>();
    for (ServerInstance serverInstance : serversMap.keySet()) {
      LOGGER.trace(serverInstance.toString());
      SegmentIdSet segmentIdSet = serversMap.get(serverInstance);
      LOGGER.trace(segmentIdSet.toString());
      selectedSegments.addAll(segmentIdSet.getSegmentsNameList());
    }
    String[] selectedSegmentArray = selectedSegments.toArray(new String[0]);
    Arrays.sort(selectedSegmentArray);
    Assert.assertEquals(selectedSegments.size(), expectedNumSegment);
    boolean matchedExpectedLists = false;
    for (String expectedSegmentList : expectedSegmentLists) {
      if (expectedSegmentList.equals(Arrays.toString(selectedSegmentArray))) {
        matchedExpectedLists = true;
      }
    }
    Assert.assertTrue(matchedExpectedLists);
    LOGGER.trace("********************************");
  }

  /**
   * Helper method to generate instance config lists. Instance names are generated as prefix_i, where
   * i ranges from start to end.
   *
   * @param prefix Instance name prefix
   * @param start Start index
   * @param end End index
   * @return
   */
  private List<InstanceConfig> generateInstanceConfigs(String prefix, int start, int end) {
    List<InstanceConfig> configs = new ArrayList<>();

    for (int i = start; i <= end; ++i) {
      String instance = prefix + "_" + i;
      configs.add(new InstanceConfig(instance));
    }
    return configs;
  }
}
