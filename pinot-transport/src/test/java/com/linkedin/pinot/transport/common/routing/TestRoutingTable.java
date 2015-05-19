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
package com.linkedin.pinot.transport.common.routing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.helix.model.ExternalView;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.SegmentNameBuilder;
import com.linkedin.pinot.routing.HelixExternalViewBasedRouting;
import com.linkedin.pinot.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.routing.builder.KafkaHighLevelConsumerBasedRoutingTableBuilder;
import com.linkedin.pinot.routing.builder.RandomRoutingTableBuilder;
import com.linkedin.pinot.routing.builder.RoutingTableBuilder;
import com.linkedin.pinot.transport.common.SegmentIdSet;


public class TestRoutingTable {

  @Test
  public void testHelixExternalViewBasedRoutingTable() {
    RoutingTableBuilder routingStrategy = new RandomRoutingTableBuilder(100);
    HelixExternalViewBasedRouting routingTable = new HelixExternalViewBasedRouting(routingStrategy, null, null, null);
    ExternalView externalView = new ExternalView("testResource0_O");
    externalView.setState("segment0", "dataServer_instance_0", "ONLINE");
    externalView.setState("segment0", "dataServer_instance_1", "ONLINE");
    externalView.setState("segment1", "dataServer_instance_1", "ONLINE");
    externalView.setState("segment1", "dataServer_instance_2", "ONLINE");
    externalView.setState("segment2", "dataServer_instance_2", "ONLINE");
    externalView.setState("segment2", "dataServer_instance_0", "ONLINE");
    routingTable.markDataResourceOnline("testResource0_O", externalView);
    ExternalView externalView1 = new ExternalView("testResource1_O");
    externalView1.setState("segment10", "dataServer_instance_0", "ONLINE");
    externalView1.setState("segment11", "dataServer_instance_1", "ONLINE");
    externalView1.setState("segment12", "dataServer_instance_2", "ONLINE");
    routingTable.markDataResourceOnline("testResource1_O", externalView1);
    ExternalView externalView2 = new ExternalView("testResource2_O");
    externalView2.setState("segment20", "dataServer_instance_0", "ONLINE");
    externalView2.setState("segment21", "dataServer_instance_0", "ONLINE");
    externalView2.setState("segment22", "dataServer_instance_0", "ONLINE");
    externalView2.setState("segment20", "dataServer_instance_1", "ONLINE");
    externalView2.setState("segment21", "dataServer_instance_1", "ONLINE");
    externalView2.setState("segment22", "dataServer_instance_1", "ONLINE");
    externalView2.setState("segment20", "dataServer_instance_2", "ONLINE");
    externalView2.setState("segment21", "dataServer_instance_2", "ONLINE");
    externalView2.setState("segment22", "dataServer_instance_2", "ONLINE");
    routingTable.markDataResourceOnline("testResource2_O", externalView2);

    for (int numRun = 0; numRun < 100; ++numRun) {
      assertResourceRequest(routingTable, "testResource0_O", "[segment0, segment1, segment2]", 3);
    }
    for (int numRun = 0; numRun < 100; ++numRun) {
      assertResourceRequest(routingTable, "testResource1_O", "[segment10, segment11, segment12]", 3);
    }
    for (int numRun = 0; numRun < 100; ++numRun) {
      assertResourceRequest(routingTable, "testResource2_O", "[segment20, segment21, segment22]", 3);
    }
  }

  private void assertResourceRequest(HelixExternalViewBasedRouting routingTable, String resource,
      String expectedSegmentList, int expectedNumSegment) {
    RoutingTableLookupRequest request = new RoutingTableLookupRequest(resource);
    Map<ServerInstance, SegmentIdSet> serversMap = routingTable.findServers(request);
    List<String> selectedSegments = new ArrayList<String>();
    for (ServerInstance serverInstance : serversMap.keySet()) {
      System.out.println(serverInstance);
      SegmentIdSet segmentIdSet = serversMap.get(serverInstance);
      System.out.println(segmentIdSet.toString());
      selectedSegments.addAll(segmentIdSet.getSegmentsNameList());
    }
    String[] selectedSegmentArray = selectedSegments.toArray(new String[0]);
    Arrays.sort(selectedSegmentArray);
    Assert.assertEquals(selectedSegments.size(), expectedNumSegment);
    Assert.assertEquals(Arrays.toString(selectedSegmentArray), expectedSegmentList);
    System.out.println("********************************");
  }

  @Test
  public void testKafkaHighLevelConsumerBasedRoutingTable() {
    RoutingTableBuilder routingStrategy = new KafkaHighLevelConsumerBasedRoutingTableBuilder();

    HelixExternalViewBasedRouting routingTable = new HelixExternalViewBasedRouting(null, routingStrategy, null, null);
    ExternalView externalView = new ExternalView("testResource0_R");
    externalView.setState(
        SegmentNameBuilder.Realtime.build("testResource0_R", "instance", "0", "0", "0"), "dataServer_instance_0", "ONLINE");
    externalView.setState(
        SegmentNameBuilder.Realtime.build("testResource0_R", "instance", "0", "1", "1"), "dataServer_instance_1", "ONLINE");
    externalView.setState(
        SegmentNameBuilder.Realtime.build("testResource0_R", "instance", "1", "0", "2"), "dataServer_instance_2", "ONLINE");
    externalView.setState(
        SegmentNameBuilder.Realtime.build("testResource0_R", "instance", "1", "1", "3"), "dataServer_instance_3", "ONLINE");
    externalView.setState(
        SegmentNameBuilder.Realtime.build("testResource0_R", "instance", "2", "0", "4"), "dataServer_instance_4", "ONLINE");
    externalView.setState(
        SegmentNameBuilder.Realtime.build("testResource0_R", "instance", "2", "1", "5"), "dataServer_instance_5", "ONLINE");
    routingTable.markDataResourceOnline("testResource0_R", externalView);
    ExternalView externalView1 = new ExternalView("testResource1_R");
    externalView1.setState(
        SegmentNameBuilder.Realtime.build("testResource1_R", "instance", "0", "0", "10"), "dataServer_instance_10", "ONLINE");
    externalView1.setState(
        SegmentNameBuilder.Realtime.build("testResource1_R", "instance", "0", "1", "11"), "dataServer_instance_11", "ONLINE");
    externalView1.setState(
        SegmentNameBuilder.Realtime.build("testResource1_R", "instance", "0", "2", "12"), "dataServer_instance_12", "ONLINE");
    routingTable.markDataResourceOnline("testResource1_R", externalView1);
    ExternalView externalView2 = new ExternalView("testResource2_R");
    externalView2.setState(
        SegmentNameBuilder.Realtime.build("testResource2_R", "instance", "0", "0", "20"), "dataServer_instance_20", "ONLINE");
    externalView2.setState(
        SegmentNameBuilder.Realtime.build("testResource2_R", "instance", "0", "1", "21"), "dataServer_instance_21", "ONLINE");
    externalView2.setState(
        SegmentNameBuilder.Realtime.build("testResource2_R", "instance", "0", "2", "22"), "dataServer_instance_22", "ONLINE");
    externalView2.setState(
        SegmentNameBuilder.Realtime.build("testResource2_R", "instance", "1", "0", "23"), "dataServer_instance_23", "ONLINE");
    externalView2.setState(
        SegmentNameBuilder.Realtime.build("testResource2_R", "instance", "1", "1", "24"), "dataServer_instance_24", "ONLINE");
    externalView2.setState(
        SegmentNameBuilder.Realtime.build("testResource2_R", "instance", "1", "2", "25"), "dataServer_instance_25", "ONLINE");
    externalView2.setState(
        SegmentNameBuilder.Realtime.build("testResource2_R", "instance", "2", "0", "26"), "dataServer_instance_26", "ONLINE");
    externalView2.setState(
        SegmentNameBuilder.Realtime.build("testResource2_R", "instance", "2", "1", "27"), "dataServer_instance_27", "ONLINE");
    externalView2.setState(
        SegmentNameBuilder.Realtime.build("testResource2_R", "instance", "2", "2", "28"), "dataServer_instance_28", "ONLINE");
    routingTable.markDataResourceOnline("testResource2_R", externalView2);

    for (int numRun = 0; numRun < 100; ++numRun) {
      assertResourceRequest(routingTable, "testResource0_R", new String[] {
          "[" + SegmentNameBuilder.Realtime.build("testResource0_R", "instance", "0", "0", "0")
              + ", " + SegmentNameBuilder.Realtime.build("testResource0_R", "instance", "0", "1", "1") + "]",
          "[" + SegmentNameBuilder.Realtime.build("testResource0_R", "instance", "1", "0", "2") + ", " +
              SegmentNameBuilder.Realtime.build("testResource0_R", "instance", "1", "1", "3") + "]",
          "[" + SegmentNameBuilder.Realtime.build("testResource0_R", "instance", "2", "0", "4") + ", " +
              SegmentNameBuilder.Realtime.build("testResource0_R", "instance", "2", "1", "5") + "]" }, 2);
    }
    for (int numRun = 0; numRun < 100; ++numRun) {
      assertResourceRequest(routingTable, "testResource1_R", new String[] {
          "[" + SegmentNameBuilder.Realtime.build("testResource1_R", "instance", "0", "0", "10")
              + ", " + SegmentNameBuilder.Realtime.build("testResource1_R", "instance", "0", "1", "11")
              + ", " + SegmentNameBuilder.Realtime.build("testResource1_R", "instance", "0", "2", "12") + "]" }, 3);
    }
    for (int numRun = 0; numRun < 100; ++numRun) {
      assertResourceRequest(routingTable, "testResource2_R", new String[] {
          "[" + SegmentNameBuilder.Realtime.build("testResource2_R", "instance", "0", "0", "20")
              + ", " + SegmentNameBuilder.Realtime.build("testResource2_R", "instance", "0", "1", "21")
              + ", " + SegmentNameBuilder.Realtime.build("testResource2_R", "instance", "0", "2", "22") + "]",
          "[" + SegmentNameBuilder.Realtime.build("testResource2_R", "instance", "1", "0", "23")
              + ", " + SegmentNameBuilder.Realtime.build("testResource2_R", "instance", "1", "1", "24")
              + ", " + SegmentNameBuilder.Realtime.build("testResource2_R", "instance", "1", "2", "25") + "]",
          "[" + SegmentNameBuilder.Realtime.build("testResource2_R", "instance", "2", "0", "26")
              + ", " + SegmentNameBuilder.Realtime.build("testResource2_R", "instance", "2", "1", "27")
              + ", " + SegmentNameBuilder.Realtime.build("testResource2_R", "instance", "2", "2", "28") + "]"
      }, 3);
    }
  }

  private void assertResourceRequest(HelixExternalViewBasedRouting routingTable, String resource,
      String[] expectedSegmentLists, int expectedNumSegment) {
    RoutingTableLookupRequest request = new RoutingTableLookupRequest(resource);
    Map<ServerInstance, SegmentIdSet> serversMap = routingTable.findServers(request);
    List<String> selectedSegments = new ArrayList<String>();
    for (ServerInstance serverInstance : serversMap.keySet()) {
      System.out.println(serverInstance);
      SegmentIdSet segmentIdSet = serversMap.get(serverInstance);
      System.out.println(segmentIdSet.toString());
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
    System.out.println("********************************");
  }

}
