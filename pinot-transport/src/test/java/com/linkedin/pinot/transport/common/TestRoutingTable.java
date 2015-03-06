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
package com.linkedin.pinot.transport.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.helix.model.ExternalView;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.routing.HelixExternalViewBasedRouting;
import com.linkedin.pinot.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.routing.builder.RandomRoutingTableBuilder;
import com.linkedin.pinot.routing.builder.RoutingTableBuilder;


public class TestRoutingTable {

  @Test
  public void testHelixRoutingTable() {
    RoutingTableBuilder routingStrategy = new RandomRoutingTableBuilder(100);
    HelixExternalViewBasedRouting routingTable = new HelixExternalViewBasedRouting(routingStrategy, null, null);
    ExternalView externalView = new ExternalView("testResource0");
    externalView.setState("segment0", "dataServer_instance_0", "ONLINE");
    externalView.setState("segment0", "dataServer_instance_1", "ONLINE");
    externalView.setState("segment1", "dataServer_instance_1", "ONLINE");
    externalView.setState("segment1", "dataServer_instance_2", "ONLINE");
    externalView.setState("segment2", "dataServer_instance_2", "ONLINE");
    externalView.setState("segment2", "dataServer_instance_0", "ONLINE");
    routingTable.markDataResourceOnline("testResource0", externalView);
    ExternalView externalView1 = new ExternalView("testResource1");
    externalView1.setState("segment10", "dataServer_instance_0", "ONLINE");
    externalView1.setState("segment11", "dataServer_instance_1", "ONLINE");
    externalView1.setState("segment12", "dataServer_instance_2", "ONLINE");
    routingTable.markDataResourceOnline("testResource1", externalView1);
    ExternalView externalView2 = new ExternalView("testResource2");
    externalView2.setState("segment20", "dataServer_instance_0", "ONLINE");
    externalView2.setState("segment21", "dataServer_instance_0", "ONLINE");
    externalView2.setState("segment22", "dataServer_instance_0", "ONLINE");
    externalView2.setState("segment20", "dataServer_instance_1", "ONLINE");
    externalView2.setState("segment21", "dataServer_instance_1", "ONLINE");
    externalView2.setState("segment22", "dataServer_instance_1", "ONLINE");
    externalView2.setState("segment20", "dataServer_instance_2", "ONLINE");
    externalView2.setState("segment21", "dataServer_instance_2", "ONLINE");
    externalView2.setState("segment22", "dataServer_instance_2", "ONLINE");
    routingTable.markDataResourceOnline("testResource2", externalView2);

    for (int numRun = 0; numRun < 100; ++numRun) {
      assertResourceRequest(routingTable, "testResource0", "[segment0, segment1, segment2]", 3);
    }
    for (int numRun = 0; numRun < 100; ++numRun) {
      assertResourceRequest(routingTable, "testResource1", "[segment10, segment11, segment12]", 3);
    }
    for (int numRun = 0; numRun < 100; ++numRun) {
      assertResourceRequest(routingTable, "testResource2", "[segment20, segment21, segment22]", 3);
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
}
