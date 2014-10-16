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


public class TestRoutingTable {

  @Test
  public void testHelixRoutingTable() {
    HelixExternalViewBasedRouting routingTable = new HelixExternalViewBasedRouting();
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

    RoutingTableLookupRequest request = new RoutingTableLookupRequest("testResource0");
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
    Assert.assertEquals(selectedSegments.size(), 3);
    Assert.assertEquals(Arrays.toString(selectedSegmentArray), "[segment0, segment1, segment2]");
    System.out.println("********************************");
    selectedSegments.clear();
    request = new RoutingTableLookupRequest("testResource1");
    serversMap = routingTable.findServers(request);
    for (ServerInstance serverInstance : serversMap.keySet()) {
      System.out.println(serverInstance);
      SegmentIdSet segmentIdSet = serversMap.get(serverInstance);
      System.out.println(segmentIdSet.toString());
      selectedSegments.addAll(segmentIdSet.getSegmentsNameList());
    }
    selectedSegmentArray = selectedSegments.toArray(new String[0]);
    Arrays.sort(selectedSegmentArray);
    Assert.assertEquals(selectedSegments.size(), 3);
    Assert.assertEquals(Arrays.toString(selectedSegmentArray), "[segment10, segment11, segment12]");
    System.out.println("********************************");
    selectedSegments.clear();
    request = new RoutingTableLookupRequest("testResource2");
    serversMap = routingTable.findServers(request);
    for (ServerInstance serverInstance : serversMap.keySet()) {
      System.out.println(serverInstance);
      SegmentIdSet segmentIdSet = serversMap.get(serverInstance);
      System.out.println(segmentIdSet.toString());
      selectedSegments.addAll(segmentIdSet.getSegmentsNameList());
    }
    selectedSegmentArray = selectedSegments.toArray(new String[0]);
    Arrays.sort(selectedSegmentArray);
    Assert.assertEquals(selectedSegments.size(), 3);
    Assert.assertEquals(Arrays.toString(selectedSegmentArray), "[segment20, segment21, segment22]");
  }
}
