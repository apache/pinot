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

import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.routing.TableConfigRoutingTableSelector;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.HLCSegmentName;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.routing.HelixExternalViewBasedRouting;
import com.linkedin.pinot.routing.PercentageBasedRoutingTableSelector;
import com.linkedin.pinot.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.routing.RoutingTableSelector;
import com.linkedin.pinot.routing.RoutingTableSelectorFactory;
import com.linkedin.pinot.routing.builder.KafkaHighLevelConsumerBasedRoutingTableBuilder;
import com.linkedin.pinot.routing.builder.RandomRoutingTableBuilder;
import com.linkedin.pinot.routing.builder.RoutingTableBuilder;
import com.linkedin.pinot.transport.common.SegmentIdSet;


public class RoutingTableTest {
  public static final String ALL_PARTITIONS = "ALL";
  private static Logger LOGGER = org.slf4j.LoggerFactory.getLogger(RoutingTableTest.class);
  private static RoutingTableSelector NO_LLC_ROUTING = new PercentageBasedRoutingTableSelector();
  @Test
  public void testHelixExternalViewBasedRoutingTable() throws Exception {
    RoutingTableBuilder routingStrategy = new RandomRoutingTableBuilder(100);
    HelixExternalViewBasedRouting routingTable = new HelixExternalViewBasedRouting(null, NO_LLC_ROUTING, null);
    Field offlineRTBField = HelixExternalViewBasedRouting.class.getDeclaredField("_offlineRoutingTableBuilder");
    offlineRTBField.setAccessible(true);
    offlineRTBField.set(routingTable, routingStrategy);

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
    RoutingTableLookupRequest request = new RoutingTableLookupRequest(resource, Collections.<String>emptyList());
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
  public void testKafkaHighLevelConsumerBasedRoutingTable() throws Exception {
    RoutingTableBuilder routingStrategy = new KafkaHighLevelConsumerBasedRoutingTableBuilder();
    final String group0 = "testResource0_REALTIME_1433316466991_0";
    final String group1 = "testResource1_REALTIME_1433316490099_1";
    final String group2 = "testResource2_REALTIME_1436589344583_1";

    final LLCSegmentName llcSegmentName = new LLCSegmentName("testResource0", 2, 65, System.currentTimeMillis());

    HelixExternalViewBasedRouting routingTable = new HelixExternalViewBasedRouting(null, NO_LLC_ROUTING, null);

    Field realtimeRTBField = HelixExternalViewBasedRouting.class.getDeclaredField("_realtimeHLCRoutingTableBuilder");
    realtimeRTBField.setAccessible(true);
    realtimeRTBField.set(routingTable, routingStrategy);

    ExternalView externalView = new ExternalView("testResource0_REALTIME");
    // Toss in an llc segment in the mix. Should not affect the results
    externalView.setState(llcSegmentName.getSegmentName(), "dataServer_instance_0", "CONSUMING");
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
    RoutingTableLookupRequest request = new RoutingTableLookupRequest(resource, Collections.<String>emptyList());
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

  // Test that we can switch between llc and hlc routing depending on what the selector tells us.
  @Test
  public void testCombinedKafkaRouting() throws Exception {
    HelixExternalViewBasedRouting routingTable = new HelixExternalViewBasedRouting(null, NO_LLC_ROUTING, null);

    final long now = System.currentTimeMillis();
    final String tableName = "table";
    final String resourceName = tableName + "_REALTIME";
    final String group1 = resourceName + "_" + Long.toString(now) + "_0";
    final String group2 = resourceName + "_" + Long.toString(now) + "_1";
    final String online = "ONLINE";
    final String consuming = "CONSUMING";
    final int partitionId = 1;
    final String partitionRange = "JUNK";
    final int segId1 = 1;
    final int segId2 = 2;
    final int port1 = 1;
    final int port2 = 2;
    final String host = "host";
    final ServerInstance serverInstance1 = new ServerInstance(host, port1);
    final ServerInstance serverInstance2 = new ServerInstance(host, port2);
    final String helixInstance1 = CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + serverInstance1;
    final String helixInstance2 = CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + serverInstance2;
    final HLCSegmentName s1HlcSegment1 = new HLCSegmentName(group1, partitionRange, Integer.toString(segId1));
    final HLCSegmentName s1HlcSegment2 = new HLCSegmentName(group1, partitionRange, Integer.toString(segId2));
    final HLCSegmentName s2HlcSegment1 = new HLCSegmentName(group2, partitionRange, Integer.toString(segId1));
    final HLCSegmentName s2HlcSegment2 = new HLCSegmentName(group2, partitionRange, Integer.toString(segId2));
    final LLCSegmentName llcSegment1 = new LLCSegmentName(tableName, partitionId, segId1, now);
    final LLCSegmentName llcSegment2 = new LLCSegmentName(tableName, partitionId, segId2, now);

    final List<InstanceConfig> instanceConfigs = new ArrayList<>(2);
    instanceConfigs.add(new InstanceConfig(helixInstance1));
    instanceConfigs.add(new InstanceConfig(helixInstance2));
    ExternalView ev = new ExternalView(resourceName);
    ev.setState(s1HlcSegment1.getSegmentName(), helixInstance1, online);
    ev.setState(s1HlcSegment2.getSegmentName(), helixInstance1, online);
    ev.setState(llcSegment1.getSegmentName(), helixInstance2, online);
    ev.setState(llcSegment2.getSegmentName(), helixInstance2, consuming);
    routingTable.markDataResourceOnline(resourceName, ev, instanceConfigs);

    RoutingTableLookupRequest request = new RoutingTableLookupRequest(resourceName, Collections.<String>emptyList());
    for (int i = 0; i < 100; i++) {
      Map<ServerInstance, SegmentIdSet> routingMap = routingTable.findServers(request);
      Assert.assertEquals(routingMap.size(), 1);
      List<String> segments = routingMap.get(serverInstance1).getSegmentsNameList();
      Assert.assertEquals(segments.size(), 2);
      Assert.assertTrue(segments.contains(s1HlcSegment1.getSegmentName()));
      Assert.assertTrue(segments.contains(s1HlcSegment2.getSegmentName()));
    }

    // Now change the percent value in the routing table selector to be 100, and we should get only LLC segments.
    Configuration configuration = new PropertiesConfiguration();
    configuration.addProperty("class", PercentageBasedRoutingTableSelector.class.getName());
    configuration.addProperty("table." + resourceName, new Integer(100));
    RoutingTableSelector selector = RoutingTableSelectorFactory.getRoutingTableSelector(configuration, null);
    selector.init(configuration, null);
    Field selectorField = HelixExternalViewBasedRouting.class.getDeclaredField("_routingTableSelector");
    selectorField.setAccessible(true);
    selectorField.set(routingTable, selector);

    // And we should find only LLC segments.
    for (int i = 0; i < 100; i++) {
      Map<ServerInstance, SegmentIdSet> routingMap = routingTable.findServers(request);
      Assert.assertEquals(routingMap.size(), 1);
      List<String> segments = routingMap.get(serverInstance2).getSegmentsNameList();
      Assert.assertEquals(segments.size(), 2);
      Assert.assertTrue(segments.contains(llcSegment1.getSegmentName()));
      Assert.assertTrue(segments.contains(llcSegment2.getSegmentName()));
    }

    // Now change it to 50, and we should find both (at least 10 times each).
    configuration = new PropertiesConfiguration();
    configuration.addProperty("table." + resourceName, new Integer(50));
    selector = new PercentageBasedRoutingTableSelector();
    selector.init(configuration, null);
    selectorField.set(routingTable, selector);

    int hlc = 0;
    int llc = 0;
    for (int i = 0; i < 100; i++) {
      Map<ServerInstance, SegmentIdSet> routingMap = routingTable.findServers(request);
      Assert.assertEquals(routingMap.size(), 1);
      if (routingMap.containsKey(serverInstance2)) {
        List<String> segments = routingMap.get(serverInstance2).getSegmentsNameList();
        Assert.assertEquals(segments.size(), 2);
        Assert.assertTrue(segments.contains(llcSegment1.getSegmentName()));
        Assert.assertTrue(segments.contains(llcSegment2.getSegmentName()));
        llc++;
      } else {
        List<String> segments = routingMap.get(serverInstance1).getSegmentsNameList();
        Assert.assertEquals(segments.size(), 2);
        Assert.assertTrue(segments.contains(s1HlcSegment1.getSegmentName()));
        Assert.assertTrue(segments.contains(s1HlcSegment2.getSegmentName()));
        hlc++;
      }
    }

    // If we do the above iteration 100 times, we should get at least 10 of each type of routing.
    // If this test fails
    Assert.assertTrue(hlc >= 10, "Got low values hlc=" + hlc + ",llc="  + llc);
    Assert.assertTrue(llc >= 10, "Got low values hlc=" + hlc + ",llc="  + llc);

    // Check that force HLC works
    request = new RoutingTableLookupRequest(resourceName, Collections.singletonList("FORCE_HLC"));
    hlc = 0;
    llc = 0;
    for (int i = 0; i < 100; i++) {
      Map<ServerInstance, SegmentIdSet> routingMap = routingTable.findServers(request);
      Assert.assertEquals(routingMap.size(), 1);
      if (routingMap.containsKey(serverInstance2)) {
        List<String> segments = routingMap.get(serverInstance2).getSegmentsNameList();
        Assert.assertEquals(segments.size(), 2);
        Assert.assertTrue(segments.contains(llcSegment1.getSegmentName()));
        Assert.assertTrue(segments.contains(llcSegment2.getSegmentName()));
        llc++;
      } else {
        List<String> segments = routingMap.get(serverInstance1).getSegmentsNameList();
        Assert.assertEquals(segments.size(), 2);
        Assert.assertTrue(segments.contains(s1HlcSegment1.getSegmentName()));
        Assert.assertTrue(segments.contains(s1HlcSegment2.getSegmentName()));
        hlc++;
      }
    }

    Assert.assertEquals(hlc, 100);
    Assert.assertEquals(llc, 0);

    // Check that force LLC works
    request = new RoutingTableLookupRequest(resourceName, Collections.singletonList("FORCE_LLC"));
    hlc = 0;
    llc = 0;
    for (int i = 0; i < 100; i++) {
      Map<ServerInstance, SegmentIdSet> routingMap = routingTable.findServers(request);
      Assert.assertEquals(routingMap.size(), 1);
      if (routingMap.containsKey(serverInstance2)) {
        List<String> segments = routingMap.get(serverInstance2).getSegmentsNameList();
        Assert.assertEquals(segments.size(), 2);
        Assert.assertTrue(segments.contains(llcSegment1.getSegmentName()));
        Assert.assertTrue(segments.contains(llcSegment2.getSegmentName()));
        llc++;
      } else {
        List<String> segments = routingMap.get(serverInstance1).getSegmentsNameList();
        Assert.assertEquals(segments.size(), 2);
        Assert.assertTrue(segments.contains(s1HlcSegment1.getSegmentName()));
        Assert.assertTrue(segments.contains(s1HlcSegment2.getSegmentName()));
        hlc++;
      }
    }

    Assert.assertEquals(hlc, 0);
    Assert.assertEquals(llc, 100);
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

  class FakePropertyStore extends ZkHelixPropertyStore<ZNRecord> {
    private ZNRecord _contents = null;
    private IZkDataListener _listener = null;

    public FakePropertyStore() {
      super((ZkBaseDataAccessor<ZNRecord>) null, null, null);
    }

    @Override
    public ZNRecord get(String path, Stat stat, int options) {
      return _contents;
    }

    @Override
    public void subscribeDataChanges(String path, IZkDataListener listener) {
      _listener = listener;
    }

    public void setContents(String path, ZNRecord contents) throws Exception {
      _contents = contents;
      if (_listener != null) {
        _listener.handleDataChange(path, contents);
      }
    }

    @Override
    public void start() {
      // Don't try to connect to zk
    }
  }

  @Test
  public void testTableConfigRoutingTableSelector() throws Exception {
    FakePropertyStore fakePropertyStore = new FakePropertyStore();

    TableConfigRoutingTableSelector tableConfigRoutingTableSelector = new TableConfigRoutingTableSelector();
    tableConfigRoutingTableSelector.init(null, fakePropertyStore);

    String propertyStoreEntry = "{\n" + "        \"tableName\":\"fakeTable\",\n" + "        \"segmentsConfig\": {\n" + "            \"retentionTimeUnit\":\"DAYS\",\n" + "            \"retentionTimeValue\":\"5\",\n"
        + "            \"segmentPushFrequency\":\"daily\",\n" + "            \"segmentPushType\":\"APPEND\",\n" + "            \"replication\": \"2\",\n" + "            \"schemaName\": \"fakeSchema\",\n"
        + "            \"timeColumnName\": \"time\",\n" + "            \"timeType\": \"MINUTES\",\n" + "            \"segmentAssignmentStrategy\": \"BalanceNumSegmentAssignmentStrategy\"\n" + "         },\n"
        + "         \"tableIndexConfig\": {\n" + "            \"invertedIndexColumns\": [\"columnA\",\"columnB\"],\n"
        + "            \"loadMode\": \"MMAP\",\n" + "            \"lazyLoad\": \"false\",\n" + "            \"streamConfigs\": {\n" + "               \"streamType\": \"kafka\",\n"
        + "               \"stream.kafka.consumer.type\": \"highLevel,simple\",\n" + "               \"stream.kafka.topic.name\": \"FakeTopic\",\n"
        + "               \"stream.kafka.decoder.class.name\": \"com.linkedin.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder\",\n"
        + "\n" + "               \"stream.kafka.zk.broker.url\": \"fakezk:1234\",\n" + "               \"stream.kafka.hlc.zk.connect.string\": \"fakezk:1234\",\n"
        + "               \"stream.kafka.decoder.prop.schema.registry.rest.url\": \"fakeschemaregistry:1234\",\n" + "               \"stream.kafka.decoder.prop.schema.registry.schema.name\": \"FakeSchema\"\n"
        + "             }\n" + "          },\n" + "          \"tenants\": {\n" + "              \"broker\":\"fakebroker\",\n" + "              \"server\":\"fakeserver\"\n"
        + "          },\n" + "          \"tableType\":\"REALTIME\",\n" + "          \"metadata\": {\n" + "              \"customConfigs\": {\n" + "                  \"routing.llc.percentage\": \"50.0\"\n"
        + "               }\n" + "          }\n" + "    }\n" + "}";

    AbstractTableConfig tableConfig = AbstractTableConfig.init(propertyStoreEntry);

    fakePropertyStore.setContents("/PROPERTYSTORE/TABLES/fakeTable_REALTIME", AbstractTableConfig.toZnRecord(tableConfig));

    tableConfigRoutingTableSelector.registerTable("fakeTable_REALTIME");

    int llcCount = 0;
    for (int i = 0; i < 10000; ++i) {
      if (tableConfigRoutingTableSelector.shouldUseLLCRouting("fakeTable_REALTIME")) {
        llcCount++;
      }
    }

    Assert.assertTrue(4500 <= llcCount && llcCount <= 5500, "Expected approximately 50% probability of picking LLC, got " + llcCount / 100.0 + " %");

    tableConfig.getCustomConfigs().setCustomConfigs(Collections.singletonMap("routing.llc.percentage", "0"));
    fakePropertyStore.setContents("/PROPERTYSTORE/TABLES/fakeTable_REALTIME", AbstractTableConfig.toZnRecord(tableConfig));

    llcCount = 0;
    for (int i = 0; i < 10000; ++i) {
      if (tableConfigRoutingTableSelector.shouldUseLLCRouting("fakeTable_REALTIME")) {
        llcCount++;
      }
    }

    Assert.assertEquals(llcCount, 0, "Expected 0% probability of picking LLC, got " + llcCount / 100.0 + " %");

    tableConfig.getCustomConfigs().setCustomConfigs(Collections.singletonMap("routing.llc.percentage", "100"));
    fakePropertyStore.setContents("/PROPERTYSTORE/TABLES/fakeTable_REALTIME", AbstractTableConfig.toZnRecord(tableConfig));

    llcCount = 0;
    for (int i = 0; i < 10000; ++i) {
      if (tableConfigRoutingTableSelector.shouldUseLLCRouting("fakeTable_REALTIME")) {
        llcCount++;
      }
    }

    Assert.assertEquals(llcCount, 10000, "Expected 100% probability of picking LLC, got " + llcCount / 100.0 + " %");
  }
}
