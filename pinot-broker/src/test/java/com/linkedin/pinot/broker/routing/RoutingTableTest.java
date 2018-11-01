/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.broker.routing.builder.HighLevelConsumerBasedRoutingTableBuilder;
import com.linkedin.pinot.broker.routing.builder.RoutingTableBuilder;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableConfig.Builder;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.HLCSegmentName;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RoutingTableTest {
  public static final String ALL_PARTITIONS = "ALL";

  @Test
  public void testHelixExternalViewBasedRoutingTable() throws Exception {
    HelixExternalViewBasedRouting routingTable = new HelixExternalViewBasedRouting(null, null, new BaseConfiguration());

    ExternalView externalView = new ExternalView("testResource0_OFFLINE");
    externalView.setState("segment0", "dataServer_instance_0", "ONLINE");
    externalView.setState("segment0", "dataServer_instance_1", "ONLINE");
    externalView.setState("segment1", "dataServer_instance_1", "ONLINE");
    externalView.setState("segment1", "dataServer_instance_2", "ONLINE");
    externalView.setState("segment2", "dataServer_instance_2", "ONLINE");
    externalView.setState("segment2", "dataServer_instance_0", "ONLINE");
    List<InstanceConfig> instanceConfigs = generateInstanceConfigs("dataServer_instance", 0, 2);
    TableConfig testResource0Config = generateTableConfig("testResource0_OFFLINE");
    routingTable.markDataResourceOnline(testResource0Config, externalView, instanceConfigs);
    ExternalView externalView1 = new ExternalView("testResource1_OFFLINE");
    externalView1.setState("segment10", "dataServer_instance_0", "ONLINE");
    externalView1.setState("segment11", "dataServer_instance_1", "ONLINE");
    externalView1.setState("segment12", "dataServer_instance_2", "ONLINE");

    TableConfig testResource1Config = generateTableConfig("testResource1_OFFLINE");
    routingTable.markDataResourceOnline(testResource1Config, externalView1, instanceConfigs);

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
    TableConfig testResource2Config = generateTableConfig("testResource2_OFFLINE");

    routingTable.markDataResourceOnline(testResource2Config, externalView2, instanceConfigs);

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


  @Test
  public void testTimeBoundaryRegression() throws Exception {
    final FakePropertyStore propertyStore = new FakePropertyStore();
    final OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
    offlineSegmentZKMetadata.setTimeUnit(TimeUnit.DAYS);
    offlineSegmentZKMetadata.setEndTime(1234L);

    propertyStore.setContents(ZKMetadataProvider.constructPropertyStorePathForSegment("myTable_OFFLINE",
        "someSegment_0"), offlineSegmentZKMetadata.toZNRecord());

    final ExternalView offlineExternalView = new ExternalView("myTable_OFFLINE");
    offlineExternalView.setState("someSegment_0", "Server_1.2.3.4_1234", "ONLINE");

    final MutableBoolean timeBoundaryUpdated = new MutableBoolean(false);

    HelixExternalViewBasedRouting routingTable =
        new HelixExternalViewBasedRouting(propertyStore, null, new BaseConfiguration()) {
      @Override
      protected ExternalView fetchExternalView(String table) {
        return offlineExternalView;
      }

      @Override
      protected void updateTimeBoundary(String tableName, ExternalView externalView) {
        if (tableName.equals("myTable_OFFLINE")) {
          timeBoundaryUpdated.setValue(true);
        }
      }
    };
    routingTable.setBrokerMetrics(new BrokerMetrics(new MetricsRegistry()));

    Assert.assertFalse(timeBoundaryUpdated.booleanValue());

    final ArrayList<InstanceConfig> instanceConfigList = new ArrayList<>();
    instanceConfigList.add(new InstanceConfig("Server_1.2.3.4_1234"));
    TableConfig myTableOfflineConfig = generateTableConfig("myTable_OFFLINE");
    TableConfig myTableRealtimeConfig = generateTableConfig("myTable_REALTIME");

    routingTable.markDataResourceOnline(myTableOfflineConfig, offlineExternalView, instanceConfigList);
    routingTable.markDataResourceOnline(myTableRealtimeConfig, new ExternalView("myTable_REALTIME"), new ArrayList<InstanceConfig>());

    Assert.assertTrue(timeBoundaryUpdated.booleanValue());
  }

  private void assertResourceRequest(HelixExternalViewBasedRouting routing, String resource, String expectedSegmentList,
      int expectedNumSegment) {
    Map<String, List<String>> routingTable = routing.getRoutingTable(new RoutingTableLookupRequest(resource));
    List<String> selectedSegments = new ArrayList<>();
    for (List<String> segmentsForServer : routingTable.values()) {
      selectedSegments.addAll(segmentsForServer);
    }
    String[] selectedSegmentArray = selectedSegments.toArray(new String[0]);
    Arrays.sort(selectedSegmentArray);
    Assert.assertEquals(selectedSegments.size(), expectedNumSegment);
    Assert.assertEquals(Arrays.toString(selectedSegmentArray), expectedSegmentList);
  }

  @Test(enabled=false)
  public void testKafkaHighLevelConsumerBasedRoutingTable() throws Exception {
    RoutingTableBuilder routingStrategy = new HighLevelConsumerBasedRoutingTableBuilder();
    final String group0 = "testResource0_REALTIME_1433316466991_0";
    final String group1 = "testResource1_REALTIME_1433316490099_1";
    final String group2 = "testResource2_REALTIME_1436589344583_1";

    final LLCSegmentName llcSegmentName = new LLCSegmentName("testResource0", 2, 65, System.currentTimeMillis());

    HelixExternalViewBasedRouting routingTable = new HelixExternalViewBasedRouting(null, null, new BaseConfiguration());

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
    routingTable.markDataResourceOnline(generateTableConfig("testResource0_REALTIME"), externalView,
        generateInstanceConfigs("dataServer_instance", 0, 5));
    ExternalView externalView1 = new ExternalView("testResource1_REALTIME");
    externalView1.setState(new HLCSegmentName(group0, ALL_PARTITIONS, "10").getSegmentName(),
        "dataServer_instance_10", "ONLINE");
    externalView1.setState(new HLCSegmentName(group0, ALL_PARTITIONS, "11").getSegmentName(),
        "dataServer_instance_11", "ONLINE");
    externalView1.setState(new HLCSegmentName(group0, ALL_PARTITIONS, "12").getSegmentName(),
        "dataServer_instance_12", "ONLINE");
    routingTable.markDataResourceOnline(generateTableConfig("testResource1_REALTIME"), externalView1,
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
    routingTable.markDataResourceOnline(generateTableConfig("testResource2_REALTIME"), externalView2,
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

  private void assertResourceRequest(HelixExternalViewBasedRouting routing, String resource,
      String[] expectedSegmentLists, int expectedNumSegment) {
    Map<String, List<String>> routingTable = routing.getRoutingTable(new RoutingTableLookupRequest(resource));
    List<String> selectedSegments = new ArrayList<>();
    for (List<String> segmentsForServer : routingTable.values()) {
      selectedSegments.addAll(segmentsForServer);
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

  private TableConfig generateTableConfig(String tableName) throws IOException, JSONException {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    Builder builder = new TableConfig.Builder(tableType);
    builder.setTableName(tableName);
    return builder.build();
  }
}
