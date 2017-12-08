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
package com.linkedin.pinot.controller.api.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.ControllerTest;
import com.linkedin.pinot.core.query.utils.SimpleSegmentMetadata;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TableViewsTest extends ControllerTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String OFFLINE_TABLE_NAME = "offlineTable";
  private static final String HYBRID_TABLE_NAME = "hybridTable";
  private static final int NUM_BROKER_INSTANCES = 3;
  private static final int NUM_SERVER_INSTANCES = 4;

  @BeforeClass
  public void setUp() throws Exception {
    startZk();
    startController();

    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(getHelixClusterName(),
        ZkStarter.DEFAULT_ZK_STR, NUM_BROKER_INSTANCES, true);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(getHelixClusterName(),
        ZkStarter.DEFAULT_ZK_STR, NUM_SERVER_INSTANCES, true);

    // Create the offline table and add one segment
    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(OFFLINE_TABLE_NAME)
            .setNumReplicas(2)
            .build();
    _helixResourceManager.addTable(tableConfig);
    _helixResourceManager.addNewSegment(new SimpleSegmentMetadata(OFFLINE_TABLE_NAME), "downloadUrl");

    // Create the hybrid table
    tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(HYBRID_TABLE_NAME)
        .setNumReplicas(2)
        .build();
    _helixResourceManager.addTable(tableConfig);

    Map<String, String> streamConfigs = new HashMap<>();
    streamConfigs.put(DataSource.STREAM_PREFIX + "." + DataSource.Realtime.Kafka.CONSUMER_TYPE,
        DataSource.Realtime.Kafka.ConsumerType.highLevel.toString());
    tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME).setTableName(HYBRID_TABLE_NAME)
        .setNumReplicas(2)
        .setStreamConfigs(streamConfigs)
        .build();
    _helixResourceManager.addTable(tableConfig);

    // Wait for external view get updated
    long endTime = System.currentTimeMillis() + 10_000L;
    while (System.currentTimeMillis() < endTime) {
      Thread.sleep(100L);
      TableViews.TableView
          tableView = getTableView(OFFLINE_TABLE_NAME, TableViews.EXTERNALVIEW, null);
      if ((tableView.offline == null) || (tableView.offline.size() != 1)) {
        continue;
      }
      tableView = getTableView(HYBRID_TABLE_NAME, TableViews.EXTERNALVIEW, null);
      if (tableView.offline == null) {
        continue;
      }
      if ((tableView.realtime == null) || (tableView.realtime.size() != NUM_SERVER_INSTANCES)) {
        continue;
      }
      return;
    }
    Assert.fail("Failed to get external view updated");
  }

  @DataProvider(name = "viewProvider")
  public Object[][] viewProvider() {
    return new Object[][]{{TableViews.IDEALSTATE}, {TableViews.EXTERNALVIEW}};
  }

  @Test(dataProvider = "viewProvider")
  public void testTableNotFound(String view) throws Exception {
    String url = _controllerRequestURLBuilder.forTableView("unknownTable", view, null);
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    Assert.assertEquals(connection.getResponseCode(), 404);
  }

  @Test(dataProvider = "viewProvider")
  public void testBadRequest(String view) throws Exception {
    String url = _controllerRequestURLBuilder.forTableView(OFFLINE_TABLE_NAME, view, "no_such_type");
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    Assert.assertEquals(connection.getResponseCode(), 400);
  }

  @Test(dataProvider = "viewProvider")
  public void testOfflineTableState(String view) throws Exception {
    TableViews.TableView
        tableView = getTableView(OFFLINE_TABLE_NAME, view, null);
    Assert.assertNotNull(tableView.offline);
    Assert.assertEquals(tableView.offline.size(), 1);
    Assert.assertNull(tableView.realtime);

    for (Map.Entry<String, Map<String, String>> segmentMapEntry : tableView.offline.entrySet()) {
      Assert.assertTrue(segmentMapEntry.getKey().startsWith("SimpleSegment"));
      Map<String, String> serverMap = segmentMapEntry.getValue();
      Assert.assertEquals(serverMap.size(), 2);
      for (Map.Entry<String, String> serverMapEntry : serverMap.entrySet()) {
        Assert.assertTrue(serverMapEntry.getKey().startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE));
        Assert.assertEquals(serverMapEntry.getValue(), "ONLINE");
      }
    }
  }

  @Test(dataProvider = "viewProvider")
  public void testHybridTableState(String state) throws Exception {
    TableViews.TableView
        tableView = getTableView(HYBRID_TABLE_NAME, state, "realtime");
    Assert.assertNull(tableView.offline);
    Assert.assertNotNull(tableView.realtime);
    Assert.assertEquals(tableView.realtime.size(), NUM_SERVER_INSTANCES);

    tableView = getTableView(HYBRID_TABLE_NAME, state, "offline");
    Assert.assertNotNull(tableView.offline);
    Assert.assertEquals(tableView.offline.size(), 0);
    Assert.assertNull(tableView.realtime);

    tableView = getTableView(HYBRID_TABLE_NAME, state, null);
    Assert.assertNotNull(tableView.offline);
    Assert.assertEquals(tableView.offline.size(), 0);
    Assert.assertNotNull(tableView.realtime);
    Assert.assertEquals(tableView.realtime.size(), NUM_SERVER_INSTANCES);

    tableView = getTableView(TableNameBuilder.OFFLINE.tableNameWithType(HYBRID_TABLE_NAME), state, null);
    Assert.assertNotNull(tableView.offline);
    Assert.assertEquals(tableView.offline.size(), 0);
    Assert.assertNull(tableView.realtime);

    tableView = getTableView(TableNameBuilder.REALTIME.tableNameWithType(HYBRID_TABLE_NAME), state, null);
    Assert.assertNull(tableView.offline);
    Assert.assertNotNull(tableView.realtime);
    Assert.assertEquals(tableView.realtime.size(), NUM_SERVER_INSTANCES);
  }

  private TableViews.TableView getTableView(String tableName, String view, String tableType) throws Exception {
    return OBJECT_MAPPER.readValue(
        sendGetRequest(_controllerRequestURLBuilder.forTableView(tableName, view, tableType)),
        TableViews.TableView.class);
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
