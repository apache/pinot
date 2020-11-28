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
package org.apache.pinot.controller.api;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import org.apache.helix.InstanceType;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.controller.api.resources.TableViews;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerTestUtils.*;

public class TableViewsTest {
  private static final String OFFLINE_TABLE_NAME = "offlineTable";
  private static final String OFFLINE_SEGMENT_NAME = "offlineSegment";
  private static final String HYBRID_TABLE_NAME = "viewsTable";

  @BeforeClass
  public void setUp()
      throws Exception {

    // Create the offline table and add one segment
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(OFFLINE_TABLE_NAME).setNumReplicas(2).build();
    Assert.assertEquals(getHelixManager().getInstanceType(), InstanceType.CONTROLLER);
    getHelixResourceManager().addTable(tableConfig);
    getHelixResourceManager().addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, OFFLINE_SEGMENT_NAME), "downloadUrl");

    // Create the hybrid table
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(HYBRID_TABLE_NAME).setNumReplicas(MIN_NUM_REPLICAS).build();
    getHelixResourceManager().addTable(tableConfig);

    // add schema for realtime table
    addDummySchema(HYBRID_TABLE_NAME);
    StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultHighLevelStreamConfigs();
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(HYBRID_TABLE_NAME).setNumReplicas(MIN_NUM_REPLICAS)
        .setStreamConfigs(streamConfig.getStreamConfigsMap()).build();
    getHelixResourceManager().addTable(tableConfig);

    // Wait for external view get updated
    long endTime = System.currentTimeMillis() + 10_000L;
    while (System.currentTimeMillis() < endTime) {
      Thread.sleep(100L);
      TableViews.TableView tableView = getTableView(OFFLINE_TABLE_NAME, TableViews.EXTERNALVIEW, null);
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
  public void testTableNotFound(String view)
      throws Exception {
    String url = getControllerRequestURLBuilder().forTableView("unknownTable", view, null);
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    Assert.assertEquals(connection.getResponseCode(), 404);
  }

  @Test(dataProvider = "viewProvider")
  public void testBadRequest(String view)
      throws Exception {
    String url = getControllerRequestURLBuilder().forTableView(OFFLINE_TABLE_NAME, view, "no_such_type");
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    Assert.assertEquals(connection.getResponseCode(), 400);
  }

  @Test(dataProvider = "viewProvider")
  public void testOfflineTableState(String view)
      throws Exception {
    TableViews.TableView tableView = getTableView(OFFLINE_TABLE_NAME, view, null);
    Assert.assertNotNull(tableView.offline);
    Assert.assertEquals(tableView.offline.size(), 1);
    Assert.assertNull(tableView.realtime);

    Map<String, String> serverMap = tableView.offline.get(OFFLINE_SEGMENT_NAME);
    Assert.assertNotNull(serverMap);
    Assert.assertEquals(serverMap.size(), 2);
    for (Map.Entry<String, String> serverMapEntry : serverMap.entrySet()) {
      Assert.assertTrue(serverMapEntry.getKey().startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE));
      Assert.assertEquals(serverMapEntry.getValue(), "ONLINE");
    }
  }

  @Test(dataProvider = "viewProvider")
  public void testHybridTableState(String state)
      throws Exception {
    TableViews.TableView tableView = getTableView(HYBRID_TABLE_NAME, state, "realtime");
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

  private TableViews.TableView getTableView(String tableName, String view, String tableType)
      throws Exception {
    return JsonUtils
        .stringToObject(sendGetRequest(getControllerRequestURLBuilder().forTableView(tableName, view, tableType)),
            TableViews.TableView.class);
  }

  @AfterClass
  public void tearDown() {
    System.out.println("All Tables: " + getHelixResourceManager().getAllTables());
    getHelixResourceManager().deleteOfflineTable(HYBRID_TABLE_NAME + "_OFFLINE");
    getHelixResourceManager().deleteRealtimeTable(HYBRID_TABLE_NAME + "_REALTIME");

    getHelixResourceManager().deleteOfflineTable(OFFLINE_TABLE_NAME + "_OFFLINE");
  }
}
