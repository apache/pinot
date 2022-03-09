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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import org.apache.helix.InstanceType;
import org.apache.pinot.controller.api.resources.TableViews;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TableViewsTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();
  private static final String OFFLINE_TABLE_NAME = "offlineTable";
  private static final String OFFLINE_SEGMENT_NAME = "offlineSegment";
  private static final String HYBRID_TABLE_NAME = "viewsTable";

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();

    // Create the offline table and add one segment
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(OFFLINE_TABLE_NAME).setNumReplicas(2).build();
    Assert.assertEquals(TEST_INSTANCE.getHelixManager().getInstanceType(), InstanceType.CONTROLLER);
    TEST_INSTANCE.getHelixResourceManager().addTable(tableConfig);
    TEST_INSTANCE.getHelixResourceManager()
        .addNewSegment(TableNameBuilder.OFFLINE.tableNameWithType(OFFLINE_TABLE_NAME),
            SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, OFFLINE_SEGMENT_NAME), "downloadUrl");

    // Create the hybrid table
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(HYBRID_TABLE_NAME)
        .setNumReplicas(TEST_INSTANCE.MIN_NUM_REPLICAS).build();
    TEST_INSTANCE.getHelixResourceManager().addTable(tableConfig);

    // add schema for realtime table
    TEST_INSTANCE.addDummySchema(HYBRID_TABLE_NAME);
    StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultHighLevelStreamConfigs();
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(HYBRID_TABLE_NAME)
        .setNumReplicas(TEST_INSTANCE.MIN_NUM_REPLICAS).setStreamConfigs(streamConfig.getStreamConfigsMap())
        .build();
    TEST_INSTANCE.getHelixResourceManager().addTable(tableConfig);

    // Wait for external view get updated
    long endTime = System.currentTimeMillis() + 10_000L;
    while (System.currentTimeMillis() < endTime) {
      Thread.sleep(100L);
      TableViews.TableView tableView;
      try {
        tableView = getTableView(OFFLINE_TABLE_NAME, TableViews.EXTERNALVIEW, null);
      } catch (IOException e) {
        // Table may not be created yet.
        continue;
      }
      if ((tableView._offline == null) || (tableView._offline.size() != 1)) {
        continue;
      }
      tableView = getTableView(HYBRID_TABLE_NAME, TableViews.EXTERNALVIEW, null);
      if (tableView._offline == null) {
        continue;
      }
      if ((tableView._realtime == null) || (tableView._realtime.size() != TEST_INSTANCE.NUM_SERVER_INSTANCES)) {
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
    String url = TEST_INSTANCE.getControllerRequestURLBuilder().forTableView("unknownTable", view, null);
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    Assert.assertEquals(connection.getResponseCode(), 404);
  }

  @Test(dataProvider = "viewProvider")
  public void testBadRequest(String view)
      throws Exception {
    String url =
        TEST_INSTANCE.getControllerRequestURLBuilder().forTableView(OFFLINE_TABLE_NAME, view, "no_such_type");
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    Assert.assertEquals(connection.getResponseCode(), 400);
  }

  @Test(dataProvider = "viewProvider")
  public void testOfflineTableState(String view)
      throws Exception {
    TableViews.TableView tableView = getTableView(OFFLINE_TABLE_NAME, view, null);
    Assert.assertNotNull(tableView._offline);
    Assert.assertEquals(tableView._offline.size(), 1);
    Assert.assertNull(tableView._realtime);

    Map<String, String> serverMap = tableView._offline.get(OFFLINE_SEGMENT_NAME);
    Assert.assertNotNull(serverMap);
    Assert.assertEquals(serverMap.size(), 2);
    for (Map.Entry<String, String> serverMapEntry : serverMap.entrySet()) {
      Assert.assertTrue(InstanceTypeUtils.isServer(serverMapEntry.getKey()));
      Assert.assertEquals(serverMapEntry.getValue(), "ONLINE");
    }
  }

  @Test(dataProvider = "viewProvider")
  public void testHybridTableState(String state)
      throws Exception {
    TableViews.TableView tableView = getTableView(HYBRID_TABLE_NAME, state, "realtime");
    Assert.assertNull(tableView._offline);
    Assert.assertNotNull(tableView._realtime);
    Assert.assertEquals(tableView._realtime.size(), TEST_INSTANCE.NUM_SERVER_INSTANCES);

    tableView = getTableView(HYBRID_TABLE_NAME, state, "offline");
    Assert.assertNotNull(tableView._offline);
    Assert.assertEquals(tableView._offline.size(), 0);
    Assert.assertNull(tableView._realtime);

    tableView = getTableView(HYBRID_TABLE_NAME, state, null);
    Assert.assertNotNull(tableView._offline);
    Assert.assertEquals(tableView._offline.size(), 0);
    Assert.assertNotNull(tableView._realtime);
    Assert.assertEquals(tableView._realtime.size(), TEST_INSTANCE.NUM_SERVER_INSTANCES);

    tableView = getTableView(TableNameBuilder.OFFLINE.tableNameWithType(HYBRID_TABLE_NAME), state, null);
    Assert.assertNotNull(tableView._offline);
    Assert.assertEquals(tableView._offline.size(), 0);
    Assert.assertNull(tableView._realtime);

    tableView = getTableView(TableNameBuilder.REALTIME.tableNameWithType(HYBRID_TABLE_NAME), state, null);
    Assert.assertNull(tableView._offline);
    Assert.assertNotNull(tableView._realtime);
    Assert.assertEquals(tableView._realtime.size(), TEST_INSTANCE.NUM_SERVER_INSTANCES);
  }

  private TableViews.TableView getTableView(String tableName, String view, String tableType)
      throws Exception {
    return JsonUtils.stringToObject(ControllerTest.sendGetRequest(
            TEST_INSTANCE.getControllerRequestURLBuilder().forTableView(tableName, view, tableType)),
        TableViews.TableView.class);
  }

  @AfterClass
  public void tearDown() {
    TEST_INSTANCE.cleanup();
  }
}
