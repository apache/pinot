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
package com.linkedin.pinot.controller.api.restlet.resources;

import com.linkedin.pinot.common.config.QuotaConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.request.helper.ControllerRequestBuilder;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.controller.helix.ControllerTest;
import com.linkedin.pinot.controller.helix.ControllerTestUtils;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource;
import static com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource.Realtime.Kafka;
import static org.testng.FileAssert.fail;


/**
 * Test for table creation
 */
public class PinotTableRestletResourceTest extends ControllerTest {
  public static final int TABLE_MIN_REPLICATION = 3;
  @BeforeClass
  public void setUp() {
    startZk();
    ControllerConf config = ControllerTestUtils.getDefaultControllerConfiguration();
    config.setTableMinReplicas(TABLE_MIN_REPLICATION);
    startController(config);
  }

  @Test
  public void testCreateTable() throws Exception {
    // Create a table with an invalid name
    JSONObject request = ControllerRequestBuilder.buildCreateOfflineTableJSON("bad__table__name", "default", "default",
        "potato", "DAYS", "DAYS", "5", 3, "BalanceNumSegmentAssignmentStrategy", Collections.<String>emptyList(),
        "MMAP", "v1", null);

    try {
      sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableCreate(),
          request.toString());
      fail("Creation of a table with two underscores in the table name did not fail");
    } catch (IOException e) {
      // Expected
    }

    // Create a table with a valid name
    request = ControllerRequestBuilder.buildCreateOfflineTableJSON("valid_table_name", "default", "default",
        "potato", "DAYS", "DAYS", "5", 3, "BalanceNumSegmentAssignmentStrategy", Collections.<String>emptyList(),
        "MMAP", "v1", null);

    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableCreate(), request.toString());
    boolean tableExistsError = false;
    try {
      sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableCreate(), request.toString());
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 409"), e.getMessage());
      tableExistsError = true;
    }
    Assert.assertTrue(tableExistsError);

    // Create a table with an invalid name
    JSONObject metadata = new JSONObject();
    metadata.put("streamType", "kafka");
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.CONSUMER_TYPE, Kafka.ConsumerType.highLevel.toString());
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.TOPIC_NAME, "fakeTopic");
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.DECODER_CLASS, "fakeClass");
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.ZK_BROKER_URL, "fakeUrl");
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.HighLevelConsumer.ZK_CONNECTION_STRING, "potato");
    metadata.put(DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE, Integer.toString(1234));
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.KAFKA_CONSUMER_PROPS_PREFIX + "." + Kafka.AUTO_OFFSET_RESET,
        "smallest");

    request = ControllerRequestBuilder.buildCreateRealtimeTableJSON("bad__table__name", "default", "default",
        "potato", "DAYS", "DAYS", "5", 3, "BalanceNumSegmentAssignmentStrategy", metadata, "fakeSchema", "fakeColumn",
        Collections.<String>emptyList(), "MMAP", true);

    try {
      sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableCreate(),
          request.toString());
      fail("Creation of a table with two underscores in the table name did not fail");
    } catch (IOException e) {
      // Expected
    }

    // Create a table with a valid name
    request = ControllerRequestBuilder.buildCreateRealtimeTableJSON("valid_table_name", "default", "default",
        "potato", "DAYS", "DAYS", "5", 3, "BalanceNumSegmentAssignmentStrategy", metadata, "fakeSchema", "fakeColumn",
        Collections.<String>emptyList(), "MMAP", true);

    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableCreate(), request.toString());

    // try again...should work because POST on existing RT table is allowed
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableCreate(), request.toString());
  }

  @Test
  public void testTableMinReplication()
      throws JSONException, IOException {
    testTableMinReplicationInternal("minReplicationOne", 1);
    testTableMinReplicationInternal("minReplicationTwo", TABLE_MIN_REPLICATION + 2);

  }

  private void testTableMinReplicationInternal(String tableName, int tableReplication)
      throws JSONException, IOException {
    JSONObject request = ControllerRequestBuilder
        .buildCreateOfflineTableJSON(tableName, "default", "default", "potato", "DAYS", "DAYS", "5", tableReplication,
            "BalanceNumSegmentAssignmentStrategy", Collections.<String>emptyList(), "MMAP", "v3", null);

    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableCreate(), request.toString());
    // table creation should succeed
    TableConfig tableConfig = getTableConfig(tableName, "OFFLINE");
    Assert.assertEquals(tableConfig.getValidationConfig().getReplicationNumber(),
        Math.max(tableReplication, TABLE_MIN_REPLICATION));

    JSONObject metadata = new JSONObject();
    metadata.put("streamType", "kafka");
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.CONSUMER_TYPE, Kafka.ConsumerType.highLevel.toString());
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.TOPIC_NAME, "fakeTopic");
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.DECODER_CLASS, "fakeClass");
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.ZK_BROKER_URL, "fakeUrl");
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.HighLevelConsumer.ZK_CONNECTION_STRING, "potato");
    metadata.put(DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE, Integer.toString(1234));
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.KAFKA_CONSUMER_PROPS_PREFIX + "." + Kafka.AUTO_OFFSET_RESET,
        "smallest");

    request = ControllerRequestBuilder.buildCreateRealtimeTableJSON(tableName, "default", "default",
        "potato", "DAYS", "DAYS", "5", tableReplication, "BalanceNumSegmentAssignmentStrategy", metadata, "fakeSchema", "fakeColumn",
        Collections.<String>emptyList(), "MMAP", false /*lowLevel*/);
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableCreate(), request.toString());
    tableConfig = getTableConfig(tableName, "REALTIME");
    Assert.assertEquals(tableConfig.getValidationConfig().getReplicationNumber(),
        Math.max(tableReplication, TABLE_MIN_REPLICATION));
    // This test can only be done via integration test
//    int replicasPerPartition = Integer.valueOf(tableConfig.getValidationConfig().getReplicasPerPartition());
//    Assert.assertEquals(replicasPerPartition, Math.max(tableReplication, TABLE_MIN_REPLICATION));
  }

  private TableConfig getTableConfig(String tableName, String type)
      throws IOException, JSONException {
    String tableConfigStr = sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableGet(tableName));
    JSONObject json = new JSONObject(tableConfigStr);
    String offlineString = json.getJSONObject(type).toString();
    return TableConfig.init(offlineString);
  }

  @Test
  public void testUpdateTableConfig()
      throws IOException, JSONException {
    String tableName = "updateTC";
    JSONObject request = ControllerRequestBuilder
        .buildCreateOfflineTableJSON(tableName, "default", "default", "potato", "DAYS", "DAYS", "5", 2,
            "BalanceNumSegmentAssignmentStrategy", Collections.<String>emptyList(), "MMAP", "v3", null);
    ControllerRequestURLBuilder controllerUrlBuilder = ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL);
    sendPostRequest(controllerUrlBuilder.forTableCreate(), request.toString());
    // table creation should succeed
    TableConfig tableConfig = getTableConfig(tableName, "OFFLINE");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeValue(), "5");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeUnit(), "DAYS");

    tableConfig.getValidationConfig().setRetentionTimeUnit("HOURS");
    tableConfig.getValidationConfig().setRetentionTimeValue("10");

    String output = null;
    output = sendPutRequest(controllerUrlBuilder.forUpdateTableConfig(tableName), tableConfig.toJSON().toString());
    JSONObject jsonResponse = new JSONObject(output);
    Assert.assertTrue(jsonResponse.has("status"));
    Assert.assertEquals(jsonResponse.getString("status"), "Success");

    TableConfig modifiedConfig = getTableConfig(tableName, "OFFLINE");
    Assert.assertEquals(modifiedConfig.getValidationConfig().getRetentionTimeUnit(), "HOURS");
    Assert.assertEquals(modifiedConfig.getValidationConfig().getRetentionTimeValue(), "10");

    // Realtime
    JSONObject metadata = new JSONObject();
    metadata.put("streamType", "kafka");
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.CONSUMER_TYPE, Kafka.ConsumerType.highLevel.toString());
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.TOPIC_NAME, "fakeTopic");
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.DECODER_CLASS, "fakeClass");
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.ZK_BROKER_URL, "fakeUrl");
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.HighLevelConsumer.ZK_CONNECTION_STRING, "potato");
    metadata.put(DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE, Integer.toString(1234));
    metadata.put(DataSource.STREAM_PREFIX + "." + Kafka.KAFKA_CONSUMER_PROPS_PREFIX + "." + Kafka.AUTO_OFFSET_RESET,
        "smallest");

    request = ControllerRequestBuilder.buildCreateRealtimeTableJSON(tableName, "default", "default",
        "potato", "DAYS", "DAYS", "5", 2, "BalanceNumSegmentAssignmentStrategy", metadata, "fakeSchema", "fakeColumn",
        Collections.<String>emptyList(), "MMAP", false /*lowLevel*/);
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableCreate(), request.toString());
    tableConfig = getTableConfig(tableName, "REALTIME");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeValue(), "5");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeUnit(), "DAYS");
    Assert.assertNull(tableConfig.getQuotaConfig());

    QuotaConfig quota = new QuotaConfig();
    quota.setStorage("10G");
    tableConfig.setQuotaConfig(quota);
    sendPutRequest(controllerUrlBuilder.forUpdateTableConfig(tableName), tableConfig.toJSON().toString());
    modifiedConfig = getTableConfig(tableName, "REALTIME");
    Assert.assertEquals(modifiedConfig.getQuotaConfig().getStorage(), "10G");
    boolean notFoundException = false;
    try {
      // table does not exist
      JSONObject jsonConfig = tableConfig.toJSON();
      jsonConfig.put("tableName", "noSuchTable_REALTIME");
      String responseStr =
          sendPutRequest(controllerUrlBuilder.forUpdateTableConfig("noSuchTable"), jsonConfig.toString());
    } catch (Exception e) {
      Assert.assertTrue(e instanceof FileNotFoundException);
      notFoundException = true;
    }
    Assert.assertTrue(notFoundException);
  }


}
