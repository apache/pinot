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
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.common.utils.ZkStarter;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Integration test that creates a Kafka broker, creates a Pinot cluster that consumes from Kafka and queries Pinot.
 *
 */
public class LLCPartitioningIntegrationTest extends RealtimeClusterIntegrationTest {
  private static int KAFKA_PARTITION_COUNT = 2;
  private static final String KAFKA_PARTITIONING_KEY = "AirlineID";
  private static final String SCHEMA_FILE_NAME = "On_Time_On_Time_Performance_2014_100k_subset_nonulls_outgoing.schema";

  protected void setUpTable(String tableName, String timeColumnName, String timeColumnType, String kafkaZkUrl,
      String kafkaTopic, File schemaFile, File avroFile)
      throws Exception {
    Schema schema = Schema.fromFile(schemaFile);
    addSchema(schemaFile, schema.getSchemaName());
    List<String> noDictionaryColumns = Arrays.asList("NASDelay", "ArrDelayMinutes", "DepDelayMinutes");
    // TODO: add SegmentPartitionConfig here
    addLLCRealtimeTable(tableName, timeColumnName, timeColumnType, -1, "", KafkaStarterUtils.DEFAULT_KAFKA_BROKER,
        kafkaTopic, schema.getSchemaName(), null, null, avroFile, ROW_COUNT_FOR_REALTIME_SEGMENT_FLUSH, "Carrier",
        Collections.<String>emptyList(), "mmap", noDictionaryColumns, null);
  }

  protected void createKafkaTopic(String kafkaTopic, String zkStr) {
    KafkaStarterUtils.createTopic(kafkaTopic, zkStr, KAFKA_PARTITION_COUNT);
    partitioningKey = KAFKA_PARTITIONING_KEY;
  }


  @Override
  public File getSchemaFile() {
    return new File(OfflineClusterIntegrationTest.class.getClassLoader()
        .getResource(SCHEMA_FILE_NAME).getFile());
  }

  @Test
  public void testSegmentFlushSize() {
    ZkClient zkClient = new ZkClient(ZkStarter.DEFAULT_ZK_STR, 10000);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    String zkPath = "/LLCPartitioningIntegrationTest/PROPERTYSTORE/SEGMENTS/mytable_REALTIME";
    List<String> segmentNames =
        zkClient.getChildren(zkPath);
    for (String segmentName : segmentNames) {
      ZNRecord znRecord = zkClient.<ZNRecord>readData(zkPath + "/" + segmentName);
      Assert.assertEquals(znRecord.getSimpleField(CommonConstants.Segment.FLUSH_THRESHOLD_SIZE),
          Integer.toString(ROW_COUNT_FOR_REALTIME_SEGMENT_FLUSH / KAFKA_PARTITION_COUNT), "Segment " + segmentName +
              " does not have the expected flush size");
    }
    zkClient.close();
  }

  @Override
  @Test(enabled = true)
  public void testHardcodedQueries()
      throws Exception {

    // Here are some sample queries.
    String query;
    query = "SELECT * FROM mytable limit 1";
    JSONObject response = postQuery(query);

    JSONObject selectionResults = ((JSONObject) response.get("selectionResults"));
    JSONArray columns = (JSONArray) selectionResults.get("columns");
    int ncols = columns.length();
    int indexOfDate = -1;
    for (int i = 0; i < ncols; i++) {
      if (columns.get(i).equals("Date")) {
        indexOfDate = i;
      }
    }

    Assert.assertTrue((indexOfDate != -1), "Date column not found" );

    JSONArray results = (JSONArray) ((JSONArray) selectionResults.get("results")).get(0);
    String date = (String) results.get(indexOfDate);

    long timeMS = Long.parseLong(date);
    long timeDay = TimeUnit.MILLISECONDS.toDays(timeMS);
    Assert.assertTrue((timeDay > 10000L) && (timeDay < 20000L));
  }

  @Override
  @Test(enabled = false)
  public void testGeneratedQueriesWithMultiValues()
      throws Exception {
    // Ignored.
  }

  @Override
  @Test(enabled = false)
  public void testHardcodedQuerySet()
      throws Exception {
    // Ignored.
  }

  @Override
  protected int getKafkaBrokerCount() {
    return 2;
  }
}
