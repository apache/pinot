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

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ZkStarter;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import java.util.List;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Integration test that creates a Kafka broker, creates a Pinot cluster that consumes from Kafka and queries Pinot.
 *
 */
public class LLCRealtimeClusterIntegrationTest extends RealtimeClusterIntegrationTest {
  private static int KAFKA_PARTITION_COUNT = 2;

  protected void setUpTable(String tableName, String timeColumnName, String timeColumnType, String kafkaZkUrl,
      String kafkaTopic, File schemaFile, File avroFile) throws Exception {
    Schema schema = Schema.fromFile(schemaFile);
    addSchema(schemaFile, schema.getSchemaName());
    addLLCRealtimeTable(tableName, timeColumnName, timeColumnType, -1, "", KafkaStarterUtils.DEFAULT_KAFKA_BROKER, kafkaTopic, schema.getSchemaName(),
        null, null, avroFile, ROW_COUNT_FOR_REALTIME_SEGMENT_FLUSH, "Carrier", Collections.<String>emptyList(), "mmap",
        null);
  }

  protected void createKafkaTopic(String kafkaTopic, String zkStr) {
    KafkaStarterUtils.createTopic(kafkaTopic, zkStr, KAFKA_PARTITION_COUNT);
  }

  @Test
  public void testSegmentFlushSize() {
    ZkClient zkClient = new ZkClient(ZkStarter.DEFAULT_ZK_STR, 10000);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    String zkPath = "/LLCRealtimeClusterIntegrationTest/PROPERTYSTORE/SEGMENTS/mytable_REALTIME";
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
  protected int getKafkaBrokerCount() {
    return 2;
  }
}
