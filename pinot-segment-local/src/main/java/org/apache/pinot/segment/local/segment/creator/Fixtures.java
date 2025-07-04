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

package org.apache.pinot.segment.local.segment.creator;

import com.google.common.collect.ImmutableList;
import java.util.Random;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;


public class Fixtures {
  private Fixtures() {
  }
  public static final int MAX_ROWS_IN_SEGMENT = 250000;
  public static final long MAX_TIME_FOR_SEGMENT_CLOSE_MS = 64368000L;
  public static final String TOPIC_NAME = "someTopic";

  //@formatter:off
  public static final String TABLE_CONFIG_JSON_TEMPLATE =
      "{"
    + "  \"metadata\":{},"
    + "  \"segmentsConfig\":{"
    + "    \"replicasPerPartition\":\"3\","
    + "    \"replication\":\"3\","
    + "    \"replicationNumber\":3,"
    + "    \"retentionTimeUnit\":\"DAYS\","
    + "    \"retentionTimeValue\":\"3\","
    + "    \"schemaName\":\"testSchema\","
    + "    \"segmentAssignmentStrategy\":\"BalanceNumSegmentAssignmentStrategy\","
    + "    \"segmentPushFrequency\":\"daily\","
    + "    \"segmentPushType\":\"APPEND\","
    + "    \"timeColumnName\":\"minutesSinceEpoch\","
    + "    \"timeType\":\"MINUTES\""
    + "  },"
    + "  \"tableIndexConfig\":{"
    + "    \"invertedIndexColumns\":[],"
    + "    \"lazyLoad\":\"false\","
    + "    \"loadMode\":\"HEAP\","
    + "    \"segmentFormatVersion\":null,"
    + "    \"sortedColumn\":[],"
    + "    \"streamConfigs\":{"
    + "      \"realtime.segment.flush.threshold.rows\":\"" + MAX_ROWS_IN_SEGMENT + "\","
    + "      \"realtime.segment.flush.threshold.time\":\"" + MAX_TIME_FOR_SEGMENT_CLOSE_MS + "\","
    + "      \"stream.fakeStream.broker.list\":\"broker:7777\","
    + "      \"stream.fakeStream.consumer.prop.auto.offset.reset\":\"smallest\","
    + "      \"stream.fakeStream.consumer.factory.class.name\":\"%s\","
    + "      \"stream.fakeStream.decoder.class.name\":\"%s\","
    + "      \"stream.fakeStream.decoder.prop.schema.registry.rest.url\":\"http://1.2.3.4:1766/schemas\","
    + "      \"stream.fakeStream.decoder.prop.schema.registry.schema.name\":\"UnknownSchema\","
    + "      \"stream.fakeStream.hlc.zk.connect.string\":\"zoo:2181/kafka-queuing\","
    + "      \"stream.fakeStream.topic.name\":\"" + TOPIC_NAME + "\","
    + "      \"stream.fakeStream.zk.broker.url\":\"kafka-broker:2181/kafka-queuing\","
    + "      \"streamType\":\"fakeStream\""
    + "    }"
    + "  },"
    + "  \"tableName\":\"Coffee_REALTIME\","
    + "  \"tableType\":\"realtime\","
    + "  \"tenants\":{"
    + "    \"broker\":\"shared\","
    + "    \"server\":\"server-1\""
    + "  },"
    + "  \"upsertConfig\":{"
    + "    \"mode\":\"FULL\""
    + "  }"
    + "}";
  public static final String SCHEMA_JSON =
      "{"
    + "  \"schemaName\":\"testSchema\","
    + "  \"metricFieldSpecs\":[{\"name\":\"m\",\"dataType\":\"LONG\"}],"
    + "  \"dimensionFieldSpecs\":[{\"name\":\"d\",\"dataType\":\"STRING\",\"singleValueField\":true}],"
    + "  \"timeFieldSpec\":{"
    + "    \"incomingGranularitySpec\":{"
    + "      \"dataType\":\"LONG\","
    + "      \"timeType\":\"MINUTES\","
    + "      \"name\":\"minutesSinceEpoch\""
    + "    },"
    + "    \"defaultNullValue\":12345"
    + "  },"
    + "  \"primaryKeyColumns\": [\"event_id\"]"
    + "}";
  //@formatter:on
  public static TableConfig createTableConfig(String consumerFactoryClass, String decoderFactoryClass)
      throws Exception {
    return JsonUtils.stringToObject(String.format(TABLE_CONFIG_JSON_TEMPLATE, consumerFactoryClass,
        decoderFactoryClass), TableConfig.class);
  }

  public static Schema createSchema() throws Exception {
    return Schema.fromString(SCHEMA_JSON);
  }

  public static GenericRow createSingleRow(long randomSeed) {
    Random rand = new Random(randomSeed);
    int colValue = rand.nextInt(Integer.MAX_VALUE);
    GenericRow retVal = new GenericRow();
    retVal.putValue("m", colValue);
    int nextVal = rand.nextInt(Integer.MAX_VALUE);
    retVal.putValue("d", nextVal + "_d_" + nextVal);
    retVal.putValue("event_id", rand.nextInt(Integer.MAX_VALUE));
    return retVal;
  }

  public static GenericRow createInvalidSingleRow(long randomSeed) {
    Random rand = new Random(randomSeed);
    int colValue = rand.nextInt(Integer.MAX_VALUE);
    GenericRow retVal = new GenericRow();
    retVal.putValue("m", colValue + "_d_" + colValue);
    int nextVal = rand.nextInt(Integer.MAX_VALUE);
    retVal.putValue("d", nextVal + "_d_" + nextVal);
    retVal.putValue("event_id", rand.nextInt(Integer.MAX_VALUE));
    return retVal;
  }

  public static GenericRow createMultipleRow(long randomSeed) {
    Random rand = new Random(randomSeed);
    GenericRow firstRow = createSingleRow(randomSeed);
    GenericRow secondRow = createSingleRow(rand.nextInt(Integer.MAX_VALUE));
    GenericRow thirdRow = createSingleRow(rand.nextInt(Integer.MAX_VALUE));
    GenericRow retVal = new GenericRow();
    retVal.putValue(GenericRow.MULTIPLE_RECORDS_KEY, ImmutableList.of(firstRow, secondRow, thirdRow));
    return retVal;
  }

  public static GenericRow createMultipleRowPartialFailure(long randomSeed) {
    Random rand = new Random(randomSeed);
    GenericRow firstRow = createSingleRow(randomSeed);
    GenericRow secondRow = createInvalidSingleRow(rand.nextInt(Integer.MAX_VALUE));
    GenericRow thirdRow = createSingleRow(rand.nextInt(Integer.MAX_VALUE));
    GenericRow retVal = new GenericRow();
    retVal.putValue(GenericRow.MULTIPLE_RECORDS_KEY, ImmutableList.of(firstRow, secondRow, thirdRow));
    return retVal;
  }
}
