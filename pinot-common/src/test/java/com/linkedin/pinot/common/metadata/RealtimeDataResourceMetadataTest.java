/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.helix.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.Schema.SchemaBuilder;
import com.linkedin.pinot.common.metadata.resource.RealtimeDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;
import com.linkedin.pinot.common.metadata.stream.StreamMetadata;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource.Realtime.StreamType;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.ResourceType;
import com.linkedin.pinot.common.utils.StringUtil;


public class RealtimeDataResourceMetadataTest {

  @Test
  public void RealtimeDataResourceMetadataCovertionTest() {
    ZNRecord znRecordRaw = getTestZNRecord();
    RealtimeDataResourceZKMetadata realtimeDataResourceZKMetadataFromZNRecord = new RealtimeDataResourceZKMetadata(znRecordRaw);
    RealtimeDataResourceZKMetadata realtimeDataResourceZKMetadata = getTestRealtimeDataResourceZKMetadata();
    ZNRecord znRecordFromMetadata = realtimeDataResourceZKMetadata.toZNRecord();
    Assert.assertTrue(MetadataUtils.comparisonZNRecords(znRecordRaw, znRecordFromMetadata));
    Assert.assertTrue(realtimeDataResourceZKMetadataFromZNRecord.equals(realtimeDataResourceZKMetadata));
    Assert.assertTrue(MetadataUtils.comparisonZNRecords(znRecordRaw, new RealtimeDataResourceZKMetadata(znRecordRaw).toZNRecord()));
    Assert.assertTrue(realtimeDataResourceZKMetadata.equals(new RealtimeDataResourceZKMetadata(realtimeDataResourceZKMetadata.toZNRecord())));

  }

  @Test
  public void schemaTest() {
    Schema schema = getTestSchema();
    Schema newSchema = Schema.getSchemaFromMap(schema.toMap());
    Assert.assertEquals(schema.toString(), newSchema.toString());
  }

  @Test
  public void streamMetadataTest() {
    StreamMetadata streamMetadata = new KafkaStreamMetadata(getTestKafkaStreamConfig());
    StreamMetadata newStreamMetadata = new KafkaStreamMetadata(streamMetadata.toMap());
    Assert.assertEquals(streamMetadata.toString(), newStreamMetadata.toString());
  }

  private ZNRecord getTestZNRecord() {
    ZNRecord record = new ZNRecord("testResource_R");
    Map<String, String> fieldMap = getTestSchema().toMap();
    fieldMap.putAll(getTestKafkaStreamConfig());
    record.setSimpleFields(fieldMap);
    record.setSimpleField(Helix.DataSource.RESOURCE_NAME, "testResource");
    record.setEnumField(Helix.DataSource.RESOURCE_TYPE, ResourceType.REALTIME);
    List<String> sortedColumnList = new ArrayList<String>();
    sortedColumnList.add("sortColumn0");
    sortedColumnList.add("sortColumn1");
    sortedColumnList.add("sortColumn2");
    record.setListField(Helix.DataSource.SORTED_COLUMN_LIST, sortedColumnList);
    record.setSimpleField(Helix.DataSource.TIME_COLUMN_NAME, "daysSinceEpoch");
    record.setSimpleField(Helix.DataSource.TIME_TYPE, "daysSinceEpoch");
    record.setIntField(Helix.DataSource.NUMBER_OF_DATA_INSTANCES, 6);
    record.setIntField(Helix.DataSource.NUMBER_OF_COPIES, 3);
    record.setEnumField(Helix.DataSource.RETENTION_TIME_UNIT, TimeUnit.DAYS);
    record.setIntField(Helix.DataSource.RETENTION_TIME_VALUE, 7);
    record.setSimpleField(Helix.DataSource.BROKER_TAG_NAME, "testBroker");
    record.setIntField(Helix.DataSource.NUMBER_OF_BROKER_INSTANCES, 2);
    record.setMapField(Helix.DataSource.METADATA, new HashMap<String, String>());
    record.setEnumField(Helix.DataSource.Realtime.STREAM_TYPE, Helix.DataSource.Realtime.StreamType.kafka);
    return record;
  }

  private RealtimeDataResourceZKMetadata getTestRealtimeDataResourceZKMetadata() {
    RealtimeDataResourceZKMetadata realtimeDataResourceZKMetadata = new RealtimeDataResourceZKMetadata();
    realtimeDataResourceZKMetadata.setResourceName("testResource");
    List<String> sortedColumnList = new ArrayList<String>();
    sortedColumnList.add("sortColumn0");
    sortedColumnList.add("sortColumn1");
    sortedColumnList.add("sortColumn2");
    realtimeDataResourceZKMetadata.setSortedColumns(sortedColumnList);
    realtimeDataResourceZKMetadata.setTimeColumnName("daysSinceEpoch");
    realtimeDataResourceZKMetadata.setTimeType("daysSinceEpoch");
    realtimeDataResourceZKMetadata.setNumDataInstances(6);
    realtimeDataResourceZKMetadata.setNumDataReplicas(3);
    realtimeDataResourceZKMetadata.setRetentionTimeUnit(TimeUnit.DAYS);
    realtimeDataResourceZKMetadata.setRetentionTimeValue(7);
    realtimeDataResourceZKMetadata.setBrokerTag("testBroker");
    realtimeDataResourceZKMetadata.setNumBrokerInstance(2);
    realtimeDataResourceZKMetadata.setMetadata(new HashMap<String, String>());
    realtimeDataResourceZKMetadata.setDataSchema(getTestSchema());
    realtimeDataResourceZKMetadata.setStreamType(StreamType.kafka);
    realtimeDataResourceZKMetadata.setStreamMetadata(new KafkaStreamMetadata(getTestKafkaStreamConfig()));
    return realtimeDataResourceZKMetadata;
  }

  private Map<String, String> getTestKafkaStreamConfig() {
    Map<String, String> streamMap = new HashMap<String, String>();

    streamMap.put(StringUtil.join(".", Helix.DataSource.STREAM_PREFIX, Helix.DataSource.Realtime.Kafka.CONSUMER_TYPE),
        Helix.DataSource.Realtime.Kafka.ConsumerType.highLevel.toString());
    streamMap.put(StringUtil.join(".", Helix.DataSource.STREAM_PREFIX, Helix.DataSource.Realtime.Kafka.TOPIC_NAME),
        "MirrorDecoratedProfileViewEvent");
    streamMap.put(StringUtil.join(".", Helix.DataSource.STREAM_PREFIX, Helix.DataSource.Realtime.Kafka.DECODER_CLASS),
        "com.linkedin.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder");
    streamMap.put(StringUtil.join(".", Helix.DataSource.STREAM_PREFIX, Helix.DataSource.Realtime.Kafka.ZK_BROKER_URL),
        "zk-eat1-kafka.corp.linkedin.com:12913/kafka-aggregate");
    streamMap.put(StringUtil.join(".", Helix.DataSource.STREAM_PREFIX, Helix.DataSource.Realtime.Kafka.HighLevelConsumer.GROUP_ID),
        "testGroupId");
    streamMap.put(StringUtil.join(".", Helix.DataSource.STREAM_PREFIX, Helix.DataSource.Realtime.Kafka.HighLevelConsumer.ZK_CONNECTION_STRING),
        "zk-eat1-kafka.corp.linkedin.com:12913/kafka-aggregate-tracking");
    streamMap.put(StringUtil.join(".", Helix.DataSource.STREAM_PREFIX, Helix.DataSource.Realtime.Kafka.getDecoderPropertyKeyFor("schema.registry.rest.url")),
        "http://eat1-ei2-schema-vip-z.stg.linkedin.com:10252/schemaRegistry/schemas");

    return streamMap;
  }

  private Schema getTestSchema() {
    SchemaBuilder testSchemaBuilder = new SchemaBuilder();
    testSchemaBuilder.addSingleValueDimension("dim0", DataType.INT);
    testSchemaBuilder.addSingleValueDimension("dim1", DataType.FLOAT);
    testSchemaBuilder.addSingleValueDimension("dim2", DataType.LONG);
    testSchemaBuilder.addSingleValueDimension("dim3", DataType.STRING);
    testSchemaBuilder.addMultiValueDimension("dim4", DataType.LONG_ARRAY, ",");
    testSchemaBuilder.addMultiValueDimension("dim5", DataType.STRING_ARRAY, ":");
    return testSchemaBuilder.build();
  }
}
