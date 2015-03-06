package com.linkedin.pinot.common.realtime;

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.Schema.SchemaBuilder;
import com.linkedin.pinot.common.metadata.resource.RealtimeDataResourceMetadata;
import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource.Realtime.StreamType;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSourceRequestType;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.ResourceType;
import com.linkedin.pinot.common.utils.StringUtil;


public class RealtimeDataSourceTest {

  @Test
  public void DataSourceTest() {

    ZNRecord record = new ZNRecord("testResource");
    record.setSimpleFields(getTestDataResource());
    RealtimeDataResourceMetadata realtimeDataResource = new RealtimeDataResourceMetadata(record);
    realtimeDataResource.getDataSchema();
    Assert.assertEquals(realtimeDataResource.getResourceName(), BrokerRequestUtils.getRealtimeResourceNameForResource("testResource"));
    Assert.assertEquals(realtimeDataResource.getStreamType(), StreamType.kafka);
    System.out.println(realtimeDataResource.getDataSchema().toString());
    Assert.assertEquals(realtimeDataResource.getDataSchema().toString(), Schema.getSchemaFromMap(getTestDataResource()).toString());
    System.out.println(realtimeDataResource.getStreamMetadata().toString());
    Assert.assertEquals(realtimeDataResource.getStreamMetadata().toString(), new KafkaStreamMetadata(getTestKafkaStreamConfig()).toString());
    RealtimeDataResourceMetadata replicaRealtimeDataResourceMetadata = new RealtimeDataResourceMetadata(realtimeDataResource.toZnRecord());
    Assert.assertTrue(realtimeDataResource.equals(replicaRealtimeDataResourceMetadata));
  }

  private Map<String, String> getTestDataResource() {
    final Map<String, String> ret = new HashMap<String, String>();
    ret.put(Helix.DataSource.REQUEST_TYPE, DataSourceRequestType.CREATE);
    ret.put(Helix.DataSource.RESOURCE_NAME, "testResource");
    ret.put(Helix.DataSource.RESOURCE_TYPE, ResourceType.REALTIME.toString());
    ret.put(Helix.DataSource.TABLE_NAME, "testTable");
    ret.put(Helix.DataSource.TIME_COLUMN_NAME, "daysSinceEpoch");
    ret.put(Helix.DataSource.TIME_TYPE, "daysSinceEpoch");
    ret.put(Helix.DataSource.NUMBER_OF_DATA_INSTANCES, String.valueOf(6));
    ret.put(Helix.DataSource.NUMBER_OF_COPIES, String.valueOf(3));
    ret.put(Helix.DataSource.RETENTION_TIME_UNIT, "DAYS");
    ret.put(Helix.DataSource.RETENTION_TIME_VALUE, "7");
    ret.put(Helix.DataSource.PUSH_FREQUENCY, "daily");
    ret.put(Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY, "Random");
    ret.put(Helix.DataSource.BROKER_TAG_NAME, "testBroker");
    ret.put(Helix.DataSource.NUMBER_OF_BROKER_INSTANCES, String.valueOf(2));
    ret.putAll(getTestSchema().toMap());
    ret.put(StringUtil.join(".", Helix.DataSource.METADATA, Helix.DataSource.Realtime.STREAM_TYPE), Helix.DataSource.Realtime.StreamType.kafka.toString());
    ret.putAll(getTestKafkaStreamConfig());
    return ret;
  }

  private Map<String, String> getTestKafkaStreamConfig() {
    Map<String, String> streamMap = new HashMap<String, String>();

    streamMap.put(StringUtil.join(".", Helix.DataSource.METADATA, Helix.DataSource.Realtime.Kafka.CONSUMER_TYPE),
        Helix.DataSource.Realtime.Kafka.ConsumerType.highLevel.toString());
    streamMap.put(StringUtil.join(".", Helix.DataSource.METADATA, Helix.DataSource.Realtime.Kafka.TOPIC_NAME),
        "MirrorDecoratedProfileViewEvent");
    streamMap.put(StringUtil.join(".", Helix.DataSource.METADATA, Helix.DataSource.Realtime.Kafka.DECODER_CLASS),
        "com.linkedin.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder");
    streamMap.put(StringUtil.join(".", Helix.DataSource.METADATA, Helix.DataSource.Realtime.Kafka.HighLevelConsumer.GROUP_ID),
        "testGroupId");
    streamMap.put(StringUtil.join(".", Helix.DataSource.METADATA, Helix.DataSource.Realtime.Kafka.HighLevelConsumer.ZK_CONNECTION_STRING),
        "zk-eat1-kafka.corp.linkedin.com:12913/kafka-aggregate-tracking");
    streamMap.put(StringUtil.join(".", Helix.DataSource.METADATA, Helix.DataSource.Realtime.Kafka.getDecoderPropertyKeyFor("schema.registry.rest.url")),
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
