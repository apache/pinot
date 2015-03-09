package com.linkedin.pinot.controller.helix;

import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.resource.RealtimeDataResourceZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.ResourceType;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource.Realtime.StreamType;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.controller.helix.core.ZKMetadataUtils;


public class ControllerPojoTest {

  @Test
  public void realtimePojoTest() throws JSONException {
    BaseJsonNode metadataJson = getMetadataJson();

    DataResource realtimeDataResource = new DataResource("create", "testResource0", ResourceType.OFFLINE.toString(), "testTable0",
        "daysSinceEpoch", "daysSinceEpoch", 3, 2, "DAYS", "7", "daily", "BalanceNumSegmentAssignmentStrategy", "testBrokerTag",
        1, metadataJson);
    RealtimeDataResourceZKMetadata realtimeDataResourceZKMetadata = ZKMetadataUtils.getRealtimeDataResourceMetadata(realtimeDataResource);

    Assert.assertEquals(realtimeDataResourceZKMetadata.getStreamType(), StreamType.kafka);
    Assert.assertEquals(realtimeDataResourceZKMetadata.getDataSchema().toString(), Schema.getSchemaFromMap(realtimeDataResourceZKMetadata.getDataSchema().toMap()).toString());
  }

  private BaseJsonNode getMetadataJson() {
    ObjectNode metadataJson = new ObjectNode(new JsonNodeFactory(false));

    metadataJson.set("schema.dim0.fieldType", new TextNode("dimension"));
    metadataJson.set("schema.dim0.columnName", new TextNode("dim0"));
    metadataJson.set("schema.dim0.dataType", new TextNode("INT"));
    metadataJson.set("schema.dim0.isSingleValue", new TextNode("true"));
    metadataJson.set("schema.dim0.delimeter", new TextNode("null"));

    metadataJson.set("schema.dim1.fieldType", new TextNode("dimension"));
    metadataJson.set("schema.dim1.columnName", new TextNode("dim1"));
    metadataJson.set("schema.dim1.dataType", new TextNode("FLOAT"));
    metadataJson.set("schema.dim1.isSingleValue", new TextNode("true"));
    metadataJson.set("schema.dim1.delimeter", new TextNode("null"));

    metadataJson.set(StringUtil.join(".", Helix.DataSource.STREAM, Helix.DataSource.Realtime.Kafka.CONSUMER_TYPE),
        new TextNode(Helix.DataSource.Realtime.Kafka.ConsumerType.highLevel.toString()));
    metadataJson.set(StringUtil.join(".", Helix.DataSource.STREAM, Helix.DataSource.Realtime.Kafka.TOPIC_NAME),
        new TextNode("MirrorDecoratedProfileViewEvent"));
    metadataJson.set(StringUtil.join(".", Helix.DataSource.STREAM, Helix.DataSource.Realtime.Kafka.DECODER_CLASS),
        new TextNode("com.linkedin.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder"));
    metadataJson.set(StringUtil.join(".", Helix.DataSource.STREAM, Helix.DataSource.Realtime.Kafka.HighLevelConsumer.GROUP_ID),
        new TextNode("testGroupId"));
    metadataJson.set(StringUtil.join(".", Helix.DataSource.STREAM, Helix.DataSource.Realtime.Kafka.HighLevelConsumer.ZK_CONNECTION_STRING),
        new TextNode("zk-eat1-kafka.corp.linkedin.com:12913/kafka-aggregate-tracking"));
    metadataJson.set(StringUtil.join(".", Helix.DataSource.STREAM, Helix.DataSource.Realtime.Kafka.getDecoderPropertyKeyFor("schema.registry.rest.url")),
        new TextNode("http://eat1-ei2-schema-vip-z.stg.linkedin.com:10252/schemaRegistry/schemas"));

    metadataJson.set(Helix.DataSource.Realtime.STREAM_TYPE, new TextNode(StreamType.kafka.toString()));
    return metadataJson;
  }

  @Test
  public void offlinePojoTest() {

  }
}
