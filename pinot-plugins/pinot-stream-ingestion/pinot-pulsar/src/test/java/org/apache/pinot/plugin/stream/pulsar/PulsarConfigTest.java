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
package org.apache.pinot.plugin.stream.pulsar;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PulsarConfigTest {
  public static final String TABLE_NAME_WITH_TYPE = "tableName_REALTIME";

  public static final String STREAM_TYPE = "pulsar";
  public static final String STREAM_PULSAR_BROKER_LIST = "pulsar://localhost:6650";
  public static final String STREAM_PULSAR_CONSUMER_TYPE = "simple";
  Map<String, String> getCommonStreamConfigMap() {
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", STREAM_TYPE);
    streamConfigMap.put("stream.pulsar.consumer.type", STREAM_PULSAR_CONSUMER_TYPE);
    streamConfigMap.put("stream.pulsar.topic.name", "test-topic");
    streamConfigMap.put("stream.pulsar.bootstrap.servers", STREAM_PULSAR_BROKER_LIST);
    streamConfigMap.put("stream.pulsar.consumer.prop.auto.offset.reset", "smallest");
    streamConfigMap.put("stream.pulsar.consumer.factory.class.name", PulsarConsumerFactory.class.getName());
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_FETCH_TIMEOUT_MILLIS),
        "1000");
    streamConfigMap.put("stream.pulsar.decoder.class.name", "decoderClass");
    return streamConfigMap;
  }

  @Test
  public void testParsingMetadataConfigWithConfiguredFields() throws Exception {
    Map<String, String> streamConfigMap = getCommonStreamConfigMap();
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.METADATA_POPULATE), "true");
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, PulsarConfig.METADATA_FIELDS),
        "messageId,messageIdBytes, publishTime, eventTime, key, topicName, ");
    StreamConfig streamConfig = new StreamConfig(TABLE_NAME_WITH_TYPE, streamConfigMap);
    PulsarConfig pulsarConfig = new PulsarConfig(streamConfig, "testId");
    Set<PulsarStreamMessageMetadata.PulsarMessageMetadataValue> metadataFieldsToExtract =
        pulsarConfig.getMetadataFields();
    Assert.assertEquals(metadataFieldsToExtract.size(), 6);
    Assert.assertTrue(metadataFieldsToExtract.containsAll(ImmutableList.of(
        PulsarStreamMessageMetadata.PulsarMessageMetadataValue.MESSAGE_ID,
        PulsarStreamMessageMetadata.PulsarMessageMetadataValue.MESSAGE_ID_BYTES_B64,
        PulsarStreamMessageMetadata.PulsarMessageMetadataValue.PUBLISH_TIME,
        PulsarStreamMessageMetadata.PulsarMessageMetadataValue.EVENT_TIME,
        PulsarStreamMessageMetadata.PulsarMessageMetadataValue.MESSAGE_KEY,
        PulsarStreamMessageMetadata.PulsarMessageMetadataValue.TOPIC_NAME)));
  }

  @Test
  public void testParsingMetadataConfigWithoutConfiguredFields() throws Exception {
    Map<String, String> streamConfigMap = getCommonStreamConfigMap();
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.METADATA_POPULATE),
        "true");
    StreamConfig streamConfig = new StreamConfig(TABLE_NAME_WITH_TYPE, streamConfigMap);
    PulsarConfig pulsarConfig = new PulsarConfig(streamConfig, "testId");
    Set<PulsarStreamMessageMetadata.PulsarMessageMetadataValue> metadataFieldsToExtract =
        pulsarConfig.getMetadataFields();
    Assert.assertEquals(metadataFieldsToExtract.size(), 0);
  }

  @Test
  public void testParsingNoMetadataConfig() throws Exception {
    Map<String, String> streamConfigMap = getCommonStreamConfigMap();
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.METADATA_POPULATE),
        "false");
    StreamConfig streamConfig = new StreamConfig(TABLE_NAME_WITH_TYPE, streamConfigMap);
    PulsarConfig pulsarConfig = new PulsarConfig(streamConfig, "testId");
    Assert.assertFalse(pulsarConfig.isPopulateMetadata());
    Set<PulsarStreamMessageMetadata.PulsarMessageMetadataValue> metadataFieldsToExtract =
        pulsarConfig.getMetadataFields();
    Assert.assertEquals(metadataFieldsToExtract.size(), 0);
  }

  @Test
  public void testParsingNoMetadataConfigWithConfiguredFields() throws Exception {
    Map<String, String> streamConfigMap = getCommonStreamConfigMap();
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.METADATA_POPULATE),
        "false");
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, PulsarConfig.METADATA_FIELDS),
        "messageId,messageIdBytes, publishTime, eventTime, key, topicName, ");
    StreamConfig streamConfig = new StreamConfig(TABLE_NAME_WITH_TYPE, streamConfigMap);
    PulsarConfig pulsarConfig = new PulsarConfig(streamConfig, "testId");
    Set<PulsarStreamMessageMetadata.PulsarMessageMetadataValue> metadataFieldsToExtract =
        pulsarConfig.getMetadataFields();
    Assert.assertFalse(pulsarConfig.isPopulateMetadata());
    Assert.assertEquals(metadataFieldsToExtract.size(), 0);
  }

  @Test
  public void testParsingConfigForOAuth() throws Exception {
    Map<String, String> streamConfigMap = getCommonStreamConfigMap();
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, PulsarConfig.OAUTH_ISSUER_URL),
        "http://auth.test.com");
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, PulsarConfig.OAUTH_CREDS_FILE_PATH),
        "file:///tmp/creds.json");
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, PulsarConfig.OAUTH_AUDIENCE),
        "urn:test:test");
    StreamConfig streamConfig = new StreamConfig(TABLE_NAME_WITH_TYPE, streamConfigMap);
    PulsarConfig pulsarConfig = new PulsarConfig(streamConfig, "testId");
    Assert.assertEquals(pulsarConfig.getIssuerUrl(), "http://auth.test.com");
    Assert.assertEquals(pulsarConfig.getCredentialsFilePath(), "file:///tmp/creds.json");
    Assert.assertEquals(pulsarConfig.getAudience(), "urn:test:test");
  }
}
