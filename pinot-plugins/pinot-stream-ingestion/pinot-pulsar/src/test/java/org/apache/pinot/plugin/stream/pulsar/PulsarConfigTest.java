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
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.junit.Assert.assertTrue;


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
    Path testFile = null;
    try {
      testFile = Files.createTempFile("test_cred_file", ".json");
      Map<String, String> streamConfigMap = getCommonStreamConfigMap();
      streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, PulsarConfig.OAUTH_ISSUER_URL),
          "http://auth.test.com");
      streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE,
              PulsarConfig.OAUTH_CREDS_FILE_PATH), "file://" + testFile.toFile().getAbsolutePath());
      streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, PulsarConfig.OAUTH_AUDIENCE),
          "urn:test:test");
      StreamConfig streamConfig = new StreamConfig(TABLE_NAME_WITH_TYPE, streamConfigMap);

      PulsarConfig pulsarConfig = new PulsarConfig(streamConfig, "testId");
      Assert.assertEquals(pulsarConfig.getIssuerUrl(), "http://auth.test.com");
      Assert.assertEquals(pulsarConfig.getCredentialsFilePath(),
          "file://" + testFile.toFile().getAbsolutePath());
      Assert.assertEquals(pulsarConfig.getAudience(), "urn:test:test");
      PulsarPartitionLevelConnectionHandler pulsarPartitionLevelConnectionHandler =
          new PulsarPartitionLevelConnectionHandler("testId", streamConfig);
      assertTrue(pulsarPartitionLevelConnectionHandler.getAuthenticationFactory(pulsarConfig).isPresent());
    } catch (Exception e) {
      Assert.fail("Should not throw exception", e);
    } finally {
      Optional.ofNullable(testFile).map(Path::toFile).ifPresent(File::delete);
    }
  }

  @Test
  public void testParsingConfigFailFileValidationForOAuth() throws Exception {
    String testFilePath = "file://path/to/file.json";
    try {
      Map<String, String> streamConfigMap = getCommonStreamConfigMap();
      streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, PulsarConfig.OAUTH_ISSUER_URL),
          "http://auth.test.com");
      streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE,
              PulsarConfig.OAUTH_CREDS_FILE_PATH),
          testFilePath);
      streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, PulsarConfig.OAUTH_AUDIENCE),
          "urn:test:test");
      StreamConfig streamConfig = new StreamConfig(TABLE_NAME_WITH_TYPE, streamConfigMap);
      PulsarConfig pulsarConfig = new PulsarConfig(streamConfig, "testId"); //will throw exception
    } catch (IllegalArgumentException mue) {
      //expected case.
      String errorMessage = String.format("Invalid credentials file path: %s. File does not exist.", testFilePath);
      Assert.assertEquals(errorMessage, mue.getMessage());
    } catch (Exception e) {
      Assert.fail("Should not throw other exception", e);
    }
  }
}
