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
package org.apache.pinot.common.utils.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamLevelConsumer;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link TableConfigUtils} class.
 */
public class TableConfigUtilsTest {

  private static final String TABLE_NAME = "testTable";

  /**
   * Test the {@link TableConfigUtils#convertFromLegacyTableConfig(TableConfig)} utility.
   * <ul>
   *   <li>Creates a Table Config setting deprecated fields in Indexing Config.</li>
   *   <li>Asserts that the utility can convert these fields into Ingestion Config.</li>
   * </ul>
   */
  @Test
  public void testIndexToIngestionConfigConversion() {
    String expectedPushFrequency = "HOURLY";
    String expectedPushType = "APPEND";

    Map<String, String> expectedStreamConfigsMap = getTestStreamConfigs();
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setSegmentPushFrequency(expectedPushFrequency).setSegmentPushType(expectedPushType)
        .setStreamConfigs(expectedStreamConfigsMap).build();

    // Before conversion, the ingestion config should be null.
    Assert.assertNull(tableConfig.getIngestionConfig());

    // Perform conversion.
    TableConfigUtils.convertFromLegacyTableConfig(tableConfig);

    // After conversion, assert that the configs are transferred ingestionConfig.
    BatchIngestionConfig batchIngestionConfig = tableConfig.getIngestionConfig().getBatchIngestionConfig();
    Assert.assertEquals(batchIngestionConfig.getSegmentIngestionFrequency(), expectedPushFrequency);
    Assert.assertEquals(batchIngestionConfig.getSegmentIngestionType(), expectedPushType);

    Map<String, String> actualStreamConfigsMap =
        tableConfig.getIngestionConfig().getStreamIngestionConfig().getStreamConfigMaps().get(0);
    Assert.assertEquals(actualStreamConfigsMap, expectedStreamConfigsMap);
  }

  /**
   * Helper method to create a test StreamConfigs map.
   * @return Map containing Stream Configs
   */
  private Map<String, String> getTestStreamConfigs() {
    String streamType = "testStream";
    String topic = "testTopic";
    String consumerType = StreamConfig.ConsumerType.LOWLEVEL.toString();
    String consumerFactoryClass = TestStreamConsumerFactory.class.getName();
    String decoderClass = TestStreamMessageDecoder.class.getName();

    // All mandatory properties set
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, "streamType");
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME),
            topic);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
            consumerType);
    streamConfigMap.put(StreamConfigProperties
            .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS),
        consumerFactoryClass);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
            decoderClass);

    return streamConfigMap;
  }

  private class TestStreamMessageDecoder implements StreamMessageDecoder<byte[]> {
    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
        throws Exception {

    }

    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
      return null;
    }

    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      return null;
    }
  }

  private class TestStreamConsumerFactory extends StreamConsumerFactory {
    @Override
    public PartitionLevelConsumer createPartitionLevelConsumer(String clientId, int partition) {
      return null;
    }

    @Override
    public StreamLevelConsumer createStreamLevelConsumer(String clientId, String tableName, Set<String> fieldsToRead,
        String groupId) {
      return null;
    }

    @Override
    public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
      return null;
    }

    @Override
    public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
      return null;
    }
  }
}
