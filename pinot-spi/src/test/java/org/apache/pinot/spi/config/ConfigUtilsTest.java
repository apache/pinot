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
package org.apache.pinot.spi.config;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ConfigUtilsTest {

  @Test
  public void testIndexing()
      throws Exception {
    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setLoadMode("${LOAD_MODE}");
    indexingConfig.setAggregateMetrics(true);
    List<String> invertedIndexColumns = Arrays.asList("a", "b", "c");
    indexingConfig.setInvertedIndexColumns(invertedIndexColumns);
    List<String> sortedColumn = Arrays.asList("d", "e", "f");
    indexingConfig.setSortedColumn(sortedColumn);
    List<String> onHeapDictionaryColumns = Arrays.asList("x", "y", "z");
    indexingConfig.setOnHeapDictionaryColumns(onHeapDictionaryColumns);
    List<String> bloomFilterColumns = Arrays.asList("a", "b");
    indexingConfig.setBloomFilterColumns(bloomFilterColumns);
    Map<String, String> noDictionaryConfig = new HashMap<>();
    noDictionaryConfig.put("a", "SNAPPY");
    noDictionaryConfig.put("b", "PASS_THROUGH");
    indexingConfig.setNoDictionaryConfig(noDictionaryConfig);
    List<String> varLengthDictionaryColumns = Arrays.asList("a", "x", "z");
    indexingConfig.setVarLengthDictionaryColumns(varLengthDictionaryColumns);

    String streamType = "fakeStream";
    String topic = "fakeTopic";
    String consumerType = "simple";
    String tableName = "fakeTable_REALTIME";
    String defaultConsumerFactoryClass = "org.apache.pinot.plugin.stream.kafka20.StreamConsumerFactory";
    String defaultDecoderClass = "org.apache.pinot.plugin.inputformat.avro.KafkaAvroMessageDecoder";

    String consumerFactoryClass = String.format("${CONSUMER_FACTORY_CLASS:%s}", defaultConsumerFactoryClass);
    String decoderClass = String.format("${DECODER_CLASS:%s}", defaultDecoderClass);

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME), topic);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES), consumerType);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), consumerFactoryClass);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS), decoderClass);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType, "aws.accessKey"), "${AWS_ACCESS_KEY}");
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType, "aws.secretKey"), "${AWS_SECRET_KEY}");
    indexingConfig.setStreamConfigs(streamConfigMap);

    setEnv(ImmutableMap.of("LOAD_MODE", "MMAP", "AWS_ACCESS_KEY", "default_aws_access_key", "AWS_SECRET_KEY", "default_aws_secret_key"));

    indexingConfig = ConfigUtils.applyConfigWithEnvVariables(indexingConfig);
    assertEquals(indexingConfig.getLoadMode(), "MMAP");
    assertTrue(indexingConfig.isAggregateMetrics());
    assertEquals(indexingConfig.getInvertedIndexColumns(), invertedIndexColumns);
    assertEquals(indexingConfig.getSortedColumn(), sortedColumn);
    assertEquals(indexingConfig.getOnHeapDictionaryColumns(), onHeapDictionaryColumns);
    assertEquals(indexingConfig.getBloomFilterColumns(), bloomFilterColumns);
    assertEquals(indexingConfig.getNoDictionaryConfig(), noDictionaryConfig);
    assertEquals(indexingConfig.getVarLengthDictionaryColumns(), varLengthDictionaryColumns);

    // Mandatory values + defaults
    StreamConfig streamConfig = new StreamConfig(tableName, indexingConfig.getStreamConfigs());
    Assert.assertEquals(streamConfig.getType(), streamType);
    Assert.assertEquals(streamConfig.getTopicName(), topic);
    Assert.assertEquals(streamConfig.getConsumerTypes().get(0), StreamConfig.ConsumerType.LOWLEVEL);
    Assert.assertEquals(streamConfig.getConsumerFactoryClassName(), defaultConsumerFactoryClass);
    Assert.assertEquals(streamConfig.getDecoderClass(), defaultDecoderClass);
    Assert.assertEquals(streamConfig.getStreamConfigsMap().get("stream.fakeStream.aws.accessKey"), "default_aws_access_key");
    Assert.assertEquals(streamConfig.getStreamConfigsMap().get("stream.fakeStream.aws.secretKey"), "default_aws_secret_key");
    Assert.assertEquals(streamConfig.getDecoderProperties().size(), 0);
    Assert.assertEquals(streamConfig.getOffsetCriteria(), new OffsetCriteria.OffsetCriteriaBuilder().withOffsetLargest());
    Assert.assertEquals(streamConfig.getConnectionTimeoutMillis(), StreamConfig.DEFAULT_STREAM_CONNECTION_TIMEOUT_MILLIS);
    Assert.assertEquals(streamConfig.getFetchTimeoutMillis(), StreamConfig.DEFAULT_STREAM_FETCH_TIMEOUT_MILLIS);
    Assert.assertEquals(streamConfig.getFlushThresholdRows(), StreamConfig.DEFAULT_FLUSH_THRESHOLD_ROWS);
    Assert.assertEquals(streamConfig.getFlushThresholdTimeMillis(), StreamConfig.DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS);
    Assert.assertEquals(streamConfig.getFlushThresholdSegmentSizeBytes(), StreamConfig.DEFAULT_FLUSH_THRESHOLD_SEGMENT_SIZE_BYTES);
  }

  private static void setEnv(Map<String, String> newEnvVariablsMap)
      throws Exception {
    try {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
      theEnvironmentField.setAccessible(true);
      Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
      env.putAll(newEnvVariablsMap);
      Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
      theCaseInsensitiveEnvironmentField.setAccessible(true);
      Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
      cienv.putAll(newEnvVariablsMap);
    } catch (NoSuchFieldException e) {
      Class[] classes = Collections.class.getDeclaredClasses();
      Map<String, String> env = System.getenv();
      for (Class cl : classes) {
        if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
          Field field = cl.getDeclaredField("m");
          field.setAccessible(true);
          Object obj = field.get(env);
          Map<String, String> map = (Map<String, String>) obj;
          map.clear();
          map.putAll(newEnvVariablsMap);
        }
      }
    }
  }
}
