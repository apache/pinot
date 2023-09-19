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
package org.apache.pinot.core.realtime.impl.fakestream;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;


/**
 * Helper methods to provide fake stream details
 * TODO: make input tar file and pinot schema configurable
 */
public class FakeStreamConfigUtils {
  private FakeStreamConfigUtils() {
  }

  private static final String TABLE_NAME_WITH_TYPE = "fake_tableName_REALTIME";
  private static final String AVRO_TAR_FILE = "fake_stream_avro_data.tar.gz";
  // This avro schema file must be in sync with the avro data
  private static final String AVRO_SCHEMA_FILE = "fake_stream_avro_schema.avsc";
  private static final String PINOT_SCHEMA_FILE = "fake_stream_pinot_schema.json";
  private static final String TIME_COLUMN_NAME = "DaysSinceEpoch";

  private static final LongMsgOffset SMALLEST_OFFSET = new LongMsgOffset(0);
  private static final LongMsgOffset LARGEST_OFFSET = new LongMsgOffset(Integer.MAX_VALUE);
  private static final String NUM_PARTITIONS_KEY = "num.partitions";
  private static final int DEFAULT_NUM_PARTITIONS = 2;

  private static final String STREAM_TYPE = "fakeStream";
  private static final String TOPIC_NAME = "fakeTopic";
  private static final String CONSUMER_FACTORY_CLASS = FakeStreamConsumerFactory.class.getName();
  private static final String OFFSET_CRITERIA = "smallest";
  private static final String DECODER_CLASS = FakeStreamMessageDecoder.class.getName();
  public static final int SEGMENT_FLUSH_THRESHOLD_ROWS = 500;
  public static final int MESSAGE_BATCH_SIZE = SEGMENT_FLUSH_THRESHOLD_ROWS;

  /**
   * Gets default num partitions
   */
  static int getNumPartitions(StreamConfig streamConfig) {
    Map<String, String> streamConfigsMap = streamConfig.getStreamConfigsMap();
    String numPartitionsKey =
        StreamConfigProperties.constructStreamProperty(streamConfig.getType(), NUM_PARTITIONS_KEY);
    if (streamConfigsMap.containsKey(numPartitionsKey)) {
      return Integer.parseInt(streamConfigsMap.get(numPartitionsKey));
    }
    return DEFAULT_NUM_PARTITIONS;
  }

  /**
   * Gets smallest offset based on data
   */
  static StreamPartitionMsgOffset getSmallestOffset() {
    return SMALLEST_OFFSET;
  }

  /**
   * Gets largest offset based on data
   */
  static StreamPartitionMsgOffset getLargestOffset() {
    return LARGEST_OFFSET;
  }

  /**
   * Unpacks avro tar file
   */
  static List<File> unpackAvroTarFile(File outputDir)
      throws Exception {
    if (outputDir.exists()) {
      FileUtils.deleteDirectory(outputDir);
    }
    File avroTarFile = getResourceFile(AVRO_TAR_FILE);
    return TarGzCompressionUtils.untar(avroTarFile, outputDir);
  }

  /**
   * Gets avro schema
   */
  static File getAvroSchemaFile() {
    return getResourceFile(AVRO_SCHEMA_FILE);
  }

  /**
   * Gets pinot schema
   */
  static Schema getPinotSchema()
      throws IOException {
    File schemaFile = getResourceFile(PINOT_SCHEMA_FILE);
    return Schema.fromFile(schemaFile);
  }

  /**
   * Gets pinot table
   */
  static TableConfig getTableConfig() {
    return new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME_WITH_TYPE)
        .setTimeColumnName(TIME_COLUMN_NAME).build();
  }

  private static File getResourceFile(String fileName) {
    URL resourceURL = FakeStreamConfigUtils.class.getClassLoader().getResource("data/fakestream");
    Assert.assertNotNull(resourceURL);
    return new File(TestUtils.getFileFromResourceUrl(resourceURL), fileName);
  }

  /**
   * Generate fake stream configs for low level stream with custom number of partitions
   */
  public static StreamConfig getDefaultLowLevelStreamConfigs(int numPartitions) {
    Map<String, String> streamConfigMap = getDefaultStreamConfigs();
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, NUM_PARTITIONS_KEY),
        String.valueOf(numPartitions));
    return new StreamConfig(TABLE_NAME_WITH_TYPE, streamConfigMap);
  }

  /**
   * Generate fake stream configs for low level stream
   */
  public static StreamConfig getDefaultLowLevelStreamConfigs() {
    return getDefaultLowLevelStreamConfigs(DEFAULT_NUM_PARTITIONS);
  }

  private static Map<String, String> getDefaultStreamConfigs() {
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, STREAM_TYPE);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_TOPIC_NAME),
            TOPIC_NAME);
    streamConfigMap.put(StreamConfigProperties
            .constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS),
        CONSUMER_FACTORY_CLASS);
    streamConfigMap.put(StreamConfigProperties
        .constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), OFFSET_CRITERIA);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_DECODER_CLASS),
            DECODER_CLASS);
    streamConfigMap
        .put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, Integer.toString(SEGMENT_FLUSH_THRESHOLD_ROWS));
    return streamConfigMap;
  }
}
