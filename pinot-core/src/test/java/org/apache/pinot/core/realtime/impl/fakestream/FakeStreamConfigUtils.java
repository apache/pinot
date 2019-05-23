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
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.realtime.stream.StreamConfig;
import org.apache.pinot.core.realtime.stream.StreamConfigProperties;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;


/**
 * Helper methods to provide fake stream details
 */
public class FakeStreamConfigUtils {
  private static final String AVRO_TAR_FILE_NAME_PREFIX = "fake_stream_partition_";
  private static final String AVRO_TAR_FILE_NAME_SUFFIX = ".tar.gz";
  private static final String AVRO_SCHEMA_FILE = "fake_stream_avro_schema.avsc";
  private static final String PINOT_SCHEMA_FILE = "fake_stream_pinot_schema.json";

  private static final int NUM_PARTITIONS = 2;
  private static final int SMALLEST_OFFSET = 0;
  private static final int LARGEST_OFFSET = 58307;

  private static final String STREAM_TYPE = "fakeStream";
  private static final String TOPIC_NAME = "fakeTopic";
  private static final String CONSUMER_FACTORY_CLASS = FakeStreamConsumerFactory.class.getName();
  private static final String OFFSET_CRITERIA = "smallest";
  private static final String DECODER_CLASS = FakeStreamMessageDecoder.class.getName();
  private static final int SEGMENT_FLUSH_THRESHOLD_ROWS = 5000;

  /**
   * Gets default num partitions
   */
  static int getNumPartitions() {
    return NUM_PARTITIONS;
  }

  /**
   * Gets smallest offset based on data
   */
  static int getSmallestOffset() {
    return SMALLEST_OFFSET;
  }

  /**
   * Gets largest offset based on data
   */
  static int getLargestOffset() {
    return LARGEST_OFFSET;
  }

  /**
   * Gets avro tar file for partition
   */
  static String getAvroTarFileName(int partition) {
    return AVRO_TAR_FILE_NAME_PREFIX + partition + AVRO_TAR_FILE_NAME_SUFFIX;
  }

  /**
   * Unpacks avro tar file
   */
  static List<File> unpackAvroTarFile(String avroTarFileName, File outputDir) throws Exception {
    if (outputDir.exists()) {
      FileUtils.deleteDirectory(outputDir);
    }
    File avroTarFile = getResourceFile(avroTarFileName);
    return TarGzCompressionUtils.unTar(avroTarFile, outputDir);
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
  static Schema getPinotSchema() throws IOException {
    File schemaFile = getResourceFile(PINOT_SCHEMA_FILE);
    return Schema.fromFile(schemaFile);
  }

  private static File getResourceFile(String fileName) {
    URL resourceURL = FakeStreamConfigUtils.class.getClassLoader().getResource("data/fakestream");
    Assert.assertNotNull(resourceURL);
    return new File(TestUtils.getFileFromResourceUrl(resourceURL), fileName);
  }

  /**
   * Generate fake stream configs for low level stream
   */
  public static StreamConfig getDefaultLowLevelStreamConfigs() {
    Map<String, String> streamConfigMap = getDefaultStreamConfigs();
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_CONSUMER_TYPES),
        StreamConfig.ConsumerType.LOWLEVEL.toString());

    return new StreamConfig(streamConfigMap);
  }

  /**
   * Generate fake stream configs for high level stream
   */
  public static StreamConfig getDefaultHighLevelStreamConfigs() {
    Map<String, String> streamConfigMap = getDefaultStreamConfigs();
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_CONSUMER_TYPES),
        StreamConfig.ConsumerType.HIGHLEVEL.toString());

    return new StreamConfig(streamConfigMap);
  }

  private static Map<String, String> getDefaultStreamConfigs() {
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, STREAM_TYPE);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_TOPIC_NAME),
        TOPIC_NAME);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), CONSUMER_FACTORY_CLASS);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE,
        StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), OFFSET_CRITERIA);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_DECODER_CLASS),
        DECODER_CLASS);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS,
        Integer.toString(SEGMENT_FLUSH_THRESHOLD_ROWS));
    return streamConfigMap;
  }
}
