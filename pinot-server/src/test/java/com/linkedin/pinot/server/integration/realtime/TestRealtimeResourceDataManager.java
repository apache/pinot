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
package com.linkedin.pinot.server.integration.realtime;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.ZNRecord;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.RealtimeDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource.Realtime.StreamType;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.core.data.manager.config.ResourceDataManagerConfig;
import com.linkedin.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import com.linkedin.pinot.core.realtime.TestRealtimeFileBasedReader;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;


public class TestRealtimeResourceDataManager {

  private static RealtimeDataResourceZKMetadata realtimeDataResourceZKMetadata;
  private static InstanceZKMetadata instanceZKMetadata;
  private static RealtimeSegmentZKMetadata realtimeSegmentZKMetadata;
  private static ResourceDataManagerConfig resourceDataManagerConfig;
  private static final String AVRO_DATA = "data/mirror-mv.avro";
  private static String filePath;
  private static Map<String, FieldType> fieldTypeMap;

  private static final String RESOURCE_DATA_MANAGER_NUM_QUERY_EXECUTOR_THREADS = "numQueryExecutorThreads";
  private static final String RESOURCE_DATA_MANAGER_TYPE = "dataManagerType";
  private static final String READ_MODE = "readMode";
  private static final String RESOURCE_DATA_MANAGER_DATA_DIRECTORY = "directory";
  private static final String RESOURCE_DATA_MANAGER_NAME = "name";

  @BeforeClass
  public static void setup() throws Exception {
    realtimeDataResourceZKMetadata = getRealtimeDataResourceZKMetadata();
    instanceZKMetadata = getInstanceZKMetadata();
    realtimeSegmentZKMetadata = getRealtimeSegmentZKMetadata();
    resourceDataManagerConfig = getResourceDataManagerConfig();
  }

  private static ResourceDataManagerConfig getResourceDataManagerConfig() throws ConfigurationException {
    String resourceName = "testResource_R";
    Configuration defaultConfig = new PropertiesConfiguration();
    defaultConfig.addProperty(RESOURCE_DATA_MANAGER_NAME, resourceName);
    String dataDir = "/tmp/" + resourceName;
    defaultConfig.addProperty(RESOURCE_DATA_MANAGER_DATA_DIRECTORY, dataDir);
    defaultConfig.addProperty(READ_MODE, ReadMode.heap.toString());
    defaultConfig.addProperty(RESOURCE_DATA_MANAGER_NUM_QUERY_EXECUTOR_THREADS, 20);
    ResourceDataManagerConfig resourceDataManagerConfig = new ResourceDataManagerConfig(defaultConfig);

    defaultConfig.addProperty(RESOURCE_DATA_MANAGER_TYPE, "realtime");

    return resourceDataManagerConfig;
  }

  @Test
  public void testSetup() throws Exception {
    RealtimeSegmentDataManager manager =
        new RealtimeSegmentDataManager(realtimeSegmentZKMetadata, realtimeDataResourceZKMetadata, instanceZKMetadata,
            null, resourceDataManagerConfig.getDataDir(), ReadMode.valueOf(resourceDataManagerConfig.getReadMode()));
    Thread.sleep(30000);
  }

  private static InstanceZKMetadata getInstanceZKMetadata() {
    ZNRecord record = new ZNRecord("Server_lva1-app0120.corp.linkedin.com_8001");
    Map<String, String> groupIdMap = new HashMap<String, String>();
    Map<String, String> partitionMap = new HashMap<String, String>();

    groupIdMap.put("testResource_R", "groupId_testResource_" + String.valueOf(System.currentTimeMillis()));
    partitionMap.put("testResource_R", "0");
    record.setMapField("KAFKA_HLC_GROUP_MAP", groupIdMap);
    record.setMapField("KAFKA_HLC_PARTITION_MAP", partitionMap);
    return new InstanceZKMetadata(record);
  }

  private static RealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata() {
    RealtimeSegmentZKMetadata realtimeSegmentMetadata = new RealtimeSegmentZKMetadata();
    realtimeSegmentMetadata.setSegmentName("testResource_R_testTable_1000_groupId0_part0");
    realtimeSegmentMetadata.setResourceName("testResource");
    realtimeSegmentMetadata.setTableName("testTable");
    realtimeSegmentMetadata.setSegmentType(SegmentType.REALTIME);
    realtimeSegmentMetadata.setIndexVersion("v1");
    realtimeSegmentMetadata.setStartTime(1000);
    realtimeSegmentMetadata.setEndTime(-1);
    realtimeSegmentMetadata.setTimeUnit(TimeUnit.HOURS);
    realtimeSegmentMetadata.setStatus(Status.IN_PROGRESS);
    realtimeSegmentMetadata.setTotalDocs(-1);
    realtimeSegmentMetadata.setCrc(-1);
    realtimeSegmentMetadata.setCreationTime(1000);
    return realtimeSegmentMetadata;
  }

  private static RealtimeDataResourceZKMetadata getRealtimeDataResourceZKMetadata() throws FileNotFoundException,
      IOException {
    RealtimeDataResourceZKMetadata realtimeDataResourceZKMetadata = new RealtimeDataResourceZKMetadata();
    realtimeDataResourceZKMetadata.setResourceName("testResource");
    realtimeDataResourceZKMetadata.addToTableList("testTable");
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

  private static Map<String, String> getTestKafkaStreamConfig() {
    Map<String, String> streamMap = new HashMap<String, String>();

    streamMap.put(StringUtil.join(".", Helix.DataSource.STREAM, Helix.DataSource.Realtime.Kafka.CONSUMER_TYPE),
        Helix.DataSource.Realtime.Kafka.ConsumerType.highLevel.toString());
    streamMap.put(StringUtil.join(".", Helix.DataSource.STREAM, Helix.DataSource.Realtime.Kafka.TOPIC_NAME),
        "MirrorDecoratedProfileViewEvent");
    streamMap.put(StringUtil.join(".", Helix.DataSource.STREAM, Helix.DataSource.Realtime.Kafka.DECODER_CLASS),
        "com.linkedin.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder");
    streamMap.put(
        StringUtil.join(".", Helix.DataSource.STREAM, Helix.DataSource.Realtime.Kafka.HighLevelConsumer.GROUP_ID),
        "testGroupId");
    streamMap.put(StringUtil.join(".", Helix.DataSource.STREAM,
        Helix.DataSource.Realtime.Kafka.HighLevelConsumer.ZK_CONNECTION_STRING),
        "zk-eat1-kafka.corp.linkedin.com:12913/kafka-aggregate-tracking");
    streamMap.put(
        StringUtil.join(".", Helix.DataSource.STREAM,
            Helix.DataSource.Realtime.Kafka.getDecoderPropertyKeyFor("schema.registry.rest.url")),
        "http://eat1-ei2-schema-vip-z.stg.linkedin.com:10252/schemaRegistry/schemas");

    return streamMap;
  }

  private static Schema getTestSchema() throws FileNotFoundException, IOException {
    filePath = TestRealtimeFileBasedReader.class.getClassLoader().getResource(AVRO_DATA).getFile();
    fieldTypeMap = new HashMap<String, FieldSpec.FieldType>();
    fieldTypeMap.put("viewerId", FieldType.DIMENSION);
    fieldTypeMap.put("vieweeId", FieldType.DIMENSION);
    fieldTypeMap.put("viewerPrivacySetting", FieldType.DIMENSION);
    fieldTypeMap.put("vieweePrivacySetting", FieldType.DIMENSION);
    fieldTypeMap.put("viewerObfuscationType", FieldType.DIMENSION);
    fieldTypeMap.put("viewerCompanies", FieldType.DIMENSION);
    fieldTypeMap.put("viewerOccupations", FieldType.DIMENSION);
    fieldTypeMap.put("viewerRegionCode", FieldType.DIMENSION);
    fieldTypeMap.put("viewerIndustry", FieldType.DIMENSION);
    fieldTypeMap.put("viewerSchool", FieldType.DIMENSION);
    fieldTypeMap.put("weeksSinceEpochSunday", FieldType.DIMENSION);
    fieldTypeMap.put("daysSinceEpoch", FieldType.DIMENSION);
    fieldTypeMap.put("minutesSinceEpoch", FieldType.TIME);
    fieldTypeMap.put("count", FieldType.METRIC);
    return SegmentTestUtils.extractSchemaFromAvro(new File(filePath), fieldTypeMap, TimeUnit.MINUTES);
  }
}
