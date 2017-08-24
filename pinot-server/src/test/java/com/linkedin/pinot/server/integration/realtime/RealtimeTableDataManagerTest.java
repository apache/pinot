/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.data.manager.config.InstanceDataManagerConfig;
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
import com.linkedin.pinot.core.data.manager.realtime.HLRealtimeSegmentDataManager;
import com.linkedin.pinot.core.data.manager.realtime.TimerService;
import com.linkedin.pinot.core.realtime.RealtimeFileBasedReaderTest;
import com.linkedin.pinot.core.realtime.RealtimeSegment;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;

import static org.mockito.Mockito.*;


public class RealtimeTableDataManagerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeTableDataManagerTest.class);

  private static TableConfig tableConfig;

  private static InstanceZKMetadata instanceZKMetadata;
  private static RealtimeSegmentZKMetadata realtimeSegmentZKMetadata;
  private static TableDataManagerConfig tableDataManagerConfig;
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static String filePath;
  private static Map<String, FieldType> fieldTypeMap;

  private static final String TABLE_DATA_MANAGER_NUM_QUERY_EXECUTOR_THREADS = "numQueryExecutorThreads";
  private static final String TABLE_DATA_MANAGER_TYPE = "dataManagerType";
  private static final String READ_MODE = "readMode";
  private static final String TABLE_DATA_MANAGER_DATA_DIRECTORY = "directory";
  private static final String TABLE_DATA_MANAGER_NAME = "name";

  private static final long SEGMENT_CONSUMING_TIME = 1000 * 60 * 3;

  private static volatile boolean keepOnRunning = true;

  @BeforeClass
  public static void setup()
      throws Exception {
    instanceZKMetadata = getInstanceZKMetadata();
    realtimeSegmentZKMetadata = getRealtimeSegmentZKMetadata();
    tableDataManagerConfig = getTableDataManagerConfig();

    Map<String, String> streamConfigs = new HashMap<>();
    streamConfigs.put("streamType", "kafka");
    streamConfigs.put("stream.kafka.consumer.type", "highLevel");
    streamConfigs.put("stream.kafka.topic.name", "kafkaTopic");
    streamConfigs.put("stream.kafka.decoder.class.name",
        "com.linkedin.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder");
    streamConfigs.put("stream.kafka.hlc.zk.connect.string", "localhost:1111/zkConnect");
    streamConfigs.put("stream.kafka.decoder.prop.schema.registry.rest.url", "http://localhost:2222/schemaRegistry");
    tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME).setTableName("mirror")
        .setStreamConfigs(streamConfigs)
        .build();
  }

  private static TableDataManagerConfig getTableDataManagerConfig() throws ConfigurationException {
    String tableName = "testTable_R";
    Configuration defaultConfig = new PropertiesConfiguration();
    defaultConfig.addProperty(TABLE_DATA_MANAGER_NAME, tableName);
    String dataDir = "/tmp/" + tableName;
    defaultConfig.addProperty(TABLE_DATA_MANAGER_DATA_DIRECTORY, dataDir);
    defaultConfig.addProperty(READ_MODE, ReadMode.heap.toString());
    defaultConfig.addProperty(TABLE_DATA_MANAGER_NUM_QUERY_EXECUTOR_THREADS, 20);
    TableDataManagerConfig tableDataManagerConfig = new TableDataManagerConfig(defaultConfig);

    defaultConfig.addProperty(TABLE_DATA_MANAGER_TYPE, "realtime");

    return tableDataManagerConfig;
  }

  private InstanceDataManagerConfig makeInstanceDataManagerConfig() {
    InstanceDataManagerConfig dataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(dataManagerConfig.getReadMode()).thenReturn(null);
    when(dataManagerConfig.getAvgMultiValueCount()).thenReturn(null);
    when(dataManagerConfig.getSegmentFormatVersion()).thenReturn(null);
    when(dataManagerConfig.isEnableDefaultColumns()).thenReturn(false);
    when(dataManagerConfig.isEnableSplitCommit()).thenReturn(false);
    when(dataManagerConfig.isRealtimeOffHeapAllocation()).thenReturn(false);
    return dataManagerConfig;
  }

  public void testSetup() throws Exception {
    InstanceDataManagerConfig dataManagerConfig = makeInstanceDataManagerConfig();
    final HLRealtimeSegmentDataManager manager =
        new HLRealtimeSegmentDataManager(realtimeSegmentZKMetadata, tableConfig, instanceZKMetadata, null,
            tableDataManagerConfig.getDataDir(), new IndexLoadingConfig(dataManagerConfig, tableConfig), getTestSchema(),
            new ServerMetrics(new MetricsRegistry()));

    final long start = System.currentTimeMillis();
    TimerService.timer.scheduleAtFixedRate(new TimerTask() {

      @Override
      public void run() {
        if (System.currentTimeMillis() - start >= (SEGMENT_CONSUMING_TIME)) {
          keepOnRunning = false;
        }
      }
    }, 1000, 1000 * 60 * 1);

    TimerService.timer.scheduleAtFixedRate(new TimerTask() {

      @Override
      public void run() {
        long start = System.currentTimeMillis();
        long sum = 0;
        try {
          RealtimeSegment segment = (RealtimeSegment) manager.getSegment();
          DataSource mDs = segment.getDataSource("count");
          BlockValSet valSet = mDs.nextBlock().getBlockValueSet();
          BlockSingleValIterator valIt = (BlockSingleValIterator) valSet.iterator();
          int val = valIt.nextIntVal();
          while (val != Constants.EOF) {
            val = valIt.nextIntVal();
            sum += val;
          }
        } catch (Exception e) {
          LOGGER.info("count column exception");
          e.printStackTrace();
        }

        long stop = System.currentTimeMillis();
        LOGGER.info("time to scan metric col count : " + (stop - start) + " sum : " + sum);
      }
    }, 20000, 1000 * 5);

    TimerService.timer.scheduleAtFixedRate(new TimerTask() {

      @Override
      public void run() {
        long start = System.currentTimeMillis();
        long sum = 0;
        try {
          RealtimeSegment segment = (RealtimeSegment) manager.getSegment();
          DataSource mDs = segment.getDataSource("viewerId");
          BlockValSet valSet = mDs.nextBlock().getBlockValueSet();
          BlockSingleValIterator valIt = (BlockSingleValIterator) valSet.iterator();
          int val = valIt.nextIntVal();
          while (val != Constants.EOF) {
            val = valIt.nextIntVal();
            sum += val;
          }
        } catch (Exception e) {
          LOGGER.info("viewerId column exception");
          e.printStackTrace();
        }

        long stop = System.currentTimeMillis();
        LOGGER.info("time to scan SV dimension col viewerId : " + (stop - start) + " sum : " + sum);
      }
    }, 20000, 1000 * 5);

    TimerService.timer.scheduleAtFixedRate(new TimerTask() {

      @Override
      public void run() {
        long start = System.currentTimeMillis();
        long sum = 0;
        try {
          RealtimeSegment segment = (RealtimeSegment) manager.getSegment();
          DataSource mDs = segment.getDataSource("daysSinceEpoch");
          BlockValSet valSet = mDs.nextBlock().getBlockValueSet();
          BlockSingleValIterator valIt = (BlockSingleValIterator) valSet.iterator();
          int val = valIt.nextIntVal();
          while (val != Constants.EOF) {
            val = valIt.nextIntVal();
            sum += val;
          }
        } catch (Exception e) {
          LOGGER.info("daysSinceEpoch column exception");
          e.printStackTrace();
        }
        long stop = System.currentTimeMillis();
        LOGGER.info("time to scan SV time col daysSinceEpoch : " + (stop - start) + " sum : " + sum);
      }
    }, 20000, 1000 * 5);

    TimerService.timer.scheduleAtFixedRate(new TimerTask() {

      @Override
      public void run() {
        long start = System.currentTimeMillis();
        long sum = 0;
        float sumOfLengths = 0F;
        float counter = 0F;
        try {
          RealtimeSegment segment = (RealtimeSegment) manager.getSegment();
          DataSource mDs = segment.getDataSource("viewerCompanies");
          Block b = mDs.nextBlock();
          BlockValSet valSet = b.getBlockValueSet();
          BlockMultiValIterator valIt = (BlockMultiValIterator) valSet.iterator();
          BlockMetadata m = b.getMetadata();
          int maxVams = m.getMaxNumberOfMultiValues();
          while (valIt.hasNext()) {
            int[] vals = new int[maxVams];
            int len = valIt.nextIntVal(vals);
            for (int i = 0; i < len; i++) {
              sum += vals[i];
            }
            sumOfLengths += len;
            counter++;
          }
        } catch (Exception e) {
          LOGGER.info("daysSinceEpoch column exception");
          e.printStackTrace();
        }
        long stop = System.currentTimeMillis();
        LOGGER.info("time to scan MV col viewerCompanies : " + (stop - start) + " sum : " + sum + " average len : "
            + (sumOfLengths / counter));
      }
    }, 20000, 1000 * 5);

    while (keepOnRunning) {
      // Wait for keepOnRunning to be set to false
    }
  }

  private static InstanceZKMetadata getInstanceZKMetadata() {
    ZNRecord record = new ZNRecord("Server_localhost_1234");
    Map<String, String> groupIdMap = new HashMap<>();
    Map<String, String> partitionMap = new HashMap<>();

    groupIdMap.put("mirror", "groupId_testTable_" + String.valueOf(System.currentTimeMillis()));
    partitionMap.put("testTable_R", "0");
    record.setMapField("KAFKA_HLC_GROUP_MAP", groupIdMap);
    record.setMapField("KAFKA_HLC_PARTITION_MAP", partitionMap);
    return new InstanceZKMetadata(record);
  }

  private static RealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata() {
    RealtimeSegmentZKMetadata realtimeSegmentMetadata = new RealtimeSegmentZKMetadata();
    realtimeSegmentMetadata.setSegmentName("testTable_R_1000_groupId0_part0");
    realtimeSegmentMetadata.setTableName("testTable");
    realtimeSegmentMetadata.setSegmentType(SegmentType.REALTIME);
    realtimeSegmentMetadata.setIndexVersion("v1");
    realtimeSegmentMetadata.setStartTime(1000);
    realtimeSegmentMetadata.setEndTime(-1);
    realtimeSegmentMetadata.setTimeUnit(TimeUnit.HOURS);
    realtimeSegmentMetadata.setStatus(Status.IN_PROGRESS);
    realtimeSegmentMetadata.setTotalRawDocs(-1);
    realtimeSegmentMetadata.setCrc(-1);
    realtimeSegmentMetadata.setCreationTime(1000);
    return realtimeSegmentMetadata;
  }

  private static Schema getTestSchema() throws FileNotFoundException, IOException {
    filePath = RealtimeFileBasedReaderTest.class.getClassLoader().getResource(AVRO_DATA).getFile();
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
