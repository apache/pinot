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
package org.apache.pinot.broker.routing.segmentpruner;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetcher;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.auth.request.BrokerRequest;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class SegmentPrunerTest extends ControllerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = "testTable_OFFLINE";
  private static final String REALTIME_TABLE_NAME = "testTable_REALTIME";
  private static final String PARTITION_COLUMN_1 = "memberId";
  private static final String PARTITION_COLUMN_2 = "memberName";
  private static final String TIME_COLUMN = "timeColumn";
  private static final String SDF_PATTERN = "yyyyMMdd";

  private static final String QUERY_1 = "SELECT * FROM testTable";
  private static final String QUERY_2 = "SELECT * FROM testTable WHERE memberId = 0";
  private static final String QUERY_3 = "SELECT * FROM testTable WHERE memberId IN (1, 2)";
  private static final String QUERY_4 = "SELECT * FROM testTable WHERE memberId = 0 AND memberName = 'xyz'";

  private static final String TIME_QUERY_1 = "SELECT * FROM testTable WHERE timeColumn = 40";
  private static final String TIME_QUERY_2 = "SELECT * FROM testTable WHERE timeColumn BETWEEN 20 AND 30";
  private static final String TIME_QUERY_3 = "SELECT * FROM testTable WHERE 30 < timeColumn AND timeColumn <= 50";
  private static final String TIME_QUERY_4 = "SELECT * FROM testTable WHERE timeColumn < 15 OR timeColumn > 45";
  private static final String TIME_QUERY_5 =
      "SELECT * FROM testTable WHERE timeColumn < 15 OR (60 < timeColumn AND timeColumn < 70)";
  private static final String TIME_QUERY_6 = "SELECT * FROM testTable WHERE timeColumn NOT BETWEEN 20 AND 30";
  private static final String TIME_QUERY_7 = "SELECT * FROM testTable WHERE NOT timeColumn > 30";
  private static final String TIME_QUERY_8 = "SELECT * FROM testTable WHERE timeColumn < 0 AND timeColumn > 0";

  private static final String SDF_QUERY_1 = "SELECT * FROM testTable WHERE timeColumn = 20200131";
  private static final String SDF_QUERY_2 = "SELECT * FROM testTable WHERE timeColumn BETWEEN 20200101 AND 20200331";
  private static final String SDF_QUERY_3 =
      "SELECT * FROM testTable WHERE 20200430 < timeColumn AND timeColumn < 20200630";
  private static final String SDF_QUERY_4 =
      "SELECT * FROM testTable WHERE timeColumn <= 20200101 OR timeColumn IN (20200201, 20200401)";
  private static final String SDF_QUERY_5 =
      "SELECT * FROM testTable WHERE timeColumn IN (20200101, 20200102) AND timeColumn >= 20200530";

  // Timestamp can be passed as string or long
  private static final String TIMESTAMP_QUERY_1 = "SELECT * FROM testTable WHERE timeColumn = '2020-01-31 00:00:00'";
  private static final String TIMESTAMP_QUERY_2 = String.format("SELECT * FROM testTable WHERE timeColumn = %d",
      Timestamp.valueOf("2020-01-31 00:00:00").getTime());
  private static final String TIMESTAMP_QUERY_3 =
      "SELECT * FROM testTable WHERE timeColumn BETWEEN '2020-01-01 00:00:00' AND '2020-03-31 00:00:00'";
  private static final String TIMESTAMP_QUERY_4 =
      String.format("SELECT * FROM testTable WHERE timeColumn BETWEEN %d AND %d",
          Timestamp.valueOf("2020-01-01 00:00:00").getTime(), Timestamp.valueOf("2020-03-31 00:00:00").getTime());
  private static final String TIMESTAMP_QUERY_5 =
      "SELECT * FROM testTable WHERE timeColumn <= '2020-01-01 00:00:00' OR timeColumn IN ('2020-02-01 00:00:00', "
          + "'2020-04-01 00:00:00')";
  private static final String TIMESTAMP_QUERY_6 =
      String.format("SELECT * FROM testTable WHERE timeColumn <= %d OR timeColumn IN (%d, %d)",
          Timestamp.valueOf("2020-01-01 00:00:00").getTime(), Timestamp.valueOf("2020-02-01 00:00:00").getTime(),
          Timestamp.valueOf("2020-04-01 00:00:00").getTime());

  // this is duplicate with KinesisConfig.STREAM_TYPE, while instead of use KinesisConfig.STREAM_TYPE directly, we
  // hardcode the value here to avoid pulling the entire pinot-kinesis module as dependency.
  private static final String KINESIS_STREAM_TYPE = "kinesis";

  private ZkClient _zkClient;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  @BeforeClass
  public void setUp() {
    startZk();
    _zkClient = new ZkClient(getZkUrl(), ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT,
        new ZNRecordSerializer());
    _propertyStore =
        new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<>(_zkClient), "/SegmentPrunerTest/PROPERTYSTORE", null);
  }

  @AfterClass
  public void tearDown() {
    _zkClient.close();
    stopZk();
  }

  @Test
  public void testSegmentPrunerFactoryForPartitionPruner() {
    TableConfig tableConfig = mock(TableConfig.class);
    when(tableConfig.getTableName()).thenReturn(OFFLINE_TABLE_NAME);
    IndexingConfig indexingConfig = mock(IndexingConfig.class);
    when(tableConfig.getIndexingConfig()).thenReturn(indexingConfig);

    // Routing config is missing
    List<SegmentPruner> segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 0);

    // Segment pruner type is not configured
    RoutingConfig routingConfig = mock(RoutingConfig.class);
    when(tableConfig.getRoutingConfig()).thenReturn(routingConfig);
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 0);

    // Segment partition config is missing
    when(routingConfig.getSegmentPrunerTypes()).thenReturn(List.of(RoutingConfig.PARTITION_SEGMENT_PRUNER_TYPE));
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 0);

    // Column partition config is missing
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    when(indexingConfig.getSegmentPartitionConfig()).thenReturn(new SegmentPartitionConfig(columnPartitionConfigMap));
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 0);

    // Partition-aware segment pruner should be returned
    columnPartitionConfigMap.put(PARTITION_COLUMN_1, new ColumnPartitionConfig("Modulo", 5));
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 1);
    assertTrue(segmentPruners.get(0) instanceof SinglePartitionColumnSegmentPruner);

    // Multiple partition columns
    columnPartitionConfigMap.put(PARTITION_COLUMN_2, new ColumnPartitionConfig("Modulo", 5));
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 1);
    assertTrue(segmentPruners.get(0) instanceof MultiPartitionColumnsSegmentPruner);
    MultiPartitionColumnsSegmentPruner partitionSegmentPruner =
        (MultiPartitionColumnsSegmentPruner) segmentPruners.get(0);
    assertEquals(partitionSegmentPruner.getPartitionColumns(),
        Stream.of(PARTITION_COLUMN_1, PARTITION_COLUMN_2).collect(Collectors.toSet()));

    // Should be backward-compatible with legacy config
    columnPartitionConfigMap.remove(PARTITION_COLUMN_1);
    when(routingConfig.getSegmentPrunerTypes()).thenReturn(null);
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 0);
    when(tableConfig.getTableType()).thenReturn(TableType.OFFLINE);
    when(routingConfig.getRoutingTableBuilderName()).thenReturn(
        SegmentPrunerFactory.LEGACY_PARTITION_AWARE_OFFLINE_ROUTING);
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertTrue(segmentPruners.get(0) instanceof SinglePartitionColumnSegmentPruner);
    when(tableConfig.getTableType()).thenReturn(TableType.REALTIME);
    when(routingConfig.getRoutingTableBuilderName()).thenReturn(
        SegmentPrunerFactory.LEGACY_PARTITION_AWARE_REALTIME_ROUTING);
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 1);
    assertTrue(segmentPruners.get(0) instanceof SinglePartitionColumnSegmentPruner);
  }

  @Test
  public void testSegmentPrunerFactoryForTimeRangePruner() {
    TableConfig tableConfig = mock(TableConfig.class);
    when(tableConfig.getTableName()).thenReturn(OFFLINE_TABLE_NAME);

    // Routing config is missing
    List<SegmentPruner> segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 0);

    // Segment pruner type is not configured
    RoutingConfig routingConfig = mock(RoutingConfig.class);
    when(tableConfig.getRoutingConfig()).thenReturn(routingConfig);
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 0);

    // Validation config is missing
    when(routingConfig.getSegmentPrunerTypes()).thenReturn(List.of(RoutingConfig.TIME_SEGMENT_PRUNER_TYPE));
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 0);

    // Time column is missing
    SegmentsValidationAndRetentionConfig validationConfig = mock(SegmentsValidationAndRetentionConfig.class);
    when(tableConfig.getValidationConfig()).thenReturn(validationConfig);
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 0);

    // Schema is missing
    when(validationConfig.getTimeColumnName()).thenReturn(TIME_COLUMN);
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 0);

    // Field spec is missing
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME).build();
    ZKMetadataProvider.setSchema(_propertyStore, schema);
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 0);

    // Time range pruner should be returned
    schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addDateTimeField(TIME_COLUMN, DataType.TIMESTAMP, "TIMESTAMP", "1:MILLISECONDS").build();
    ZKMetadataProvider.setSchema(_propertyStore, schema);
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 1);
    assertTrue(segmentPruners.get(0) instanceof TimeSegmentPruner);
  }

  @Test
  public void testSegmentPrunerFactoryForEmptySegmentPruner() {
    TableConfig tableConfig = mock(TableConfig.class);
    when(tableConfig.getTableName()).thenReturn(REALTIME_TABLE_NAME);
    IndexingConfig indexingConfig = mock(IndexingConfig.class);
    when(tableConfig.getIndexingConfig()).thenReturn(indexingConfig);
    RoutingConfig routingConfig = mock(RoutingConfig.class);
    when(tableConfig.getRoutingConfig()).thenReturn(routingConfig);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);

    // When routingConfig is configured with EmptySegmentPruner, EmptySegmentPruner should be returned.
    when(routingConfig.getSegmentPrunerTypes()).thenReturn(List.of(RoutingConfig.EMPTY_SEGMENT_PRUNER_TYPE));
    List<SegmentPruner> segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, propertyStore);
    assertEquals(segmentPruners.size(), 1);
    assertTrue(segmentPruners.get(0) instanceof EmptySegmentPruner);

    // When indexingConfig is configured with Kinesis streaming, EmptySegmentPruner should be returned.
    when(indexingConfig.getStreamConfigs()).thenReturn(Map.of(StreamConfigProperties.STREAM_TYPE, KINESIS_STREAM_TYPE));
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, propertyStore);
    assertEquals(segmentPruners.size(), 1);
    assertTrue(segmentPruners.get(0) instanceof EmptySegmentPruner);

    // When streamIngestionConfig is configured with Kinesis streaming, EmptySegmentPruner should be returned.
    StreamIngestionConfig streamIngestionConfig = mock(StreamIngestionConfig.class);
    when(streamIngestionConfig.getStreamConfigMaps()).thenReturn(
        List.of(Map.of(StreamConfigProperties.STREAM_TYPE, KINESIS_STREAM_TYPE)));
    when(indexingConfig.getStreamConfigs()).thenReturn(Map.of(StreamConfigProperties.STREAM_TYPE, KINESIS_STREAM_TYPE));
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, propertyStore);
    assertEquals(segmentPruners.size(), 1);
    assertTrue(segmentPruners.get(0) instanceof EmptySegmentPruner);
  }

  @Test
  public void testPartitionAwareSegmentPruner() {
    BrokerRequest brokerRequest1 = CalciteSqlCompiler.compileToBrokerRequest(QUERY_1);
    BrokerRequest brokerRequest2 = CalciteSqlCompiler.compileToBrokerRequest(QUERY_2);
    BrokerRequest brokerRequest3 = CalciteSqlCompiler.compileToBrokerRequest(QUERY_3);
    BrokerRequest brokerRequest4 = CalciteSqlCompiler.compileToBrokerRequest(QUERY_4);

    // NOTE: Ideal state and external view are not used in the current implementation
    IdealState idealState = Mockito.mock(IdealState.class);
    ExternalView externalView = Mockito.mock(ExternalView.class);

    SinglePartitionColumnSegmentPruner singlePartitionColumnSegmentPruner =
        new SinglePartitionColumnSegmentPruner(OFFLINE_TABLE_NAME, PARTITION_COLUMN_1);
    SegmentZkMetadataFetcher segmentZkMetadataFetcher =
        new SegmentZkMetadataFetcher(OFFLINE_TABLE_NAME, _propertyStore);
    segmentZkMetadataFetcher.register(singlePartitionColumnSegmentPruner);
    Set<String> onlineSegments = new HashSet<>();
    segmentZkMetadataFetcher.init(idealState, externalView, onlineSegments);

    Set<String> input = Set.of();
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest1, input), input);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest2, input), input);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest3, input), input);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest4, input), input);

    // Segments without metadata (not updated yet) should not be pruned
    String newSegment = "newSegment";
    onlineSegments.add(newSegment);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    input = Set.of(newSegment);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest1, input), input);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest2, input), input);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest3, input), input);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest4, input), input);

    // Segments without partition metadata should not be pruned
    String segmentWithoutPartitionMetadata = "segmentWithoutPartitionMetadata";
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, OFFLINE_TABLE_NAME,
        new SegmentZKMetadata(segmentWithoutPartitionMetadata));
    onlineSegments.add(segmentWithoutPartitionMetadata);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    input = Set.of(segmentWithoutPartitionMetadata);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest1, input), input);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest2, input), input);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest3, input), input);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest4, input), input);

    // Test different partition functions and number of partitions
    // 0 % 5 = 0; 1 % 5 = 1; 2 % 5 = 2
    String segment0 = "segment0";
    setSegmentZKPartitionMetadata(OFFLINE_TABLE_NAME, segment0, "Modulo", 5, 0);
    onlineSegments.add(segment0);
    // Murmur(0) % 4 = 0; Murmur(1) % 4 = 3; Murmur(2) % 4 = 0
    String segment1 = "segment1";
    setSegmentZKPartitionMetadata(OFFLINE_TABLE_NAME, segment1, "Murmur", 4, 0);
    onlineSegments.add(segment1);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    input = Set.of(segment0, segment1);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest1, input), input);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest2, input), input);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest3, input), Set.of(segment1));

    // Update partition metadata without refreshing should have no effect
    setSegmentZKPartitionMetadata(OFFLINE_TABLE_NAME, segment0, "Modulo", 4, 1);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest1, input), input);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest2, input), input);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest3, input), Set.of(segment1));
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest4, input), input);

    // Refresh the changed segment should update the segment pruner
    segmentZkMetadataFetcher.refreshSegment(segment0);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest1, input), input);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest2, input), Set.of(segment1));
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest3, input), input);
    assertEquals(singlePartitionColumnSegmentPruner.prune(brokerRequest4, input), Set.of(segment1));

    // Multi-column partitioned segment.
    MultiPartitionColumnsSegmentPruner multiPartitionColumnsSegmentPruner =
        new MultiPartitionColumnsSegmentPruner(OFFLINE_TABLE_NAME,
            Stream.of(PARTITION_COLUMN_1, PARTITION_COLUMN_2).collect(Collectors.toSet()));
    segmentZkMetadataFetcher = new SegmentZkMetadataFetcher(OFFLINE_TABLE_NAME, _propertyStore);
    segmentZkMetadataFetcher.register(multiPartitionColumnsSegmentPruner);
    segmentZkMetadataFetcher.init(idealState, externalView, onlineSegments);

    assertEquals(multiPartitionColumnsSegmentPruner.prune(brokerRequest1, input), input);
    assertEquals(multiPartitionColumnsSegmentPruner.prune(brokerRequest2, input), Set.of(segment1));
    assertEquals(multiPartitionColumnsSegmentPruner.prune(brokerRequest3, input), input);
    assertEquals(multiPartitionColumnsSegmentPruner.prune(brokerRequest4, input), Set.of(segment1));

    String segment2 = "segment2";
    Map<String, ColumnPartitionMetadata> columnPartitionMetadataMap = new HashMap<>();
    columnPartitionMetadataMap.put(PARTITION_COLUMN_1, new ColumnPartitionMetadata("Modulo", 4, Set.of(0), null));
    columnPartitionMetadataMap.put(PARTITION_COLUMN_2, new ColumnPartitionMetadata("BoundedColumnValue", 3, Set.of(1),
        Map.of("columnValues", "xyz|abc", "columnValuesDelimiter", "|")));
    setSegmentZKPartitionMetadata(OFFLINE_TABLE_NAME, segment2, columnPartitionMetadataMap);
    onlineSegments.add(segment2);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    input = Set.of(segment0, segment1, segment2);
    assertEquals(multiPartitionColumnsSegmentPruner.prune(brokerRequest1, input), input);
    assertEquals(multiPartitionColumnsSegmentPruner.prune(brokerRequest2, input), Set.of(segment1, segment2));
    assertEquals(multiPartitionColumnsSegmentPruner.prune(brokerRequest3, input), Set.of(segment0, segment1));
    assertEquals(multiPartitionColumnsSegmentPruner.prune(brokerRequest4, input), Set.of(segment1, segment2));
  }

  @Test
  public void testTimeSegmentPruner() {
    BrokerRequest brokerRequest1 = CalciteSqlCompiler.compileToBrokerRequest(QUERY_1);
    BrokerRequest brokerRequest2 = CalciteSqlCompiler.compileToBrokerRequest(TIME_QUERY_1);
    BrokerRequest brokerRequest3 = CalciteSqlCompiler.compileToBrokerRequest(TIME_QUERY_2);
    BrokerRequest brokerRequest4 = CalciteSqlCompiler.compileToBrokerRequest(TIME_QUERY_3);
    BrokerRequest brokerRequest5 = CalciteSqlCompiler.compileToBrokerRequest(TIME_QUERY_4);
    BrokerRequest brokerRequest6 = CalciteSqlCompiler.compileToBrokerRequest(TIME_QUERY_5);
    BrokerRequest brokerRequest7 = CalciteSqlCompiler.compileToBrokerRequest(TIME_QUERY_6);
    BrokerRequest brokerRequest8 = CalciteSqlCompiler.compileToBrokerRequest(TIME_QUERY_7);
    BrokerRequest brokerRequest9 = CalciteSqlCompiler.compileToBrokerRequest(TIME_QUERY_8);

    // NOTE: Ideal state and external view are not used in the current implementation
    IdealState idealState = Mockito.mock(IdealState.class);
    ExternalView externalView = Mockito.mock(ExternalView.class);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    DateTimeFieldSpec timeFieldSpec = new DateTimeFieldSpec(TIME_COLUMN, DataType.INT, "EPOCH|DAYS", "1:DAYS");
    TimeSegmentPruner segmentPruner = new TimeSegmentPruner(tableConfig, timeFieldSpec);
    SegmentZkMetadataFetcher segmentZkMetadataFetcher =
        new SegmentZkMetadataFetcher(REALTIME_TABLE_NAME, _propertyStore);
    segmentZkMetadataFetcher.register(segmentPruner);
    Set<String> onlineSegments = new HashSet<>();
    segmentZkMetadataFetcher.init(idealState, externalView, onlineSegments);

    Set<String> input = Set.of();
    assertEquals(segmentPruner.prune(brokerRequest1, input), input);
    assertEquals(segmentPruner.prune(brokerRequest2, input), input);
    assertEquals(segmentPruner.prune(brokerRequest3, input), input);
    assertEquals(segmentPruner.prune(brokerRequest4, input), input);
    assertEquals(segmentPruner.prune(brokerRequest5, input), input);
    assertEquals(segmentPruner.prune(brokerRequest6, input), input);
    assertEquals(segmentPruner.prune(brokerRequest7, input), input);
    assertEquals(segmentPruner.prune(brokerRequest8, input), input);
    assertEquals(segmentPruner.prune(brokerRequest9, input), input);

    // Segments without metadata (not updated yet) should not be pruned
    String newSegment = "newSegment";
    onlineSegments.add(newSegment);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    input = Set.of(newSegment);
    assertEquals(segmentPruner.prune(brokerRequest1, input), input);
    assertEquals(segmentPruner.prune(brokerRequest2, input), input);
    assertEquals(segmentPruner.prune(brokerRequest3, input), input);
    assertEquals(segmentPruner.prune(brokerRequest4, input), input);
    assertEquals(segmentPruner.prune(brokerRequest5, input), input);
    assertEquals(segmentPruner.prune(brokerRequest6, input), input);
    assertEquals(segmentPruner.prune(brokerRequest7, input), input);
    assertEquals(segmentPruner.prune(brokerRequest8, input), input);
    assertEquals(segmentPruner.prune(brokerRequest9, input), Set.of()); // Query with invalid range

    // Segments without time range metadata should not be pruned
    String segmentWithoutTimeRangeMetadata = "segmentWithoutTimeRangeMetadata";
    SegmentZKMetadata segmentZKMetadataWithoutTimeRangeMetadata =
        new SegmentZKMetadata(segmentWithoutTimeRangeMetadata);
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, REALTIME_TABLE_NAME,
        segmentZKMetadataWithoutTimeRangeMetadata);
    onlineSegments.add(segmentWithoutTimeRangeMetadata);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, input), input);
    assertEquals(segmentPruner.prune(brokerRequest2, input), input);
    assertEquals(segmentPruner.prune(brokerRequest3, input), input);
    assertEquals(segmentPruner.prune(brokerRequest4, input), input);
    assertEquals(segmentPruner.prune(brokerRequest5, input), input);
    assertEquals(segmentPruner.prune(brokerRequest6, input), input);
    assertEquals(segmentPruner.prune(brokerRequest7, input), input);
    assertEquals(segmentPruner.prune(brokerRequest8, input), input);
    assertEquals(segmentPruner.prune(brokerRequest9, input), Set.of()); // Query with invalid range

    // Test different time range
    String segment0 = "segment0";
    setSegmentZKTimeRangeMetadata(REALTIME_TABLE_NAME, segment0, 10, 60, TimeUnit.DAYS);
    onlineSegments.add(segment0);
    String segment1 = "segment1";
    setSegmentZKTimeRangeMetadata(REALTIME_TABLE_NAME, segment1, 20, 30, TimeUnit.DAYS);
    onlineSegments.add(segment1);
    String segment2 = "segment2";
    setSegmentZKTimeRangeMetadata(REALTIME_TABLE_NAME, segment2, 50, 65, TimeUnit.DAYS);
    onlineSegments.add(segment2);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    input = Set.of(segment0, segment1, segment2);
    assertEquals(segmentPruner.prune(brokerRequest1, input), input);
    assertEquals(segmentPruner.prune(brokerRequest2, input), Set.of(segment0));
    assertEquals(segmentPruner.prune(brokerRequest3, input), Set.of(segment0, segment1));
    assertEquals(segmentPruner.prune(brokerRequest4, input), Set.of(segment0, segment2));
    assertEquals(segmentPruner.prune(brokerRequest5, input), Set.of(segment0, segment2));
    assertEquals(segmentPruner.prune(brokerRequest6, input), Set.of(segment0, segment2));
    assertEquals(segmentPruner.prune(brokerRequest7, input), Set.of(segment0, segment2));
    assertEquals(segmentPruner.prune(brokerRequest8, input), Set.of(segment0, segment1));
    assertEquals(segmentPruner.prune(brokerRequest9, input), Set.of()); // Query with invalid range

    // Update metadata without external view change or refreshing should have no effect
    setSegmentZKTimeRangeMetadata(REALTIME_TABLE_NAME, segment2, 20, 30, TimeUnit.DAYS);
    assertEquals(segmentPruner.prune(brokerRequest1, input), input);
    assertEquals(segmentPruner.prune(brokerRequest2, input), Set.of(segment0));
    assertEquals(segmentPruner.prune(brokerRequest3, input), Set.of(segment0, segment1));
    assertEquals(segmentPruner.prune(brokerRequest4, input), Set.of(segment0, segment2));
    assertEquals(segmentPruner.prune(brokerRequest5, input), Set.of(segment0, segment2));
    assertEquals(segmentPruner.prune(brokerRequest6, input), Set.of(segment0, segment2));
    assertEquals(segmentPruner.prune(brokerRequest7, input), Set.of(segment0, segment2));
    assertEquals(segmentPruner.prune(brokerRequest8, input), Set.of(segment0, segment1));
    assertEquals(segmentPruner.prune(brokerRequest9, input), Set.of()); // Query with invalid range

    // Refresh the changed segment should update the segment pruner
    segmentZkMetadataFetcher.refreshSegment(segment2);
    assertEquals(segmentPruner.prune(brokerRequest1, input), input);
    assertEquals(segmentPruner.prune(brokerRequest2, input), Set.of(segment0));
    assertEquals(segmentPruner.prune(brokerRequest3, input), input);
    assertEquals(segmentPruner.prune(brokerRequest4, input), Set.of(segment0));
    assertEquals(segmentPruner.prune(brokerRequest5, input), Set.of(segment0));
    assertEquals(segmentPruner.prune(brokerRequest6, input), Set.of(segment0));
    assertEquals(segmentPruner.prune(brokerRequest7, input), Set.of(segment0));
    assertEquals(segmentPruner.prune(brokerRequest8, input), input);
    assertEquals(segmentPruner.prune(brokerRequest9, input), Set.of()); // Query with invalid range
  }

  @Test
  public void testTimeSegmentPrunerSimpleDateFormat() {
    BrokerRequest brokerRequest1 = CalciteSqlCompiler.compileToBrokerRequest(SDF_QUERY_1);
    BrokerRequest brokerRequest2 = CalciteSqlCompiler.compileToBrokerRequest(SDF_QUERY_2);
    BrokerRequest brokerRequest3 = CalciteSqlCompiler.compileToBrokerRequest(SDF_QUERY_3);
    BrokerRequest brokerRequest4 = CalciteSqlCompiler.compileToBrokerRequest(SDF_QUERY_4);
    BrokerRequest brokerRequest5 = CalciteSqlCompiler.compileToBrokerRequest(SDF_QUERY_5);

    // NOTE: Ideal state and external view are not used in the current implementation
    IdealState idealState = Mockito.mock(IdealState.class);
    ExternalView externalView = Mockito.mock(ExternalView.class);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    DateTimeFieldSpec timeFieldSpec =
        new DateTimeFieldSpec(TIME_COLUMN, DataType.STRING, "SIMPLE_DATE_FORMAT|" + SDF_PATTERN, "1:DAYS");
    TimeSegmentPruner segmentPruner = new TimeSegmentPruner(tableConfig, timeFieldSpec);
    SegmentZkMetadataFetcher segmentZkMetadataFetcher =
        new SegmentZkMetadataFetcher(REALTIME_TABLE_NAME, _propertyStore);
    segmentZkMetadataFetcher.register(segmentPruner);
    DateTimeFormatSpec timeFormatSpec = timeFieldSpec.getFormatSpec();
    Set<String> onlineSegments = new HashSet<>();
    String segment0 = "segment0";
    setSegmentZKTimeRangeMetadata(REALTIME_TABLE_NAME, segment0, timeFormatSpec.fromFormatToMillis("20200101"),
        timeFormatSpec.fromFormatToMillis("20200228"), TimeUnit.MILLISECONDS);
    onlineSegments.add(segment0);
    String segment1 = "segment1";
    setSegmentZKTimeRangeMetadata(REALTIME_TABLE_NAME, segment1, timeFormatSpec.fromFormatToMillis("20200201"),
        timeFormatSpec.fromFormatToMillis("20200530"), TimeUnit.MILLISECONDS);
    onlineSegments.add(segment1);
    String segment2 = "segment2";
    setSegmentZKTimeRangeMetadata(REALTIME_TABLE_NAME, segment2, timeFormatSpec.fromFormatToMillis("20200401"),
        timeFormatSpec.fromFormatToMillis("20200430"), TimeUnit.MILLISECONDS);
    onlineSegments.add(segment2);
    segmentZkMetadataFetcher.init(idealState, externalView, onlineSegments);

    Set<String> input = Set.of(segment0, segment1, segment2);
    assertEquals(segmentPruner.prune(brokerRequest1, input), Set.of(segment0));
    assertEquals(segmentPruner.prune(brokerRequest2, input), Set.of(segment0, segment1));
    assertEquals(segmentPruner.prune(brokerRequest3, input), Set.of(segment1));
    assertEquals(segmentPruner.prune(brokerRequest4, input), input);
    assertEquals(segmentPruner.prune(brokerRequest5, input), Set.of());
  }

  @Test
  public void testTimeSegmentPrunerTimestampFormat() {
    BrokerRequest brokerRequest1 = CalciteSqlCompiler.compileToBrokerRequest(TIMESTAMP_QUERY_1);
    BrokerRequest brokerRequest2 = CalciteSqlCompiler.compileToBrokerRequest(TIMESTAMP_QUERY_2);
    BrokerRequest brokerRequest3 = CalciteSqlCompiler.compileToBrokerRequest(TIMESTAMP_QUERY_3);
    BrokerRequest brokerRequest4 = CalciteSqlCompiler.compileToBrokerRequest(TIMESTAMP_QUERY_4);
    BrokerRequest brokerRequest5 = CalciteSqlCompiler.compileToBrokerRequest(TIMESTAMP_QUERY_5);
    BrokerRequest brokerRequest6 = CalciteSqlCompiler.compileToBrokerRequest(TIMESTAMP_QUERY_6);

    // NOTE: Ideal state and external view are not used in the current implementation
    IdealState idealState = Mockito.mock(IdealState.class);
    ExternalView externalView = Mockito.mock(ExternalView.class);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    // Intentionally put EPOCH as the format which Pinot should handle
    DateTimeFieldSpec timeFieldSpec =
        new DateTimeFieldSpec(TIME_COLUMN, DataType.TIMESTAMP, "EPOCH|MILLISECONDS", "1:DAYS");
    TimeSegmentPruner segmentPruner = new TimeSegmentPruner(tableConfig, timeFieldSpec);
    SegmentZkMetadataFetcher segmentZkMetadataFetcher =
        new SegmentZkMetadataFetcher(REALTIME_TABLE_NAME, _propertyStore);
    segmentZkMetadataFetcher.register(segmentPruner);
    DateTimeFormatSpec timeFormatSpec = timeFieldSpec.getFormatSpec();
    Set<String> onlineSegments = new HashSet<>();
    String segment0 = "segment0";
    setSegmentZKTimeRangeMetadata(REALTIME_TABLE_NAME, segment0,
        timeFormatSpec.fromFormatToMillis("2020-01-01 00:00:00"),
        timeFormatSpec.fromFormatToMillis("2020-02-28 00:00:00"), TimeUnit.MILLISECONDS);
    onlineSegments.add(segment0);
    String segment1 = "segment1";
    setSegmentZKTimeRangeMetadata(REALTIME_TABLE_NAME, segment1,
        timeFormatSpec.fromFormatToMillis("2020-02-01 00:00:00"),
        timeFormatSpec.fromFormatToMillis("2020-05-30 00:00:00"), TimeUnit.MILLISECONDS);
    onlineSegments.add(segment1);
    String segment2 = "segment2";
    setSegmentZKTimeRangeMetadata(REALTIME_TABLE_NAME, segment2,
        timeFormatSpec.fromFormatToMillis("2020-04-01 00:00:00"),
        timeFormatSpec.fromFormatToMillis("2020-04-30 00:00:00"), TimeUnit.MILLISECONDS);
    onlineSegments.add(segment2);
    segmentZkMetadataFetcher.init(idealState, externalView, onlineSegments);

    Set<String> input = Set.of(segment0, segment1, segment2);
    assertEquals(segmentPruner.prune(brokerRequest1, input), Set.of(segment0));
    assertEquals(segmentPruner.prune(brokerRequest2, input), Set.of(segment0));
    assertEquals(segmentPruner.prune(brokerRequest3, input), Set.of(segment0, segment1));
    assertEquals(segmentPruner.prune(brokerRequest4, input), Set.of(segment0, segment1));
    assertEquals(segmentPruner.prune(brokerRequest5, input), input);
    assertEquals(segmentPruner.prune(brokerRequest6, input), input);
  }

  @Test
  public void testEmptySegmentPruner() {
    BrokerRequest brokerRequest1 = CalciteSqlCompiler.compileToBrokerRequest(QUERY_1);

    // NOTE: Ideal state and external view are not used in the current implementation
    IdealState idealState = Mockito.mock(IdealState.class);
    ExternalView externalView = Mockito.mock(ExternalView.class);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).build();

    // Init with a list of segments
    EmptySegmentPruner segmentPruner = new EmptySegmentPruner(tableConfig);
    SegmentZkMetadataFetcher segmentZkMetadataFetcher =
        new SegmentZkMetadataFetcher(REALTIME_TABLE_NAME, _propertyStore);
    segmentZkMetadataFetcher.register(segmentPruner);
    Set<String> onlineSegments = new HashSet<>();
    String segment0 = "segment0";
    setSegmentZKTotalDocsMetadata(REALTIME_TABLE_NAME, segment0, 10);
    onlineSegments.add(segment0);
    String segment1 = "segment1";
    setSegmentZKTotalDocsMetadata(REALTIME_TABLE_NAME, segment1, 0);
    onlineSegments.add(segment1);
    segmentZkMetadataFetcher.init(idealState, externalView, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, onlineSegments), Set.of(segment0));

    // Init with no segment
    segmentPruner = new EmptySegmentPruner(tableConfig);
    segmentZkMetadataFetcher = new SegmentZkMetadataFetcher(REALTIME_TABLE_NAME, _propertyStore);
    segmentZkMetadataFetcher.register(segmentPruner);
    onlineSegments.clear();
    segmentZkMetadataFetcher.init(idealState, externalView, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, onlineSegments), onlineSegments);

    // Segments without metadata (not updated yet) should not be pruned
    String newSegment = "newSegment";
    onlineSegments.add(newSegment);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, onlineSegments), onlineSegments);

    // Segments without totalDocs metadata should not be pruned
    String segmentWithoutTotalDocsMetadata = "segmentWithoutTotalDocsMetadata";
    SegmentZKMetadata segmentZKMetadataWithoutTotalDocsMetadata =
        new SegmentZKMetadata(segmentWithoutTotalDocsMetadata);
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, REALTIME_TABLE_NAME,
        segmentZKMetadataWithoutTotalDocsMetadata);
    onlineSegments.add(segmentWithoutTotalDocsMetadata);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, onlineSegments), onlineSegments);

    // Segments with -1 totalDocs should not be pruned
    String segmentWithNegativeTotalDocsMetadata = "segmentWithNegativeTotalDocsMetadata";
    setSegmentZKTotalDocsMetadata(REALTIME_TABLE_NAME, segmentWithNegativeTotalDocsMetadata, -1);
    onlineSegments.add(segmentWithNegativeTotalDocsMetadata);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, onlineSegments), onlineSegments);

    // Prune segments with 0 total docs
    onlineSegments.clear();
    setSegmentZKTotalDocsMetadata(REALTIME_TABLE_NAME, segment0, 10);
    onlineSegments.add(segment0);
    setSegmentZKTotalDocsMetadata(REALTIME_TABLE_NAME, segment1, 0);
    onlineSegments.add(segment1);
    String segment2 = "segment2";
    setSegmentZKTotalDocsMetadata(REALTIME_TABLE_NAME, segment2, -1);
    onlineSegments.add(segment2);
    segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, onlineSegments), Set.of(segment0, segment2));

    // Update metadata without external view change or refreshing should have no effect
    setSegmentZKTotalDocsMetadata(REALTIME_TABLE_NAME, segment2, 0);
    assertEquals(segmentPruner.prune(brokerRequest1, onlineSegments), Set.of(segment0, segment2));

    // Refresh the changed segment should update the segment pruner
    segmentZkMetadataFetcher.refreshSegment(segment2);
    assertEquals(segmentPruner.prune(brokerRequest1, onlineSegments), Set.of(segment0));
  }

  private void setSegmentZKPartitionMetadata(String tableNameWithType, String segment, String partitionFunction,
      int numPartitions, int partitionId) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segment);
    segmentZKMetadata.setPartitionMetadata(new SegmentPartitionMetadata(Map.of(PARTITION_COLUMN_1,
        new ColumnPartitionMetadata(partitionFunction, numPartitions, Set.of(partitionId), null))));
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, tableNameWithType, segmentZKMetadata);
  }

  private void setSegmentZKPartitionMetadata(String tableNameWithType, String segment,
      Map<String, ColumnPartitionMetadata> columnPartitionMap) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segment);
    segmentZKMetadata.setPartitionMetadata(new SegmentPartitionMetadata(columnPartitionMap));
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, tableNameWithType, segmentZKMetadata);
  }

  private void setSegmentZKTimeRangeMetadata(String tableNameWithType, String segment, long startTime, long endTime,
      TimeUnit unit) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segment);
    segmentZKMetadata.setStartTime(startTime);
    segmentZKMetadata.setEndTime(endTime);
    segmentZKMetadata.setTimeUnit(unit);
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, tableNameWithType, segmentZKMetadata);
  }

  private void setSegmentZKTotalDocsMetadata(String tableNameWithType, String segment, long totalDocs) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segment);
    segmentZKMetadata.setTotalDocs(totalDocs);
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, tableNameWithType, segmentZKMetadata);
  }
}
