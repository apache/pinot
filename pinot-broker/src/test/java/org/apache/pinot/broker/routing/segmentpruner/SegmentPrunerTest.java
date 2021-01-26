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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.ColumnPartitionMetadata;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
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


public class SegmentPrunerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = "testTable_OFFLINE";
  private static final String REALTIME_TABLE_NAME = "testTable_REALTIME";
  private static final String PARTITION_COLUMN = "memberId";
  private static final String TIME_COLUMN = "timeColumn";
  private static final String SDF_PATTERN = "yyyyMMdd";

  private static final String QUERY_1 = "SELECT * FROM testTable";
  private static final String QUERY_2 = "SELECT * FROM testTable where memberId = 0";
  private static final String QUERY_3 = "SELECT * FROM testTable where memberId IN (1, 2)";

  private static final String QUERY_5 = "SELECT * FROM testTable where timeColumn = 40";
  private static final String QUERY_6 = "SELECT * FROM testTable where timeColumn BETWEEN 20 AND 30";
  private static final String QUERY_7 = "SELECT * FROM testTable where 30 < timeColumn AND timeColumn <= 50";
  private static final String QUERY_8 = "SELECT * FROM testTable where timeColumn < 15 OR timeColumn > 45";
  private static final String QUERY_9 = "SELECT * FROM testTable where timeColumn < 15 OR (60 < timeColumn AND timeColumn < 70)";
  private static final String QUERY_10 = "SELECT * FROM testTable where timeColumn < 0 AND timeColumn > 0";

  private static final String SDF_QUERY_1 = "SELECT * FROM testTable where timeColumn = 20200131";
  private static final String SDF_QUERY_2 = "SELECT * FROM testTable where timeColumn BETWEEN 20200101 AND 20200331";
  private static final String SDF_QUERY_3 = "SELECT * FROM testTable where 20200430 < timeColumn AND timeColumn < 20200630";
  private static final String SDF_QUERY_4 = "SELECT * FROM testTable where timeColumn <= 20200101 OR timeColumn in (20200201, 20200401)";
  private static final String SDF_QUERY_5 = "SELECT * FROM testTable where timeColumn in (20200101, 20200102) AND timeColumn >= 20200530";

  private ZkStarter.ZookeeperInstance _zkInstance;
  private ZkClient _zkClient;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  @BeforeClass
  public void setUp() {
    _zkInstance = ZkStarter.startLocalZkServer();
    _zkClient =
        new ZkClient(ZkStarter.DEFAULT_ZK_STR, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT,
            new ZNRecordSerializer());
    _propertyStore =
        new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<>(_zkClient), "/SegmentPrunerTest/PROPERTYSTORE", null);
  }

  @AfterClass
  public void tearDown() {
    _zkClient.close();
    ZkStarter.stopLocalZkServer(_zkInstance);
  }

  @Test
  public void testSegmentPrunerFactoryForPartitionPruner() {
    TableConfig tableConfig = mock(TableConfig.class);
    IndexingConfig indexingConfig = mock(IndexingConfig.class);
    when(tableConfig.getIndexingConfig()).thenReturn(indexingConfig);

    // Routing config is missing
    assertEquals(SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore), Collections.emptySet());

    // Segment pruner type is not configured
    RoutingConfig routingConfig = mock(RoutingConfig.class);
    when(tableConfig.getRoutingConfig()).thenReturn(routingConfig);
    assertEquals(SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore), Collections.emptySet());

    // Segment partition config is missing
    when(routingConfig.getSegmentPrunerTypes())
        .thenReturn(Collections.singletonList(RoutingConfig.PARTITION_SEGMENT_PRUNER_TYPE));
    assertEquals(SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore), Collections.emptySet());

    // Column partition config is missing
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    when(indexingConfig.getSegmentPartitionConfig()).thenReturn(new SegmentPartitionConfig(columnPartitionConfigMap));
    assertEquals(SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore), Collections.emptySet());

    // Partition-aware segment pruner should be returned
    columnPartitionConfigMap.put(PARTITION_COLUMN, new ColumnPartitionConfig("Modulo", 5));
    List<SegmentPruner> segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 1);
    assertTrue(segmentPruners.get(0) instanceof PartitionSegmentPruner);

    // Do not allow multiple partition columns
    columnPartitionConfigMap.put("anotherPartitionColumn", new ColumnPartitionConfig("Modulo", 5));
    assertEquals(SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore), Collections.emptySet());

    // Should be backward-compatible with legacy config
    columnPartitionConfigMap.remove("anotherPartitionColumn");
    when(routingConfig.getSegmentPrunerTypes()).thenReturn(null);
    assertEquals(SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore), Collections.emptySet());
    when(tableConfig.getTableType()).thenReturn(TableType.OFFLINE);
    when(routingConfig.getRoutingTableBuilderName())
        .thenReturn(SegmentPrunerFactory.LEGACY_PARTITION_AWARE_OFFLINE_ROUTING);
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 1);
    assertTrue(segmentPruners.get(0) instanceof PartitionSegmentPruner);
    when(tableConfig.getTableType()).thenReturn(TableType.REALTIME);
    when(routingConfig.getRoutingTableBuilderName())
        .thenReturn(SegmentPrunerFactory.LEGACY_PARTITION_AWARE_REALTIME_ROUTING);
    segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 1);
    assertTrue(segmentPruners.get(0) instanceof PartitionSegmentPruner);
  }

  @Test
  public void testSegmentPrunerFactoryForTimeRangePruner() {
    TableConfig tableConfig = mock(TableConfig.class);
    when(tableConfig.getTableName()).thenReturn(RAW_TABLE_NAME);
    setSchemaDateTimeFieldSpec(RAW_TABLE_NAME, TimeUnit.HOURS);

    // Routing config is missing
    assertEquals(SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore), Collections.emptySet());

    // Segment pruner type is not configured
    RoutingConfig routingConfig = mock(RoutingConfig.class);
    when(tableConfig.getRoutingConfig()).thenReturn(routingConfig);
    assertEquals(SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore), Collections.emptySet());

    // Validation config is missing
    when(routingConfig.getSegmentPrunerTypes())
        .thenReturn(Collections.singletonList(RoutingConfig.TIME_SEGMENT_PRUNER_TYPE));
    assertEquals(SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore), Collections.emptySet());

    // Time column is missing
    SegmentsValidationAndRetentionConfig validationConfig = mock(SegmentsValidationAndRetentionConfig.class);
    when(tableConfig.getValidationConfig()).thenReturn(validationConfig);
    assertEquals(SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore), Collections.emptySet());

    // Time range pruner should be returned
    when(validationConfig.getTimeColumnName()).thenReturn(TIME_COLUMN);
    List<SegmentPruner> segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    assertEquals(segmentPruners.size(), 1);
    assertTrue(segmentPruners.get(0) instanceof TimeSegmentPruner);

  }

  @Test
  public void testPartitionAwareSegmentPruner() {
    Pql2Compiler compiler = new Pql2Compiler();
    BrokerRequest brokerRequest1 = compiler.compileToBrokerRequest(QUERY_1);
    BrokerRequest brokerRequest2 = compiler.compileToBrokerRequest(QUERY_2);
    BrokerRequest brokerRequest3 = compiler.compileToBrokerRequest(QUERY_3);
    // NOTE: External view and ideal state are not used in the current implementation.
    ExternalView externalView = Mockito.mock(ExternalView.class);
    IdealState idealState = Mockito.mock(IdealState.class);

    PartitionSegmentPruner segmentPruner =
        new PartitionSegmentPruner(OFFLINE_TABLE_NAME, PARTITION_COLUMN, _propertyStore);
    Set<String> onlineSegments = new HashSet<>();
    segmentPruner.init(externalView, idealState, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, Collections.emptySet()), Collections.emptySet());
    assertEquals(segmentPruner.prune(brokerRequest2, Collections.emptySet()), Collections.emptySet());
    assertEquals(segmentPruner.prune(brokerRequest3, Collections.emptySet()), Collections.emptySet());

    // Segments without metadata (not updated yet) should not be pruned
    String newSegment = "newSegment";
    assertEquals(segmentPruner.prune(brokerRequest1, new HashSet<>(Collections.singletonList(newSegment))),
        Collections.singletonList(newSegment));
    assertEquals(segmentPruner.prune(brokerRequest2, new HashSet<>(Collections.singletonList(newSegment))),
        Collections.singletonList(newSegment));
    assertEquals(segmentPruner.prune(brokerRequest3, new HashSet<>(Collections.singletonList(newSegment))),
        Collections.singletonList(newSegment));

    // Segments without partition metadata should not be pruned
    String segmentWithoutPartitionMetadata = "segmentWithoutPartitionMetadata";
    onlineSegments.add(segmentWithoutPartitionMetadata);
    OfflineSegmentZKMetadata segmentZKMetadataWithoutPartitionMetadata = new OfflineSegmentZKMetadata();
    segmentZKMetadataWithoutPartitionMetadata.setSegmentName(segmentWithoutPartitionMetadata);
    ZKMetadataProvider
        .setOfflineSegmentZKMetadata(_propertyStore, OFFLINE_TABLE_NAME, segmentZKMetadataWithoutPartitionMetadata);
    segmentPruner.onExternalViewChange(externalView, idealState, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, new HashSet<>(Collections.singletonList(segmentWithoutPartitionMetadata))),
        Collections.singletonList(segmentWithoutPartitionMetadata));
    assertEquals(segmentPruner.prune(brokerRequest2, new HashSet<>(Collections.singletonList(segmentWithoutPartitionMetadata))),
        Collections.singletonList(segmentWithoutPartitionMetadata));
    assertEquals(segmentPruner.prune(brokerRequest3, new HashSet<>(Collections.singletonList(segmentWithoutPartitionMetadata))),
        Collections.singletonList(segmentWithoutPartitionMetadata));

    // Test different partition functions and number of partitions
    // 0 % 5 = 0; 1 % 5 = 1; 2 % 5 = 2
    String segment0 = "segment0";
    onlineSegments.add(segment0);
    setSegmentZKPartitionMetadata(segment0, "Modulo", 5, 0);
    // Murmur(0) % 4 = 0; Murmur(1) % 4 = 3; Murmur(2) % 4 = 0
    String segment1 = "segment1";
    onlineSegments.add(segment1);
    setSegmentZKPartitionMetadata(segment1, "Murmur", 4, 0);
    segmentPruner.onExternalViewChange(externalView, idealState, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, new HashSet<>(Arrays.asList(segment0, segment1))),
        new HashSet<>(Arrays.asList(segment0, segment1)));
    assertEquals(segmentPruner.prune(brokerRequest2, new HashSet<>(Arrays.asList(segment0, segment1))),
        new HashSet<>(Arrays.asList(segment0, segment1)));
    assertEquals(segmentPruner.prune(brokerRequest3, new HashSet<>(Arrays.asList(segment0, segment1))),
        new HashSet<>(Collections.singletonList(segment1)));

    // Update partition metadata without refreshing should have no effect
    setSegmentZKPartitionMetadata(segment0, "Modulo", 4, 1);
    segmentPruner.onExternalViewChange(externalView, idealState, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, new HashSet<>(Arrays.asList(segment0, segment1))),
        new HashSet<>(Arrays.asList(segment0, segment1)));
    assertEquals(segmentPruner.prune(brokerRequest2, new HashSet<>(Arrays.asList(segment0, segment1))),
        new HashSet<>(Arrays.asList(segment0, segment1)));
    assertEquals(segmentPruner.prune(brokerRequest3, new HashSet<>(Arrays.asList(segment0, segment1))),
        new HashSet<>(Collections.singletonList(segment1)));

    // Refresh the changed segment should update the segment pruner
    segmentPruner.refreshSegment(segment0);
    assertEquals(segmentPruner.prune(brokerRequest1, new HashSet<>(Arrays.asList(segment0, segment1))),
        new HashSet<>(Arrays.asList(segment0, segment1)));
    assertEquals(segmentPruner.prune(brokerRequest2, new HashSet<>(Arrays.asList(segment0, segment1))),
        new HashSet<>(Collections.singletonList(segment1)));
    assertEquals(segmentPruner.prune(brokerRequest3, new HashSet<>(Arrays.asList(segment0, segment1))),
        new HashSet<>(Arrays.asList(segment0, segment1)));
  }

  @Test
  public void testTimeSegmentPruner() {
    Pql2Compiler pql2Compiler = new Pql2Compiler();
    CalciteSqlCompiler sqlCompiler = new CalciteSqlCompiler();
    BrokerRequest brokerRequest1 = pql2Compiler.compileToBrokerRequest(QUERY_1);
    BrokerRequest brokerRequest2 = pql2Compiler.compileToBrokerRequest(QUERY_5);
    BrokerRequest brokerRequest3 = pql2Compiler.compileToBrokerRequest(QUERY_6);
    BrokerRequest brokerRequest4 = pql2Compiler.compileToBrokerRequest(QUERY_7);
    BrokerRequest brokerRequest5 = sqlCompiler.compileToBrokerRequest(QUERY_8);
    BrokerRequest brokerRequest6 = sqlCompiler.compileToBrokerRequest(QUERY_9);
    BrokerRequest brokerRequest7 = sqlCompiler.compileToBrokerRequest(QUERY_10);
    // NOTE: External view and ideal state are not used in the current implementation.
    ExternalView externalView = Mockito.mock(ExternalView.class);
    IdealState idealState = Mockito.mock(IdealState.class);

    TableConfig tableConfig = getTableConfig(RAW_TABLE_NAME, TableType.REALTIME);
    setSchemaDateTimeFieldSpec(RAW_TABLE_NAME, TimeUnit.DAYS);

    TimeSegmentPruner segmentPruner = new TimeSegmentPruner(tableConfig, _propertyStore);
    Set<String> onlineSegments = new HashSet<>();
    segmentPruner.init(externalView, idealState, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, Collections.emptySet()), Collections.emptySet());
    assertEquals(segmentPruner.prune(brokerRequest2, Collections.emptySet()), Collections.emptySet());
    assertEquals(segmentPruner.prune(brokerRequest3, Collections.emptySet()), Collections.emptySet());
    assertEquals(segmentPruner.prune(brokerRequest4, Collections.emptySet()), Collections.emptySet());
    assertEquals(segmentPruner.prune(brokerRequest5, Collections.emptySet()), Collections.emptySet());
    assertEquals(segmentPruner.prune(brokerRequest6, Collections.emptySet()), Collections.emptySet());
    assertEquals(segmentPruner.prune(brokerRequest7, Collections.emptySet()), Collections.emptySet());

    // Initialize with non-empty onlineSegments
    // Segments without metadata (not updated yet) should not be pruned
    segmentPruner = new TimeSegmentPruner(tableConfig, _propertyStore);
    String newSegment = "newSegment";
    onlineSegments.add(newSegment);
    segmentPruner.init(externalView, idealState, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, new HashSet<>(Collections.singletonList(newSegment))),
        Collections.singletonList(newSegment));
    assertEquals(segmentPruner.prune(brokerRequest2, new HashSet<>(Collections.singletonList(newSegment))),
        Collections.singletonList(newSegment));
    assertEquals(segmentPruner.prune(brokerRequest3, new HashSet<>(Collections.singletonList(newSegment))),
        Collections.singletonList(newSegment));
    assertEquals(segmentPruner.prune(brokerRequest4, new HashSet<>(Collections.singletonList(newSegment))),
        Collections.singletonList(newSegment));
    assertEquals(segmentPruner.prune(brokerRequest5, new HashSet<>(Collections.singletonList(newSegment))),
        Collections.singletonList(newSegment));
    assertEquals(segmentPruner.prune(brokerRequest6, new HashSet<>(Collections.singletonList(newSegment))),
        Collections.singletonList(newSegment));
    assertEquals(segmentPruner.prune(brokerRequest7, new HashSet<>(Collections.singletonList(newSegment))),
        Collections.emptySet()); // query with invalid range will always have empty filtered result

    // Segments without time range metadata should not be pruned
    String segmentWithoutTimeRangeMetadata = "segmentWithoutTimeRangeMetadata";
    onlineSegments.add(segmentWithoutTimeRangeMetadata);
    RealtimeSegmentZKMetadata segmentZKMetadataWithoutTimeRangeMetadata = new RealtimeSegmentZKMetadata();
    segmentZKMetadataWithoutTimeRangeMetadata.setSegmentName(segmentWithoutTimeRangeMetadata);
    segmentZKMetadataWithoutTimeRangeMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    ZKMetadataProvider
        .setRealtimeSegmentZKMetadata(_propertyStore, REALTIME_TABLE_NAME, segmentZKMetadataWithoutTimeRangeMetadata);
    segmentPruner.onExternalViewChange(externalView, idealState, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, new HashSet<>(Collections.singletonList(segmentWithoutTimeRangeMetadata))),
        Collections.singletonList(segmentWithoutTimeRangeMetadata));
    assertEquals(segmentPruner.prune(brokerRequest2, new HashSet<>(Collections.singletonList(segmentWithoutTimeRangeMetadata))),
        Collections.singletonList(segmentWithoutTimeRangeMetadata));
    assertEquals(segmentPruner.prune(brokerRequest3, new HashSet<>(Collections.singletonList(segmentWithoutTimeRangeMetadata))),
        Collections.singletonList(segmentWithoutTimeRangeMetadata));
    assertEquals(segmentPruner.prune(brokerRequest4, new HashSet<>(Collections.singletonList(segmentWithoutTimeRangeMetadata))),
        Collections.singletonList(segmentWithoutTimeRangeMetadata));
    assertEquals(segmentPruner.prune(brokerRequest5, new HashSet<>(Collections.singletonList(segmentWithoutTimeRangeMetadata))),
        Collections.singletonList(segmentWithoutTimeRangeMetadata));
    assertEquals(segmentPruner.prune(brokerRequest6, new HashSet<>(Collections.singletonList(segmentWithoutTimeRangeMetadata))),
        Collections.singletonList(segmentWithoutTimeRangeMetadata));
    assertEquals(segmentPruner.prune(brokerRequest7, new HashSet<>(Collections.singletonList(segmentWithoutTimeRangeMetadata))),
        Collections.emptySet());

    // Test different time range
    String segment0 = "segment0";
    onlineSegments.add(segment0);
    setSegmentZKTimeRangeMetadata(segment0,10, 60, TimeUnit.DAYS);

    String segment1 = "segment1";
    onlineSegments.add(segment1);
    setSegmentZKTimeRangeMetadata(segment1, 20, 30, TimeUnit.DAYS);

    String segment2 = "segment2";
    onlineSegments.add(segment2);
    setSegmentZKTimeRangeMetadata(segment2, 50, 65, TimeUnit.DAYS);

    segmentPruner.onExternalViewChange(externalView, idealState, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, new HashSet<>(Arrays.asList(segment0, segment1, segment2))),
        new HashSet<>(Arrays.asList(segment0, segment1, segment2)));
    assertEquals(new HashSet(segmentPruner.prune(brokerRequest2, new HashSet<>(Arrays.asList(segment0, segment1, segment2)))),
        new HashSet(Collections.singletonList(segment0)));
    assertEquals(new HashSet(segmentPruner.prune(brokerRequest3, new HashSet<>(Arrays.asList(segment0, segment1, segment2)))),
        new HashSet(Arrays.asList(segment0, segment1)));
    assertEquals(new HashSet(segmentPruner.prune(brokerRequest4, new HashSet<>(Arrays.asList(segment0, segment1, segment2)))),
        new HashSet(Arrays.asList(segment0, segment2)));
    assertEquals(new HashSet(segmentPruner.prune(brokerRequest5, new HashSet<>(Arrays.asList(segment0, segment1, segment2)))),
        new HashSet(Arrays.asList(segment0, segment2)));
    assertEquals(new HashSet(segmentPruner.prune(brokerRequest6, new HashSet<>(Arrays.asList(segment0, segment1, segment2)))),
        new HashSet(Arrays.asList(segment0, segment2)));
    assertEquals(new HashSet(segmentPruner.prune(brokerRequest7, new HashSet<>(Arrays.asList(segment0, segment1, segment2)))),
        Collections.emptySet());

    // Update metadata without external view change or refreshing should have no effect
    setSegmentZKTimeRangeMetadata(segment2, 20, 30, TimeUnit.DAYS);
    assertEquals(segmentPruner.prune(brokerRequest1,  new HashSet<>(Arrays.asList(segment0, segment1, segment2))),
        new HashSet<>(Arrays.asList(segment0, segment1, segment2)));
    assertEquals(new HashSet(segmentPruner.prune(brokerRequest2, new HashSet<>(Arrays.asList(segment0, segment1, segment2)))),
        new HashSet(Collections.singletonList(segment0)));
    assertEquals(new HashSet(segmentPruner.prune(brokerRequest3, new HashSet<>(Arrays.asList(segment0, segment1, segment2)))),
        new HashSet(Arrays.asList(segment0, segment1)));
    assertEquals(new HashSet(segmentPruner.prune(brokerRequest4, new HashSet<>(Arrays.asList(segment0, segment1, segment2)))),
        new HashSet(Arrays.asList(segment0, segment2)));
    assertEquals(new HashSet(segmentPruner.prune(brokerRequest5, new HashSet<>(Arrays.asList(segment0, segment1, segment2)))),
        new HashSet(Arrays.asList(segment0, segment2)));
    assertEquals(new HashSet(segmentPruner.prune(brokerRequest6, new HashSet<>(Arrays.asList(segment0, segment1, segment2)))),
        new HashSet(Arrays.asList(segment0, segment2)));
    assertEquals(new HashSet(segmentPruner.prune(brokerRequest7, new HashSet<>(Arrays.asList(segment0, segment1, segment2)))),
        Collections.emptySet());

    // Refresh the changed segment should update the segment pruner
    segmentPruner.refreshSegment(segment2);
    assertEquals(segmentPruner.prune(brokerRequest1, new HashSet<>(Arrays.asList(segment0, segment1, segment2))),
        new HashSet<>(Arrays.asList(segment0, segment1,segment2)));
    assertEquals(new HashSet(segmentPruner.prune(brokerRequest2, new HashSet<>(Arrays.asList(segment0, segment1, segment2)))),
        new HashSet(Collections.singletonList(segment0)));
    assertEquals(new HashSet(segmentPruner.prune(brokerRequest3, new HashSet<>(Arrays.asList(segment0, segment1, segment2)))),
        new HashSet(Arrays.asList(segment0, segment1, segment2)));
    assertEquals(new HashSet(segmentPruner.prune(brokerRequest4, new HashSet<>(Arrays.asList(segment0, segment1, segment2)))),
        new HashSet(Collections.singletonList(segment0)));
    assertEquals(new HashSet(segmentPruner.prune(brokerRequest5, new HashSet<>(Arrays.asList(segment0, segment1, segment2)))),
        new HashSet(Collections.singletonList(segment0)));
    assertEquals(new HashSet(segmentPruner.prune(brokerRequest6, new HashSet<>(Arrays.asList(segment0, segment1, segment2)))),
        new HashSet(Collections.singletonList(segment0)));
    assertEquals(new HashSet(segmentPruner.prune(brokerRequest7, new HashSet<>(Arrays.asList(segment0, segment1, segment2)))),
        Collections.emptySet());
  }

  @Test
  public void testTimeSegmentPrunerSimpleDateFormat() {
    CalciteSqlCompiler sqlCompiler = new CalciteSqlCompiler();
    BrokerRequest brokerRequest1 = sqlCompiler.compileToBrokerRequest(SDF_QUERY_1);
    BrokerRequest brokerRequest2 = sqlCompiler.compileToBrokerRequest(SDF_QUERY_2);
    BrokerRequest brokerRequest3 = sqlCompiler.compileToBrokerRequest(SDF_QUERY_3);
    BrokerRequest brokerRequest4 = sqlCompiler.compileToBrokerRequest(SDF_QUERY_4);
    BrokerRequest brokerRequest5 = sqlCompiler.compileToBrokerRequest(SDF_QUERY_5);

    ExternalView externalView = Mockito.mock(ExternalView.class);
    IdealState idealState = Mockito.mock(IdealState.class);

    TableConfig tableConfig = getTableConfig(RAW_TABLE_NAME, TableType.REALTIME);
    setSchemaDateTimeFieldSpecSDF(RAW_TABLE_NAME, SDF_PATTERN);

    TimeSegmentPruner segmentPruner = new TimeSegmentPruner(tableConfig, _propertyStore);
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, RAW_TABLE_NAME);
    DateTimeFormatSpec dateTimeFormatSpec = new DateTimeFormatSpec(schema.getSpecForTimeColumn(TIME_COLUMN).getFormat());

    Set<String> onlineSegments = new HashSet<>();
    String segment0 = "segment0";
    onlineSegments.add(segment0);
    setSegmentZKTimeRangeMetadata(segment0,
        dateTimeFormatSpec.fromFormatToMillis("20200101"),
        dateTimeFormatSpec.fromFormatToMillis("20200228"),
        TimeUnit.MILLISECONDS);

    String segment1 = "segment1";
    onlineSegments.add(segment1);
    setSegmentZKTimeRangeMetadata(segment1,
        dateTimeFormatSpec.fromFormatToMillis("20200201"),
        dateTimeFormatSpec.fromFormatToMillis("20200530"),
        TimeUnit.MILLISECONDS);

    String segment2 = "segment2";
    onlineSegments.add(segment2);
    setSegmentZKTimeRangeMetadata(segment2,
        dateTimeFormatSpec.fromFormatToMillis("20200401"),
        dateTimeFormatSpec.fromFormatToMillis("20200430"),
        TimeUnit.MILLISECONDS);

    segmentPruner.init(externalView, idealState, onlineSegments);
    assertEquals(segmentPruner.prune(brokerRequest1, onlineSegments), new HashSet<>(Collections.singletonList(segment0)));
    assertEquals(segmentPruner.prune(brokerRequest2, onlineSegments), new HashSet<>(Arrays.asList(segment0, segment1)));
    assertEquals(segmentPruner.prune(brokerRequest3, onlineSegments), new HashSet<>(Collections.singletonList(segment1)));
    assertEquals(segmentPruner.prune(brokerRequest4, onlineSegments), new HashSet<>(Arrays.asList(segment0, segment1, segment2)));
    assertEquals(segmentPruner.prune(brokerRequest5, onlineSegments), Collections.emptySet());
  }

  private TableConfig getTableConfig(String rawTableName, TableType type) {
    return new TableConfigBuilder(type).setTableName(rawTableName).setTimeColumnName(TIME_COLUMN).build();
  }

  private void setSchemaDateTimeFieldSpec(String rawTableName, TimeUnit timeUnit) {
    ZKMetadataProvider.setSchema(_propertyStore, new Schema.SchemaBuilder().setSchemaName(rawTableName)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:" + timeUnit + ":EPOCH", "1:" + timeUnit).build());
  }

  private void setSchemaDateTimeFieldSpecSDF(String rawTableName, String format) {
    ZKMetadataProvider.setSchema(_propertyStore, new Schema.SchemaBuilder().setSchemaName(rawTableName)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:" + format, "1:DAYS").build());
  }

  private void setSegmentZKPartitionMetadata(String segment, String partitionFunction, int numPartitions, int partitionId) {
    OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
    offlineSegmentZKMetadata.setSegmentName(segment);
    offlineSegmentZKMetadata.setPartitionMetadata(new SegmentPartitionMetadata(Collections
        .singletonMap(PARTITION_COLUMN,
            new ColumnPartitionMetadata(partitionFunction, numPartitions, Collections.singleton(partitionId)))));
    ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, OFFLINE_TABLE_NAME, offlineSegmentZKMetadata);
  }

  private void setSegmentZKTimeRangeMetadata(String segment, long startTime, long endTime, TimeUnit unit) {
    RealtimeSegmentZKMetadata realtimeSegmentZKMetadata = new RealtimeSegmentZKMetadata();
    realtimeSegmentZKMetadata.setSegmentName(segment);
    realtimeSegmentZKMetadata.setSegmentType(CommonConstants.Segment.SegmentType.REALTIME);
    realtimeSegmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    realtimeSegmentZKMetadata.setStartTime(startTime);
    realtimeSegmentZKMetadata.setEndTime(endTime);
    realtimeSegmentZKMetadata.setTimeUnit(unit);
    ZKMetadataProvider.setRealtimeSegmentZKMetadata(_propertyStore, REALTIME_TABLE_NAME, realtimeSegmentZKMetadata);
  }
}
