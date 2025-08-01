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
package org.apache.pinot.broker.routing.timeboundary;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class TimeBoundaryManagerTest extends ControllerTest {
  private static final String TIME_COLUMN = "time";

  private ZkClient _zkClient;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  @BeforeClass
  public void setUp() {
    startZk();
    _zkClient =
        new ZkClient(getZkUrl(), ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT,
            new ZNRecordSerializer());
    _propertyStore =
        new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<>(_zkClient), "/TimeBoundaryManagerTest/PROPERTYSTORE", null);
  }

  @AfterClass
  public void tearDown() {
    _zkClient.close();
    stopZk();
  }

  @Test
  public void testTimeBoundaryManager() {
    for (TimeUnit timeUnit : TimeUnit.values()) {
      // Test DAILY push table, with timeFieldSpec
      String rawTableName = "testTable_" + timeUnit + "_DAILY";
      TableConfig tableConfig = getTableConfig(rawTableName, "DAILY");
      setSchemaTimeFieldSpec(rawTableName, timeUnit);
      testDailyPushTable(rawTableName, tableConfig, timeUnit);

      // Test HOURLY push table, with timeFieldSpec
      rawTableName = "testTable_" + timeUnit + "_HOURLY";
      tableConfig = getTableConfig(rawTableName, "HOURLY");
      setSchemaTimeFieldSpec(rawTableName, timeUnit);
      testHourlyPushTable(rawTableName, tableConfig, timeUnit);

      // Test DAILY push table with dateTimeFieldSpec
      rawTableName = "testTableDateTime_" + timeUnit + "_DAILY";
      tableConfig = getTableConfig(rawTableName, "DAILY");
      setSchemaDateTimeFieldSpec(rawTableName, timeUnit);
      testDailyPushTable(rawTableName, tableConfig, timeUnit);

      // Test HOURLY push table
      rawTableName = "testTableDateTime_" + timeUnit + "_HOURLY";
      tableConfig = getTableConfig(rawTableName, "HOURLY");
      setSchemaDateTimeFieldSpec(rawTableName, timeUnit);
      testHourlyPushTable(rawTableName, tableConfig, timeUnit);
    }
  }

  private void testDailyPushTable(String rawTableName, TableConfig tableConfig, TimeUnit timeUnit) {
    ExternalView externalView = new ExternalView(tableConfig.getTableName());
    Map<String, Map<String, String>> segmentAssignment = externalView.getRecord().getMapFields();
    Map<String, String> onlineInstanceStateMap = Collections.singletonMap("server", ONLINE);
    Map<String, String> offlineInstanceStateMap = Collections.singletonMap("server", OFFLINE);
    Set<String> onlineSegments = new HashSet<>();
    // NOTE: Ideal state is not used in the current implementation.
    IdealState idealState = new IdealState("");

    // Start with no segment
    TimeBoundaryManager timeBoundaryManager =
        new TimeBoundaryManager(tableConfig, _propertyStore, Mockito.mock(BrokerMetrics.class));
    timeBoundaryManager.init(idealState, externalView, onlineSegments);
    assertNull(timeBoundaryManager.getTimeBoundaryInfo());

    // Add the first segment should update the time boundary
    String segment0 = "segment0";
    onlineSegments.add(segment0);
    segmentAssignment.put(segment0, onlineInstanceStateMap);
    setSegmentZKMetadata(rawTableName, segment0, 2, timeUnit);
    timeBoundaryManager.init(idealState, externalView, onlineSegments);
    verifyTimeBoundaryInfo(timeBoundaryManager.getTimeBoundaryInfo(), timeUnit.convert(1, TimeUnit.DAYS));

    // Add a new segment with larger end time but no ONLINE instance should not update the time boundary
    String segment1 = "segment1";
    onlineSegments.add(segment1);
    segmentAssignment.put(segment1, offlineInstanceStateMap);
    setSegmentZKMetadata(rawTableName, segment1, 4, timeUnit);
    timeBoundaryManager.onAssignmentChange(idealState, externalView, onlineSegments);
    verifyTimeBoundaryInfo(timeBoundaryManager.getTimeBoundaryInfo(), timeUnit.convert(1, TimeUnit.DAYS));

    // Turn the new segment ONLINE should update the time boundary
    segmentAssignment.put(segment1, onlineInstanceStateMap);
    timeBoundaryManager.onAssignmentChange(idealState, externalView, onlineSegments);
    verifyTimeBoundaryInfo(timeBoundaryManager.getTimeBoundaryInfo(), timeUnit.convert(3, TimeUnit.DAYS));

    // Add new segment with larger end time but 0 total docs, should not update time boundary
    String segmentEmpty = "segmentEmpty";
    onlineSegments.add(segmentEmpty);
    segmentAssignment.put(segmentEmpty, onlineInstanceStateMap);
    setSegmentZKMetadataWithTotalDocs(rawTableName, segmentEmpty, 6, timeUnit, 0);
    timeBoundaryManager.onAssignmentChange(idealState, externalView, onlineSegments);
    verifyTimeBoundaryInfo(timeBoundaryManager.getTimeBoundaryInfo(), timeUnit.convert(3, TimeUnit.DAYS));

    // Add a new segment with smaller end time should not change the time boundary
    String segment2 = "segment2";
    onlineSegments.add(segment2);
    segmentAssignment.put(segment2, onlineInstanceStateMap);
    setSegmentZKMetadata(rawTableName, segment2, 3, timeUnit);
    timeBoundaryManager.onAssignmentChange(idealState, externalView, onlineSegments);
    verifyTimeBoundaryInfo(timeBoundaryManager.getTimeBoundaryInfo(), timeUnit.convert(3, TimeUnit.DAYS));

    // Remove the segment with largest end time should update the time boundary
    onlineSegments.remove(segment1);
    segmentAssignment.remove(segment1);
    timeBoundaryManager.onAssignmentChange(idealState, externalView, onlineSegments);
    verifyTimeBoundaryInfo(timeBoundaryManager.getTimeBoundaryInfo(), timeUnit.convert(2, TimeUnit.DAYS));

    // Change segment ZK metadata without refreshing should not update the time boundary
    setSegmentZKMetadata(rawTableName, segment2, 5, timeUnit);
    timeBoundaryManager.onAssignmentChange(idealState, externalView, onlineSegments);
    verifyTimeBoundaryInfo(timeBoundaryManager.getTimeBoundaryInfo(), timeUnit.convert(2, TimeUnit.DAYS));

    // Refresh the changed segment should update the time boundary
    timeBoundaryManager.refreshSegment(segment2);
    verifyTimeBoundaryInfo(timeBoundaryManager.getTimeBoundaryInfo(), timeUnit.convert(4, TimeUnit.DAYS));

    // Setting the enforced time boundary in ideal state should update the time boundary
    idealState.getRecord().setSimpleField(CommonConstants.IdealState.HYBRID_TABLE_TIME_BOUNDARY,
        Long.toString(TimeUnit.MILLISECONDS.convert(50, TimeUnit.DAYS)));
    timeBoundaryManager.onAssignmentChange(idealState, externalView, onlineSegments);
    verifyTimeBoundaryInfo(timeBoundaryManager.getTimeBoundaryInfo(), timeUnit.convert(50, TimeUnit.DAYS));

    // Refresh with more recent segment should not update the enforced time boundary
    String segment3 = "segment3";
    setSegmentZKMetadata(rawTableName, segment3, 100, timeUnit);
    onlineSegments.add(segment3);
    segmentAssignment.put(segment3, onlineInstanceStateMap);
    timeBoundaryManager.refreshSegment(segment3);
    verifyTimeBoundaryInfo(timeBoundaryManager.getTimeBoundaryInfo(), timeUnit.convert(50, TimeUnit.DAYS));

    // Unsetting the enforced time boundary should change it to most recent time boundary
    idealState.getRecord().getSimpleFields().remove(CommonConstants.IdealState.HYBRID_TABLE_TIME_BOUNDARY);
    timeBoundaryManager.onAssignmentChange(idealState, externalView, onlineSegments);
    verifyTimeBoundaryInfo(timeBoundaryManager.getTimeBoundaryInfo(), timeUnit.convert(99, TimeUnit.DAYS));
  }

  private void testHourlyPushTable(String rawTableName, TableConfig tableConfig, TimeUnit timeUnit) {
    // NOTE: External view and ideal state are not used in the current implementation.
    ExternalView externalView = Mockito.mock(ExternalView.class);
    IdealState idealState = new IdealState("");

    TimeBoundaryManager timeBoundaryManager =
        new TimeBoundaryManager(tableConfig, _propertyStore, Mockito.mock(BrokerMetrics.class));
    Set<String> onlineSegments = new HashSet<>();
    String segment0 = "segment0";
    onlineSegments.add(segment0);
    setSegmentZKMetadata(rawTableName, segment0, 2, timeUnit);
    timeBoundaryManager.init(idealState, externalView, onlineSegments);
    long expectedTimeValue;
    if (timeUnit == TimeUnit.DAYS) {
      // Time boundary should be endTime - 1 DAY when time unit is DAYS
      expectedTimeValue = timeUnit.convert(1, TimeUnit.DAYS);
    } else {
      // Time boundary should be endTime - 1 HOUR when time unit is other than DAYS
      expectedTimeValue = timeUnit.convert(47, TimeUnit.HOURS);
    }
    verifyTimeBoundaryInfo(timeBoundaryManager.getTimeBoundaryInfo(), expectedTimeValue);

    // Setting the enforced time boundary in ideal state should update the time boundary
    idealState.getRecord().setSimpleField(CommonConstants.IdealState.HYBRID_TABLE_TIME_BOUNDARY,
        Long.toString(TimeUnit.MILLISECONDS.convert(50, TimeUnit.HOURS)));
    timeBoundaryManager.onAssignmentChange(idealState, externalView, onlineSegments);
    verifyTimeBoundaryInfo(timeBoundaryManager.getTimeBoundaryInfo(), timeUnit.convert(50, TimeUnit.HOURS));

    // Unsetting the enforced time boundary should change it back to original time boundary
    idealState.getRecord().getSimpleFields().remove(CommonConstants.IdealState.HYBRID_TABLE_TIME_BOUNDARY);
    timeBoundaryManager.onAssignmentChange(idealState, externalView, onlineSegments);
    verifyTimeBoundaryInfo(timeBoundaryManager.getTimeBoundaryInfo(), expectedTimeValue);
  }

  private TableConfig getTableConfig(String rawTableName, String pushFrequency) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).setTimeColumnName(TIME_COLUMN)
        .setSegmentPushFrequency(pushFrequency).build();
  }

  private void setSchemaTimeFieldSpec(String rawTableName, TimeUnit timeUnit) {
    ZKMetadataProvider.setSchema(_propertyStore, new Schema.SchemaBuilder().setSchemaName(rawTableName)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, timeUnit, TIME_COLUMN), null).build());
  }

  private void setSchemaDateTimeFieldSpec(String rawTableName, TimeUnit timeUnit) {
    ZKMetadataProvider.setSchema(_propertyStore, new Schema.SchemaBuilder().setSchemaName(rawTableName)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:" + timeUnit + ":EPOCH", "1:" + timeUnit).build());
  }

  private void setSegmentZKMetadata(String rawTableName, String segment, int endTimeInDays, TimeUnit timeUnit) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segment);
    segmentZKMetadata.setEndTime(timeUnit.convert(endTimeInDays, TimeUnit.DAYS));
    segmentZKMetadata.setTimeUnit(timeUnit);
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, TableNameBuilder.OFFLINE.tableNameWithType(rawTableName),
        segmentZKMetadata);
  }

  private void setSegmentZKMetadataWithTotalDocs(String rawTableName, String segment, int endTimeInDays,
      TimeUnit timeUnit, long totalDocs) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segment);
    segmentZKMetadata.setEndTime(timeUnit.convert(endTimeInDays, TimeUnit.DAYS));
    segmentZKMetadata.setTimeUnit(timeUnit);
    segmentZKMetadata.setTotalDocs(totalDocs);
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, TableNameBuilder.OFFLINE.tableNameWithType(rawTableName),
        segmentZKMetadata);
  }

  private void verifyTimeBoundaryInfo(TimeBoundaryInfo timeBoundaryInfo, long expectedTimeValue) {
    assertNotNull(timeBoundaryInfo);
    assertEquals(timeBoundaryInfo.getTimeColumn(), TIME_COLUMN);
    assertEquals(Long.parseLong(timeBoundaryInfo.getTimeValue()), expectedTimeValue);
  }
}
