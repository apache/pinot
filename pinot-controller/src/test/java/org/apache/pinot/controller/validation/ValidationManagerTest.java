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
package org.apache.pinot.controller.validation;

import java.util.ArrayList;
import java.util.List;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for the ValidationManagers.
 */
public class ValidationManagerTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();
  private static final String TEST_TABLE_NAME = "validationTable";
  private static final String OFFLINE_TEST_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(TEST_TABLE_NAME);
  private static final String TEST_SEGMENT_NAME = "testSegment";
  private static final int EXPECTED_VERSION = -1;

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();
    // Create a schema
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName(TEST_TABLE_NAME)
        .addSingleValueDimension("dim1", FieldSpec.DataType.STRING)
        .addMetric("metric1", FieldSpec.DataType.INT)
        .build();
    TEST_INSTANCE.getHelixResourceManager().addSchema(schema, true, true);
    // Create a table
    TableConfig offlineTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TEST_TABLE_NAME).setNumReplicas(2).build();
    TEST_INSTANCE.getHelixResourceManager().addTable(offlineTableConfig);
  }

  @Test
  public void testPushTimePersistence() {
    SegmentMetadata segmentMetadata = SegmentMetadataMockUtils.mockSegmentMetadata(TEST_TABLE_NAME, TEST_SEGMENT_NAME);

    TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_TEST_TABLE_NAME, segmentMetadata, "downloadUrl");
    SegmentZKMetadata segmentZKMetadata =
        TEST_INSTANCE.getHelixResourceManager().getSegmentZKMetadata(OFFLINE_TEST_TABLE_NAME, TEST_SEGMENT_NAME);
    assertNotNull(segmentZKMetadata);
    long pushTime = segmentZKMetadata.getPushTime();
    // Check that the segment has been pushed in the last 30 seconds
    assertTrue(System.currentTimeMillis() - pushTime < 30_000);
    // Check that there is no refresh time
    assertEquals(segmentZKMetadata.getRefreshTime(), Long.MIN_VALUE);

    // Refresh the segment
    // NOTE: In order to send the refresh message, the segment need to be in the ExternalView
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(TEST_TABLE_NAME);
    TestUtils.waitForCondition(aVoid -> {
      ExternalView externalView =
          TEST_INSTANCE.getHelixAdmin().getResourceExternalView(TEST_INSTANCE.getHelixClusterName(), offlineTableName);
      return externalView != null && externalView.getPartitionSet().contains(TEST_SEGMENT_NAME);
    }, 30_000L, "Failed to find the segment in the ExternalView");
    Mockito.when(segmentMetadata.getCrc()).thenReturn(Long.toString(System.nanoTime()));
    TEST_INSTANCE.getHelixResourceManager()
        .refreshSegment(offlineTableName, segmentMetadata, segmentZKMetadata, EXPECTED_VERSION, "downloadUrl");

    segmentZKMetadata =
        TEST_INSTANCE.getHelixResourceManager().getSegmentZKMetadata(OFFLINE_TEST_TABLE_NAME, TEST_SEGMENT_NAME);
    assertNotNull(segmentZKMetadata);
    // Check that the segment still has the same push time
    assertEquals(segmentZKMetadata.getPushTime(), pushTime);
    // Check that the refresh time is in the last 30 seconds
    assertTrue(System.currentTimeMillis() - segmentZKMetadata.getRefreshTime() < 30_000L);
  }

  @Test
  public void testTotalDocumentCountRealTime() {
    // Create some dummy LLC segments (both committed and uploaded)
    List<SegmentZKMetadata> segmentsZKMetadata = new ArrayList<>();
    String segmentName5 = new LLCSegmentName(TEST_TABLE_NAME, 1, 0, 1000).getSegmentName();
    String segmentName6 = new LLCSegmentName(TEST_TABLE_NAME, 2, 27, 10000).getSegmentName();
    segmentsZKMetadata.add(SegmentMetadataMockUtils.mockSegmentZKMetadata(segmentName5, 10));
    segmentsZKMetadata.add(SegmentMetadataMockUtils.mockSegmentZKMetadata(segmentName6, 5));
    segmentsZKMetadata.add(SegmentMetadataMockUtils.mockSegmentZKMetadata(TEST_SEGMENT_NAME, 15));
    assertEquals(RealtimeSegmentValidationManager.computeTotalDocumentCount(segmentsZKMetadata), 30);
  }

  @Test
  public void testComputeNumMissingSegments() {
    Interval jan1st = new Interval(new DateTime(2015, 1, 1, 0, 0, 0), new DateTime(2015, 1, 1, 23, 59, 59));
    Interval jan2nd = new Interval(new DateTime(2015, 1, 2, 0, 0, 0), new DateTime(2015, 1, 2, 23, 59, 59));
    Interval jan3rd = new Interval(new DateTime(2015, 1, 3, 0, 0, 0), new DateTime(2015, 1, 3, 23, 59, 59));
    Interval jan4th = new Interval(new DateTime(2015, 1, 4, 0, 0, 0), new DateTime(2015, 1, 4, 23, 59, 59));
    Interval jan5th = new Interval(new DateTime(2015, 1, 5, 0, 0, 0), new DateTime(2015, 1, 5, 23, 59, 59));

    ArrayList<Interval> jan1st2nd3rd = new ArrayList<>();
    jan1st2nd3rd.add(jan1st);
    jan1st2nd3rd.add(jan2nd);
    jan1st2nd3rd.add(jan3rd);
    assertEquals(OfflineSegmentIntervalChecker.computeNumMissingSegments(jan1st2nd3rd, Duration.standardDays(1)), 0);

    ArrayList<Interval> jan1st2nd3rd5th = new ArrayList<>(jan1st2nd3rd);
    jan1st2nd3rd5th.add(jan5th);
    assertEquals(OfflineSegmentIntervalChecker.computeNumMissingSegments(jan1st2nd3rd5th, Duration.standardDays(1)), 1);

    // Should also work if the intervals are in random order
    ArrayList<Interval> jan5th2nd1st = new ArrayList<>();
    jan5th2nd1st.add(jan5th);
    jan5th2nd1st.add(jan2nd);
    jan5th2nd1st.add(jan1st);
    assertEquals(OfflineSegmentIntervalChecker.computeNumMissingSegments(jan5th2nd1st, Duration.standardDays(1)), 2);

    // Should also work if the intervals are of different sizes
    Interval jan1stAnd2nd = new Interval(new DateTime(2015, 1, 1, 0, 0, 0), new DateTime(2015, 1, 2, 23, 59, 59));
    ArrayList<Interval> jan1st2nd4th5th = new ArrayList<Interval>();
    jan1st2nd4th5th.add(jan1stAnd2nd);
    jan1st2nd4th5th.add(jan4th);
    jan1st2nd4th5th.add(jan5th);
    assertEquals(OfflineSegmentIntervalChecker.computeNumMissingSegments(jan1st2nd4th5th, Duration.standardDays(1)), 1);
  }

  @AfterClass
  public void tearDown() {
    TEST_INSTANCE.cleanup();
  }
}
