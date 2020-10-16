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
package org.apache.pinot.controller.helix.core.minion.mergestrategy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.core.common.MinionConstants;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TimeBasedMergeStrategyTest {

  @Test
  public void testNoneSegmentGranularity() {
    int numReplicas = 3;
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.MergeRollupTask.SEGMENT_GRANULARITY, "NONE");

    List<SegmentZKMetadata> segmentZKMetadataList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      long start = new DateTime(2020, 1, i + 1, 0, 0, 0, 0, DateTimeZone.UTC).getMillis();
      long end = new DateTime(2020, 1, i + 1, 23, 59, 59, 0, DateTimeZone.UTC).getMillis();
      String segmentName = "segment_" + i;
      segmentZKMetadataList.add(getSegmentZkMetadata(start, end, segmentName));
    }

    TimeBasedMergeStrategy timeBasedMergeStrategy = new TimeBasedMergeStrategy(configs);

    List<List<SegmentZKMetadata>> scheduledSegments =
        timeBasedMergeStrategy.generateMergeTaskCandidates(segmentZKMetadataList, 5);

    Assert.assertEquals(scheduledSegments.size(), 2);
    Assert.assertEquals(scheduledSegments.get(0).size(), 5);
    Assert.assertEquals(scheduledSegments.get(1).size(), 5);

    scheduledSegments = timeBasedMergeStrategy.generateMergeTaskCandidates(segmentZKMetadataList, 2);
    Assert.assertEquals(scheduledSegments.size(), 5);
    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(scheduledSegments.get(i).size(), 2);
    }
  }

  @Test
  public void testDaysSegmentGranularity() {
    List<SegmentZKMetadata> segmentZKMetadataList = new ArrayList<>();

    // Generate 6 segments/day, for 5 days
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 6; j++) {
        long start = new DateTime(2020, 1, i + 1, 0, 0, 0, 0, DateTimeZone.UTC).getMillis();
        long end = new DateTime(2020, 1, i + 1, 23, 59, 59, 0, DateTimeZone.UTC).getMillis();
        String segmentName = "segment_" + i + "_" + j;
        segmentZKMetadataList.add(getSegmentZkMetadata(start, end, segmentName));
      }
    }

    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.MergeRollupTask.SEGMENT_GRANULARITY, "DAYS");
    TimeBasedMergeStrategy timeBasedMergeStrategy = new TimeBasedMergeStrategy(configs);

    List<List<SegmentZKMetadata>> scheduledSegments =
        timeBasedMergeStrategy.generateMergeTaskCandidates(segmentZKMetadataList, 6);

    Assert.assertEquals(scheduledSegments.size(), 5);
    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(scheduledSegments.get(i).size(), 6);
      int dayOfMonth = new DateTime(scheduledSegments.get(i).get(0).getEndTime()).getDayOfMonth();
      for (int j = 1; j < 6; j++) {
        Assert.assertEquals(
            new DateTime(scheduledSegments.get(i).get(j).getStartTime(), DateTimeZone.UTC).getDayOfMonth(), dayOfMonth);
        Assert
            .assertEquals(new DateTime(scheduledSegments.get(i).get(j).getEndTime(), DateTimeZone.UTC).getDayOfMonth(),
                dayOfMonth);
      }
    }

    scheduledSegments = timeBasedMergeStrategy.generateMergeTaskCandidates(segmentZKMetadataList, 3);

    Assert.assertEquals(scheduledSegments.size(), 10);
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(scheduledSegments.get(i).size(), 3);

      int dayOfMonth = new DateTime(scheduledSegments.get(i).get(0).getEndTime()).getDayOfMonth();
      for (int j = 1; j < 3; j++) {
        Assert.assertEquals(
            new DateTime(scheduledSegments.get(i).get(j).getStartTime(), DateTimeZone.UTC).getDayOfMonth(), dayOfMonth);
        Assert
            .assertEquals(new DateTime(scheduledSegments.get(i).get(j).getEndTime(), DateTimeZone.UTC).getDayOfMonth(),
                dayOfMonth);
      }
    }
  }

  @Test
  public void testMonthsSegmentGranularity() {
    List<SegmentZKMetadata> segmentZKMetadataList = new ArrayList<>();
    for (int month = 6; month < 11; month++) {
      for (int day = 1; day < 31; day++) {
        long start = new DateTime(2020, month, day, 0, 0, 0, 0, DateTimeZone.UTC).getMillis();
        long end = new DateTime(2020, month, day, 23, 59, 59, 0, DateTimeZone.UTC).getMillis();
        String segmentName = "segment_" + month + "_" + day;
        segmentZKMetadataList.add(getSegmentZkMetadata(start, end, segmentName));
      }
    }

    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.MergeRollupTask.SEGMENT_GRANULARITY, "MONTHS");
    TimeBasedMergeStrategy timeBasedMergeStrategy = new TimeBasedMergeStrategy(configs);

    List<List<SegmentZKMetadata>> scheduledSegments =
        timeBasedMergeStrategy.generateMergeTaskCandidates(segmentZKMetadataList, 31);

    Assert.assertEquals(scheduledSegments.size(), 5);
    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(scheduledSegments.get(i).size(), 30);
      int monthOfYear = new DateTime(scheduledSegments.get(i).get(0).getEndTime()).getMonthOfYear();
      for (int j = 1; j < 30; j++) {
        Assert.assertEquals(new DateTime(scheduledSegments.get(i).get(j).getStartTime()).getMonthOfYear(), monthOfYear);
        Assert.assertEquals(new DateTime(scheduledSegments.get(i).get(j).getEndTime()).getMonthOfYear(), monthOfYear);
      }
    }
  }

  private SegmentZKMetadata getSegmentZkMetadata(long startTime, long endTime, String segmentName) {
    SegmentZKMetadata segmentZKMetadata = mock(SegmentZKMetadata.class);
    when(segmentZKMetadata.getStartTime()).thenReturn(startTime);
    when(segmentZKMetadata.getEndTime()).thenReturn(endTime);
    when(segmentZKMetadata.getSegmentName()).thenReturn(segmentName);
    when(segmentZKMetadata.getTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);
    return segmentZKMetadata;
  }
}
