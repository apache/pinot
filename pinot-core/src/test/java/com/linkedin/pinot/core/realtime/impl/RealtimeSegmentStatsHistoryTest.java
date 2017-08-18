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

package com.linkedin.pinot.core.realtime.impl;

import java.io.File;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RealtimeSegmentStatsHistoryTest {
  private static final String STATS_FILE_NAME = RealtimeSegmentStatsHistoryTest.class.getSimpleName() + ".ser";

  private void addSegmentStats(int segmentId, RealtimeSegmentStatsHistory history) {
    RealtimeSegmentStatsHistory.SegmentStats segmentStats = new RealtimeSegmentStatsHistory.SegmentStats();
    segmentStats.setMemUsed(segmentId);
    segmentStats.setNumMinutes(segmentId);
    segmentStats.setNumRowsConsumed(segmentId);
    for (int i = 0; i < 2; i++) {
      RealtimeSegmentStatsHistory.ColumnStats columnStats = new RealtimeSegmentStatsHistory.ColumnStats();
      columnStats.setAvgStringSize(segmentId*100 + i);
      columnStats.setCardinality(segmentId*100 + i);
      segmentStats.setColumnStats(String.valueOf(i), columnStats);
    }
    history.addSegmentStats(segmentStats);
  }

  @Test
  public void serdeTest() throws Exception {
    final String tmpDir = System.getProperty("java.io.tmpdir");
    File serializedFile = new File(tmpDir, STATS_FILE_NAME);
    serializedFile.deleteOnExit();

    int maxNumEntries = RealtimeSegmentStatsHistory.getMaxNumEntries();
    int segmentId = 0;
    {
      RealtimeSegmentStatsHistory history = new RealtimeSegmentStatsHistory();

      history.getEstimatedAvgColSize("1");
      history.getEstimatedCardinality("1");

      // Add one less segment than max.
      for (; segmentId < maxNumEntries - 1; segmentId++) {
        addSegmentStats(segmentId, history);
        Assert.assertEquals(history.getCursor(), segmentId + 1);
        Assert.assertEquals(history.isFull(), false);
      }
      Assert.assertEquals(history.getArraySize(), maxNumEntries);
      history.serializeInto(serializedFile);
      history.getEstimatedAvgColSize("0");
      history.getEstimatedCardinality("0");
    }

    {
      // Set the max to be 2 higher.
      int prevMax = maxNumEntries;
      maxNumEntries += 2;
      RealtimeSegmentStatsHistory.setMaxNumEntries(maxNumEntries);
      // Deserialize
      RealtimeSegmentStatsHistory history = RealtimeSegmentStatsHistory.deserialzeFrom(serializedFile);
      Assert.assertEquals(history.isFull(), false);
      Assert.assertEquals(history.getArraySize(), maxNumEntries);
      Assert.assertEquals(history.getCursor(), prevMax-1);
      // Add one segment
      addSegmentStats(segmentId++, history);
      Assert.assertEquals(history.isFull(), false);
      Assert.assertEquals(history.getArraySize(), maxNumEntries);
      Assert.assertEquals(history.getCursor(), prevMax);
      Assert.assertEquals(history.getCursor(), segmentId);
      history.getEstimatedAvgColSize("0");
      history.getEstimatedCardinality("0");
    // Now add 2 more segments for it to go over.
      addSegmentStats(segmentId++, history);
      Assert.assertEquals(history.isFull(), false);
      Assert.assertEquals(history.getArraySize(), maxNumEntries);
      Assert.assertEquals(history.getCursor(), prevMax+1);
      Assert.assertEquals(history.getCursor(), segmentId);
      history.getEstimatedAvgColSize("0");
      history.getEstimatedCardinality("0");

      addSegmentStats(segmentId++, history);
      Assert.assertEquals(history.isFull(), true);
      Assert.assertEquals(history.getArraySize(), maxNumEntries);
      Assert.assertEquals(history.getCursor(), 0);
      history.getEstimatedAvgColSize("0");
      history.getEstimatedCardinality("0");

      // And then one more to bump the cursor.

      addSegmentStats(segmentId++, history);
      Assert.assertEquals(history.isFull(), true);
      Assert.assertEquals(history.getArraySize(), maxNumEntries);
      Assert.assertEquals(history.getCursor(), 1);
      // Rewrite the history file
      history.serializeInto(serializedFile);
      history.getEstimatedAvgColSize("0");
      history.getEstimatedCardinality("0");
    }

    // Now there should be "maxNumEntries" entries in the file, and the cursor is at 1.
    {
      maxNumEntries -=2;
      RealtimeSegmentStatsHistory.setMaxNumEntries(maxNumEntries);

      RealtimeSegmentStatsHistory history = RealtimeSegmentStatsHistory.deserialzeFrom(serializedFile);
      Assert.assertEquals(history.isFull(), true);
      Assert.assertEquals(history.getArraySize(), maxNumEntries);
      Assert.assertEquals(history.getCursor(), 0);

      history.serializeInto(serializedFile);
      history.getEstimatedAvgColSize("0");
      history.getEstimatedCardinality("0");
    }

    // Now increase it, the cursor should go to the prev max.
    {
      int prevMax = maxNumEntries;
      maxNumEntries += 2;
      RealtimeSegmentStatsHistory.setMaxNumEntries(maxNumEntries);

      RealtimeSegmentStatsHistory history = RealtimeSegmentStatsHistory.deserialzeFrom(serializedFile);
      Assert.assertEquals(history.isFull(), false);
      Assert.assertEquals(history.getArraySize(), maxNumEntries);
      Assert.assertEquals(history.getCursor(), prevMax);
      history.getEstimatedAvgColSize("0");
      history.getEstimatedCardinality("0");
    }
  }
}
