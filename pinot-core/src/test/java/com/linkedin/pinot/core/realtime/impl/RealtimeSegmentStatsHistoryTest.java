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
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RealtimeSegmentStatsHistoryTest {
  private static final String STATS_FILE_NAME = RealtimeSegmentStatsHistoryTest.class.getSimpleName() + ".ser";
  private static final String COL1 = "col1";
  private static final String COL2 = "col2";

  private void addSegmentStats(int segmentId, RealtimeSegmentStatsHistory history) {
    RealtimeSegmentStatsHistory.SegmentStats segmentStats = new RealtimeSegmentStatsHistory.SegmentStats();
    segmentStats.setMemUsedBytes(segmentId);
    segmentStats.setNumSeconds(segmentId);
    segmentStats.setNumRowsConsumed(segmentId);
    for (int i = 0; i < 2; i++) {
      RealtimeSegmentStatsHistory.ColumnStats columnStats = new RealtimeSegmentStatsHistory.ColumnStats();
      columnStats.setAvgColumnSize(segmentId*100 + i);
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
    FileUtils.deleteQuietly(serializedFile);

    int maxNumEntries = RealtimeSegmentStatsHistory.getMaxNumEntries();
    int segmentId = 0;
    {
      RealtimeSegmentStatsHistory history = RealtimeSegmentStatsHistory.deserialzeFrom(serializedFile);
      // We should have got an empty one here.

      history.getEstimatedAvgColSize("1");
      history.getEstimatedCardinality("1");

      // Add one less segment than max.
      for (; segmentId < maxNumEntries - 1; segmentId++) {
        addSegmentStats(segmentId, history);
        Assert.assertEquals(history.getCursor(), segmentId + 1);
        Assert.assertEquals(history.isFull(), false);
      }
      Assert.assertEquals(history.getArraySize(), maxNumEntries);
      history.save();
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
      history.save();
      // Add one segment
      addSegmentStats(segmentId++, history);
      Assert.assertEquals(history.isFull(), false);
      Assert.assertEquals(history.getArraySize(), maxNumEntries);
      Assert.assertEquals(history.getCursor(), prevMax);
      Assert.assertEquals(history.getCursor(), segmentId);
      history.getEstimatedAvgColSize("0");
      history.getEstimatedCardinality("0");
      history.save();
    // Now add 2 more segments for it to go over.
      addSegmentStats(segmentId++, history);
      Assert.assertEquals(history.isFull(), false);
      Assert.assertEquals(history.getArraySize(), maxNumEntries);
      Assert.assertEquals(history.getCursor(), prevMax+1);
      Assert.assertEquals(history.getCursor(), segmentId);
      history.getEstimatedAvgColSize("0");
      history.getEstimatedCardinality("0");
      history.save();

      addSegmentStats(segmentId++, history);
      Assert.assertEquals(history.isFull(), true);
      Assert.assertEquals(history.getArraySize(), maxNumEntries);
      Assert.assertEquals(history.getCursor(), 0);
      history.getEstimatedAvgColSize("0");
      history.getEstimatedCardinality("0");
      history.save();

      // And then one more to bump the cursor.

      addSegmentStats(segmentId++, history);
      Assert.assertEquals(history.isFull(), true);
      Assert.assertEquals(history.getArraySize(), maxNumEntries);
      Assert.assertEquals(history.getCursor(), 1);
      // Rewrite the history file
      history.save();
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

      history.save();
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

  @Test
  public void testMultiThreadedUse() throws Exception {
    final int numThreads = 8;
    final int numIterations = 10;
    final long avgSleepTimeMs = 300;
    Thread[] threads = new Thread[numThreads];
    final String tmpDir = System.getProperty("java.io.tmpdir");
    File serializedFile = new File(tmpDir, STATS_FILE_NAME);
    FileUtils.deleteQuietly(serializedFile);
    serializedFile.deleteOnExit();
    RealtimeSegmentStatsHistory statsHistory = RealtimeSegmentStatsHistory.deserialzeFrom(serializedFile);

    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread(new StatsUpdater(statsHistory, numIterations, avgSleepTimeMs));
      threads[i].start();
    }

    for (int i = 0; i < numThreads; i++) {
      threads[i].join();
    }

    System.out.println(statsHistory.getEstimatedCardinality(COL1));
    System.out.println(statsHistory.getEstimatedCardinality(COL2));
    System.out.println(statsHistory.getEstimatedAvgColSize(COL1));
    System.out.println(statsHistory.getEstimatedAvgColSize(COL2));

    FileUtils.deleteQuietly(serializedFile);
  }

  private static class StatsUpdater implements Runnable {
    private final RealtimeSegmentStatsHistory _statsHistory;
    private final int _numIterations;
    private final long _avgSleepTimeMs;
    private final int _sleepVariationMs;
    private final Random _random = new Random();

    private static final int MAX_AVGLEN = 200;
    private static final int MAX_CARDINALITY = 50000;

    private StatsUpdater(RealtimeSegmentStatsHistory statsHistory, int numInterations, long avgSleepTimeMs){
      _statsHistory = statsHistory;
      _numIterations = numInterations;
      _avgSleepTimeMs = avgSleepTimeMs;
      _sleepVariationMs = (int)_avgSleepTimeMs/10;
    }

    @Override
    public void run() {
      for (int i = 0; i < _numIterations; i++) {
        try {
          Thread.sleep(_avgSleepTimeMs - _sleepVariationMs + _random.nextInt(2 * _sleepVariationMs));
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        RealtimeSegmentStatsHistory.SegmentStats segmentStats = new RealtimeSegmentStatsHistory.SegmentStats();
        RealtimeSegmentStatsHistory.ColumnStats columnStats;

        columnStats = new RealtimeSegmentStatsHistory.ColumnStats();
        columnStats.setAvgColumnSize(_random.nextInt(MAX_AVGLEN));
        columnStats.setCardinality(_random.nextInt(MAX_CARDINALITY));
        segmentStats.setColumnStats(COL1, columnStats);
        System.out.println("Setting column stats for " + COL1 + ":" + columnStats.toString());

        columnStats = new RealtimeSegmentStatsHistory.ColumnStats();
        columnStats.setAvgColumnSize(_random.nextInt(MAX_AVGLEN));
        columnStats.setCardinality(_random.nextInt(MAX_CARDINALITY));
        segmentStats.setColumnStats(COL2, columnStats);
        System.out.println("Setting column stats for " + COL2 + ":" + columnStats.toString());

        _statsHistory.addSegmentStats(segmentStats);
        _statsHistory.save();
      }
    }
  }
}
