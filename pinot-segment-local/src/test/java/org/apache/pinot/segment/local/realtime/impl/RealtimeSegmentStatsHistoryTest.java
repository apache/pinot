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
package org.apache.pinot.segment.local.realtime.impl;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.util.TestUtils;
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
    segmentStats.setNumRowsIndexed(segmentId);
    for (int i = 0; i < 2; i++) {
      RealtimeSegmentStatsHistory.ColumnStats columnStats = new RealtimeSegmentStatsHistory.ColumnStats();
      columnStats.setAvgColumnSize(segmentId * 100 + i);
      columnStats.setCardinality(segmentId * 100 + i);
      segmentStats.setColumnStats(String.valueOf(i), columnStats);
    }
    history.addSegmentStats(segmentStats);
  }

  @Test
  public void zeroStatTest()
      throws Exception {
    final String tmpDir = System.getProperty("java.io.tmpdir");
    File serializedFile = new File(tmpDir, STATS_FILE_NAME);
    serializedFile.deleteOnExit();
    FileUtils.deleteQuietly(serializedFile);
    String columName = "col1";

    {
      RealtimeSegmentStatsHistory history = RealtimeSegmentStatsHistory.deserialzeFrom(serializedFile);
      RealtimeSegmentStatsHistory.SegmentStats segmentStats = new RealtimeSegmentStatsHistory.SegmentStats();
      segmentStats.setMemUsedBytes(100);
      segmentStats.setNumSeconds(101);
      segmentStats.setNumRowsConsumed(102);
      segmentStats.setNumRowsIndexed(103);

      RealtimeSegmentStatsHistory.ColumnStats columnStats = new RealtimeSegmentStatsHistory.ColumnStats();
      columnStats.setAvgColumnSize(0);
      columnStats.setCardinality(0);
      segmentStats.setColumnStats(columName, columnStats);

      history.addSegmentStats(segmentStats);
    }
    {
      RealtimeSegmentStatsHistory history = RealtimeSegmentStatsHistory.deserialzeFrom(serializedFile);
      Assert.assertTrue(history.getEstimatedAvgColSize(columName) > 0);
      Assert.assertTrue(history.getEstimatedCardinality(columName) > 0);
      Assert.assertEquals(history.getEstimatedRowsToIndex(), 103);
    }
  }

  @Test
  public void serdeTest()
      throws Exception {
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
      Assert.assertEquals(history.getCursor(), prevMax - 1);
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
      Assert.assertEquals(history.getCursor(), prevMax + 1);
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
      history.getEstimatedAvgColSize("0");
      history.getEstimatedCardinality("0");
    }

    // Now there should be "maxNumEntries" entries in the file, and the cursor is at 1.
    {
      maxNumEntries -= 2;
      RealtimeSegmentStatsHistory.setMaxNumEntries(maxNumEntries);

      RealtimeSegmentStatsHistory history = RealtimeSegmentStatsHistory.deserialzeFrom(serializedFile);
      Assert.assertEquals(history.isFull(), true);
      Assert.assertEquals(history.getArraySize(), maxNumEntries);
      Assert.assertEquals(history.getCursor(), 0);
      // Force a save by calling addSegmentStats
      addSegmentStats(segmentId++, history);

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
    // Now add a new column
    boolean savedIsFull;
    int savedCursor;
    {
      RealtimeSegmentStatsHistory history = RealtimeSegmentStatsHistory.deserialzeFrom(serializedFile);
      Assert.assertEquals(history.getEstimatedAvgColSize("new"), RealtimeSegmentStatsHistory.getDefaultEstAvgColSize());
      Assert
          .assertEquals(history.getEstimatedCardinality("new"), RealtimeSegmentStatsHistory.getDefaultEstCardinality());
      savedIsFull = history.isFull();
      savedCursor = history.getCursor();
    }
    {
      RealtimeSegmentStatsHistory history = RealtimeSegmentStatsHistory.deserialzeFrom(serializedFile);
      Assert.assertEquals(history.isFull(), savedIsFull);
      Assert.assertEquals(history.getCursor(), savedCursor);
    }
  }

  @Test
  public void testMultiThreadedUse()
      throws Exception {
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

    FileUtils.deleteQuietly(serializedFile);
  }

  // This test attempts to ensure that future modifications to RealtimeSegmentStatsHistory does not prevent the software
  // from reading data serialized by earlier versions. The serialized data has one segment, with 2 columns --
  // "v1col1" and
  // "v1col2".
  @Test
  public void testVersion1()
      throws Exception {
    final String fileName = "realtime-segment-stats-history-v1.ser";
    File v1StatsFile = new File(
        TestUtils.getFileFromResourceUrl(RealtimeSegmentStatsHistoryTest.class.getClassLoader().getResource("data")),
        fileName);
    RealtimeSegmentStatsHistory statsHistory = RealtimeSegmentStatsHistory.deserialzeFrom(v1StatsFile);
    RealtimeSegmentStatsHistory.SegmentStats segmentStats = statsHistory.getSegmentStatsAt(0);
    RealtimeSegmentStatsHistory.ColumnStats columnStats;

    columnStats = segmentStats.getColumnStats("v1col1");
    Assert.assertEquals(columnStats.getCardinality(), 100);
    Assert.assertEquals(columnStats.getAvgColumnSize(), 200);
    columnStats = segmentStats.getColumnStats("v1col2");
    Assert.assertEquals(columnStats.getCardinality(), 300);
    Assert.assertEquals(columnStats.getAvgColumnSize(), 400);

    Assert.assertEquals(segmentStats.getNumRowsConsumed(), 500);
    Assert.assertEquals(segmentStats.getNumRowsIndexed(), 0); // Input file does not have this field.
    Assert.assertEquals(segmentStats.getMemUsedBytes(), 600);
    Assert.assertEquals(segmentStats.getNumSeconds(), 700);
  }

  @Test
  public void testLatestConsumedMemory()
      throws IOException, ClassNotFoundException {
    final String tmpDir = System.getProperty("java.io.tmpdir");
    File serializedFile = new File(tmpDir, STATS_FILE_NAME);
    serializedFile.deleteOnExit();
    FileUtils.deleteQuietly(serializedFile);
    long[] memoryValues = {100, 100, 200, 400, 450, 600};

    RealtimeSegmentStatsHistory history = RealtimeSegmentStatsHistory.deserialzeFrom(serializedFile);
    Assert.assertEquals(history.getLatestSegmentMemoryConsumed(), -1);
    RealtimeSegmentStatsHistory.SegmentStats segmentStats = null;

    for (int i = 0; i < memoryValues.length; i++) {
      segmentStats = new RealtimeSegmentStatsHistory.SegmentStats();
      segmentStats.setMemUsedBytes(memoryValues[i]);
      history.addSegmentStats(segmentStats);
    }

    long expectedMemUsed = memoryValues[memoryValues.length - 1];
    Assert.assertEquals(history.getLatestSegmentMemoryConsumed(), expectedMemUsed);
  }

  private static class StatsUpdater implements Runnable {
    private final RealtimeSegmentStatsHistory _statsHistory;
    private final int _numIterations;
    private final long _avgSleepTimeMs;
    private final int _sleepVariationMs;
    private final Random _random = new Random();

    private static final int MAX_AVGLEN = 200;
    private static final int MAX_CARDINALITY = 50000;

    private StatsUpdater(RealtimeSegmentStatsHistory statsHistory, int numInterations, long avgSleepTimeMs) {
      _statsHistory = statsHistory;
      _numIterations = numInterations;
      _avgSleepTimeMs = avgSleepTimeMs;
      _sleepVariationMs = (int) _avgSleepTimeMs / 10;
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

        columnStats = new RealtimeSegmentStatsHistory.ColumnStats();
        columnStats.setAvgColumnSize(_random.nextInt(MAX_AVGLEN));
        columnStats.setCardinality(_random.nextInt(MAX_CARDINALITY));
        segmentStats.setColumnStats(COL2, columnStats);

        _statsHistory.addSegmentStats(segmentStats);
      }
    }
  }
}
