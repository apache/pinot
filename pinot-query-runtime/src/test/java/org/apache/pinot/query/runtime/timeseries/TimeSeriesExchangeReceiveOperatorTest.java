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
package org.apache.pinot.query.runtime.timeseries;

import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.SimpleTimeSeriesBuilderFactory;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TimeSeriesExchangeReceiveOperatorTest {
  private static final int NUM_SERVERS_QUERIED = 3;
  private static final AggInfo SUM_AGG_INFO = new AggInfo("SUM", false, Collections.emptyMap());
  private static final TimeBuckets TIME_BUCKETS = TimeBuckets.ofSeconds(1000, Duration.ofSeconds(200), 4);
  private static final List<String> TAG_NAMES = ImmutableList.of("city", "zip");
  private static final Object[] CHICAGO_SERIES_VALUES = new Object[]{"Chicago", "60605"};
  private static final Object[] SF_SERIES_VALUES = new Object[]{"San Francisco", "94107"};
  private static final Long CHICAGO_SERIES_HASH = TimeSeries.hash(CHICAGO_SERIES_VALUES);
  private static final Long SF_SERIES_HASH = TimeSeries.hash(SF_SERIES_VALUES);
  private static final SimpleTimeSeriesBuilderFactory SERIES_BUILDER_FACTORY = new SimpleTimeSeriesBuilderFactory();

  @Test
  public void testGetNextBlockWithAggregation() {
    // Setup test
    long deadlineMs = Long.MAX_VALUE;
    ArrayBlockingQueue<Object> blockingQueue = new ArrayBlockingQueue<>(NUM_SERVERS_QUERIED);
    blockingQueue.addAll(generateTimeSeriesBlocks());
    TimeSeriesExchangeReceiveOperator operator = new TimeSeriesExchangeReceiveOperator(blockingQueue, deadlineMs,
        NUM_SERVERS_QUERIED, SUM_AGG_INFO, SERIES_BUILDER_FACTORY);
    // Run test
    TimeSeriesBlock block = operator.nextBlock();
    // Validate results
    assertEquals(block.getSeriesMap().size(), 2);
    assertTrue(block.getSeriesMap().containsKey(CHICAGO_SERIES_HASH), "Chicago series not present in received block");
    assertTrue(block.getSeriesMap().containsKey(SF_SERIES_HASH), "SF series not present in received block");
    assertEquals(block.getSeriesMap().get(CHICAGO_SERIES_HASH).size(), 1, "Expected 1 series for Chicago");
    assertEquals(block.getSeriesMap().get(SF_SERIES_HASH).size(), 1, "Expected 1 series for SF");
    // Ensure Chicago had series addition performed
    Double[] chicagoSeriesValues = block.getSeriesMap().get(CHICAGO_SERIES_HASH).get(0).getDoubleValues();
    assertEquals(chicagoSeriesValues, new Double[]{20.0, 20.0, 20.0, 20.0});
    // Ensure SF had input series unmodified
    Double[] sanFranciscoSeriesValues = block.getSeriesMap().get(SF_SERIES_HASH).get(0).getDoubleValues();
    assertEquals(sanFranciscoSeriesValues, new Double[]{10.0, 10.0, 10.0, 10.0});
  }

  @Test
  public void testGetNextBlockNoAggregation() {
    // Setup test
    long deadlineMs = Long.MAX_VALUE;
    ArrayBlockingQueue<Object> blockingQueue = new ArrayBlockingQueue<>(NUM_SERVERS_QUERIED);
    blockingQueue.addAll(generateTimeSeriesBlocks());
    TimeSeriesExchangeReceiveOperator operator = new TimeSeriesExchangeReceiveOperator(blockingQueue, deadlineMs,
        NUM_SERVERS_QUERIED, null, SERIES_BUILDER_FACTORY);
    // Run test
    TimeSeriesBlock block = operator.nextBlock();
    // Validate results
    assertEquals(block.getSeriesMap().size(), 2);
    assertTrue(block.getSeriesMap().containsKey(CHICAGO_SERIES_HASH), "Chicago series not present in received block");
    assertTrue(block.getSeriesMap().containsKey(SF_SERIES_HASH), "SF series not present in received block");
    assertEquals(block.getSeriesMap().get(CHICAGO_SERIES_HASH).size(), 2, "Expected 2 series for Chicago");
    assertEquals(block.getSeriesMap().get(SF_SERIES_HASH).size(), 1, "Expected 1 series for SF");
    // Ensure Chicago has unmodified series values
    Double[] firstChicagoSeriesValues = block.getSeriesMap().get(CHICAGO_SERIES_HASH).get(0).getDoubleValues();
    Double[] secondChicagoSeriesValues = block.getSeriesMap().get(CHICAGO_SERIES_HASH).get(1).getDoubleValues();
    assertEquals(firstChicagoSeriesValues, new Double[]{10.0, 10.0, 10.0, 10.0});
    assertEquals(secondChicagoSeriesValues, new Double[]{10.0, 10.0, 10.0, 10.0});
    // Ensure SF has input unmodified series values
    Double[] sanFranciscoSeriesValues = block.getSeriesMap().get(SF_SERIES_HASH).get(0).getDoubleValues();
    assertEquals(sanFranciscoSeriesValues, new Double[]{10.0, 10.0, 10.0, 10.0});
  }

  @Test
  public void testGetNextBlockFailure() {
    // Setup test
    long deadlineMs = Long.MAX_VALUE;
    ArrayBlockingQueue<Object> blockingQueue = new ArrayBlockingQueue<>(NUM_SERVERS_QUERIED);
    blockingQueue.add(new TimeoutException("Test error"));
    // Run test with aggregation
    try {
      TimeSeriesExchangeReceiveOperator operator = new TimeSeriesExchangeReceiveOperator(blockingQueue, deadlineMs,
          NUM_SERVERS_QUERIED, SUM_AGG_INFO, SERIES_BUILDER_FACTORY);
      TimeSeriesBlock block = operator.nextBlock();
      fail();
    } catch (Throwable t) {
      assertEquals(t.getMessage(), "Test error");
    }
    blockingQueue.add(new TimeoutException("Test error"));
    try {
      TimeSeriesExchangeReceiveOperator operator = new TimeSeriesExchangeReceiveOperator(blockingQueue, deadlineMs,
          NUM_SERVERS_QUERIED, null, SERIES_BUILDER_FACTORY);
      TimeSeriesBlock block = operator.nextBlock();
      fail();
    } catch (Throwable t) {
      assertEquals(t.getMessage(), "Test error");
    }
  }

  private List<TimeSeriesBlock> generateTimeSeriesBlocks() {
    List<TimeSeriesBlock> seriesBlocks = new ArrayList<>();
    {
      Map<Long, List<TimeSeries>> seriesMap = new HashMap<>();
      seriesMap.put(CHICAGO_SERIES_HASH, ImmutableList.of(createChicagoSeries(new Double[]{10.0, 10.0, 10.0, 10.0})));
      seriesMap.put(SF_SERIES_HASH, ImmutableList.of(createSanFranciscoSeries(new Double[]{10.0, 10.0, 10.0, 10.0})));
      seriesBlocks.add(new TimeSeriesBlock(TIME_BUCKETS, seriesMap));
    }
    {
      Map<Long, List<TimeSeries>> seriesMap = new HashMap<>();
      seriesMap.put(CHICAGO_SERIES_HASH, ImmutableList.of(createChicagoSeries(new Double[]{10.0, 10.0, 10.0, 10.0})));
      seriesBlocks.add(new TimeSeriesBlock(TIME_BUCKETS, seriesMap));
    }
    {
      Map<Long, List<TimeSeries>> seriesMap = new HashMap<>();
      seriesBlocks.add(new TimeSeriesBlock(TIME_BUCKETS, seriesMap));
    }
    // Shuffle the output to test multiple scenarios over time
    Collections.shuffle(seriesBlocks);
    return seriesBlocks;
  }

  private TimeSeries createChicagoSeries(Double[] values) {
    return new TimeSeries(CHICAGO_SERIES_HASH.toString(), null, TIME_BUCKETS, values, TAG_NAMES, CHICAGO_SERIES_VALUES);
  }

  private TimeSeries createSanFranciscoSeries(Double[] values) {
    return new TimeSeries(SF_SERIES_HASH.toString(), null, TIME_BUCKETS, values, TAG_NAMES, SF_SERIES_VALUES);
  }
}
