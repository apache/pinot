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
package org.apache.pinot.query.runtime.timeseries.serde;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TimeSeriesBlockSerdeTest {
  private static final TimeBuckets TIME_BUCKETS = TimeBuckets.ofSeconds(1000, Duration.ofSeconds(200), 5);

  @Test
  public void testSerde()
      throws IOException {
    // To test serde of TimeSeriesBlock, we do the following:
    // 1. Serialize the time-series block (say Block-1) to get ByteString-1
    // 2. Deserialize ByteString-1 to get Block-2.
    // 3. Serialize Block-2 to get ByteString-2.
    // 4. Compare ByteString-1 and ByteString-2.
    // 5. Compare values of Block-1 and Block-2.
    List<TimeSeriesBlock> blocks = List.of(buildBlockWithNoTags(), buildBlockWithSingleTag(),
        buildBlockWithMultipleTags());
    for (TimeSeriesBlock block1 : blocks) {
      // Serialize, deserialize and serialize again
      ByteString byteString1 = TimeSeriesBlockSerde.serializeTimeSeriesBlock(block1);
      String serializedBlockString1 = byteString1.toStringUtf8();
      TimeSeriesBlock block2 = TimeSeriesBlockSerde.deserializeTimeSeriesBlock(byteString1.asReadOnlyByteBuffer());
      String serializedBlockString2 = TimeSeriesBlockSerde.serializeTimeSeriesBlock(block2).toStringUtf8();
      // Serialized blocks in both cases should be the same since serialization is deterministic.
      assertEquals(serializedBlockString1, serializedBlockString2);
      // Compare block1 and block2
      compareBlocks(block1, block2);
    }
  }

  /**
   * Compares time series blocks in a way which makes it easy to debug test failures when/if they happen in CI.
   */
  private static void compareBlocks(TimeSeriesBlock block1, TimeSeriesBlock block2) {
    assertEquals(block1.getTimeBuckets(), block2.getTimeBuckets(), "Time buckets are different across blocks");
    assertEquals(block1.getSeriesMap().size(), block2.getSeriesMap().size(), String.format(
        "Different number of series in blocks: %s and %s", block1.getSeriesMap().size(), block2.getSeriesMap().size()));
    assertEquals(block1.getSeriesMap().keySet(), block2.getSeriesMap().keySet(),
        String.format("Series blocks have different keys: %s vs %s",
            block1.getSeriesMap().keySet(), block2.getSeriesMap().keySet()));
    for (long seriesHash : block1.getSeriesMap().keySet()) {
      List<TimeSeries> seriesList1 = block1.getSeriesMap().get(seriesHash);
      List<TimeSeries> seriesList2 = block2.getSeriesMap().get(seriesHash);
      compareTimeSeries(seriesList1, seriesList2);
    }
  }

  private static void compareTimeSeries(List<TimeSeries> series1, List<TimeSeries> series2) {
    assertEquals(series1.size(), series2.size(),
        String.format("Different count of series with the same id: %s vs %s", series1.size(), series2.size()));
    for (int index = 0; index < series1.size(); index++) {
      TimeSeries seriesOne = series1.get(index);
      TimeSeries seriesTwo = series2.get(index);
      assertEquals(seriesOne.getTagNames(), seriesTwo.getTagNames());
      assertEquals(seriesOne.getValues(), seriesTwo .getValues());
    }
  }

  private static TimeSeriesBlock buildBlockWithNoTags() {
    TimeBuckets timeBuckets = TIME_BUCKETS;
    // Single series: []
    List<String> tagNames = Collections.emptyList();
    Object[] seriesValues = new Object[0];
    long seriesHash = TimeSeries.hash(seriesValues);
    Map<Long, List<TimeSeries>> seriesMap = new HashMap<>();
    seriesMap.put(seriesHash, ImmutableList.of(new TimeSeries(Long.toString(seriesHash), null, timeBuckets,
        new Double[]{null, 123.0, 0.0, 1.0}, tagNames, seriesValues)));
    return new TimeSeriesBlock(timeBuckets, seriesMap);
  }

  private static TimeSeriesBlock buildBlockWithSingleTag() {
    TimeBuckets timeBuckets = TIME_BUCKETS;
    // Series are: [cityId=Chicago] and [cityId=San Francisco]
    List<String> tagNames = ImmutableList.of("cityId");
    Object[] seriesOneValues = new Object[]{"Chicago"};
    Object[] seriesTwoValues = new Object[]{"San Francisco"};
    long seriesOneHash = TimeSeries.hash(seriesOneValues);
    long seriesTwoHash = TimeSeries.hash(seriesTwoValues);
    Map<Long, List<TimeSeries>> seriesMap = new HashMap<>();
    seriesMap.put(seriesOneHash, ImmutableList.of(new TimeSeries(Long.toString(seriesOneHash), null, timeBuckets,
        new Double[]{null, 123.0, 0.0, 1.0}, tagNames, seriesOneValues)));
    seriesMap.put(seriesTwoHash, ImmutableList.of(new TimeSeries(Long.toString(seriesTwoHash), null, timeBuckets,
        new Double[]{null, null, null, null}, tagNames, seriesTwoValues)));
    return new TimeSeriesBlock(timeBuckets, seriesMap);
  }

  private static TimeSeriesBlock buildBlockWithMultipleTags() {
    TimeBuckets timeBuckets = TIME_BUCKETS;
    // Series are: [cityId=Chicago, zip=60605] and [cityId=San Francisco, zip=94107]
    List<String> tagNames = ImmutableList.of("cityId", "zip");
    Object[] seriesOneValues = new Object[]{"Chicago", "60605"};
    Object[] seriesTwoValues = new Object[]{"San Francisco", "94107"};
    long seriesOneHash = TimeSeries.hash(seriesOneValues);
    long seriesTwoHash = TimeSeries.hash(seriesTwoValues);
    Map<Long, List<TimeSeries>> seriesMap = new HashMap<>();
    seriesMap.put(seriesOneHash, ImmutableList.of(new TimeSeries(Long.toString(seriesOneHash), null, timeBuckets,
        new Double[]{null, 123.0, Double.NaN, 1.0}, tagNames, seriesOneValues)));
    seriesMap.put(seriesTwoHash, ImmutableList.of(new TimeSeries(Long.toString(seriesTwoHash), null, timeBuckets,
        new Double[]{Double.NaN, -1.0, -1231231.0, 3.14}, tagNames, seriesTwoValues)));
    return new TimeSeriesBlock(timeBuckets, seriesMap);
  }
}
