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

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.MmapMemoryManager;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Unit test for {@link RealtimeSegmentImpl}
 */
public class RealtimeSegmentImplTest {

  private static final String DIMENSION_1 = "dim1";
  private static final String DIMENSION_2 = "dim2";
  private static final String METRIC_COLUMN = "metric";
  private static final String SEGMENT_NAME = "realtimeSegmentImplTest";
  private static final String _keySeparator = "\t\t";
  private static final int NUM_ROWS = 10001;

  private RealtimeSegmentConfig.Builder _configBuilder;
  private RealtimeSegmentImpl _realtimeSegment;
  private Random _random;

  @BeforeClass
  public void setup() {
    PinotDataBufferMemoryManager memoryManager = new MmapMemoryManager("/tmp", SEGMENT_NAME);
    _configBuilder = new RealtimeSegmentConfig.Builder();
    _random = new Random(System.nanoTime());

    RealtimeSegmentStatsHistory statsHistory = mock(RealtimeSegmentStatsHistory.class);
    when(statsHistory.getEstimatedAvgColSize(any(String.class))).thenReturn(32);
    when(statsHistory.getEstimatedCardinality(any(String.class))).thenReturn(200);

    _configBuilder.setCapacity(1000000)
        .setMemoryManager(memoryManager)
        .setNoDictionaryColumns(new HashSet<>(Collections.singletonList(METRIC_COLUMN)))
        .setOffHeap(true)
        .setSchema(buildSchema())
        .setSegmentName(SEGMENT_NAME)
        .setStatsHistory(statsHistory)
        .setInvertedIndexColumns(new HashSet<>(Collections.singletonList(DIMENSION_1)))
        .setRealtimeSegmentZKMetadata(new RealtimeSegmentZKMetadata())
        .setAggregateMetrics(true);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _realtimeSegment.destroy();
  }

  /**
   * This test generates a {@link RealtimeSegmentImpl} object and indexes rows with
   * duplicate dimension values in it, and ensures that metrics are aggregated correctly.
   *
   */
  @Test
  public void test() {
    _realtimeSegment = new RealtimeSegmentImpl(_configBuilder.build());
    String[] stringValues = new String[10]; // 10 unique strings.
    for (int i = 0; i < stringValues.length; i++) {
      stringValues[i] = RandomStringUtils.random(10);
    }

    Map<String, Integer> expectedMap = new LinkedHashMap<>();
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();

      row.putField(DIMENSION_1, (long) _random.nextInt(10)); // 10 unique values
      row.putField(DIMENSION_2, stringValues[_random.nextInt(stringValues.length)]); // 10 unique values

      int metricValue = _random.nextInt();
      row.putField(METRIC_COLUMN, metricValue);
      _realtimeSegment.index(row);

      // Collect expected results.
      String key = buildKey(row);
      Integer sum = expectedMap.get(key);
      if (sum == null) {
        expectedMap.put(key, metricValue);
      } else {
        expectedMap.put(key, sum + metricValue);
      }
    }

    int numDocsIndexed = _realtimeSegment.getNumDocsIndexed();
    Assert.assertEquals(numDocsIndexed, expectedMap.size());

    GenericRow row = new GenericRow();
    for (int docId = 0; docId < numDocsIndexed; docId++) {
      _realtimeSegment.getRecord(docId, row);
      String key = buildKey(row);
      Assert.assertEquals(row.getValue(METRIC_COLUMN), expectedMap.get(key));
    }
  }

  /**
   * Helper method to build key containing dimension column values.
   *
   * @param row Generic row to be used for building key.
   * @return String key for the given row.
   */
  private String buildKey(GenericRow row) {
    return String.valueOf(row.getValue(DIMENSION_1)) +
        _keySeparator +
        row.getValue(DIMENSION_2);
  }

  /**
   * Helper method to build schema for test segment, containing two dimension columns and one metric column.
   *
   * @return Schema for test segment.
   */
  private Schema buildSchema() {
    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(DIMENSION_1, FieldSpec.DataType.LONG, true));
    schema.addField(new DimensionFieldSpec(DIMENSION_2, FieldSpec.DataType.STRING, true));
    schema.addField(new MetricFieldSpec(METRIC_COLUMN, FieldSpec.DataType.INT));
    return schema;
  }
}
