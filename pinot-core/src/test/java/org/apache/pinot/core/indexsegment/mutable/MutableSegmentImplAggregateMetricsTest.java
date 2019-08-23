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
package org.apache.pinot.core.indexsegment.mutable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.realtime.stream.StreamMessageMetadata;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MutableSegmentImplAggregateMetricsTest {
  private static final String DIMENSION_1 = "dim1";
  private static final String DIMENSION_2 = "dim2";
  private static final String METRIC = "metric";
  private static final String METRIC_2 = "metric2";
  private static final String TIME_COLUMN = "time";
  private static final String KEY_SEPARATOR = "\t\t";
  private static final int NUM_ROWS = 10001;

  private MutableSegmentImpl _mutableSegmentImpl;

  @BeforeClass
  public void setUp() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addSingleValueDimension(DIMENSION_1, FieldSpec.DataType.INT)
            .addSingleValueDimension(DIMENSION_2, FieldSpec.DataType.STRING)
            .addMetric(METRIC, FieldSpec.DataType.LONG)
            .addMetric(METRIC_2, FieldSpec.DataType.FLOAT)
            .addTime(TIME_COLUMN, TimeUnit.DAYS, FieldSpec.DataType.INT)
            .build();
    _mutableSegmentImpl = MutableSegmentImplTestUtils
        .createMutableSegmentImpl(schema, new HashSet<>(Arrays.asList(DIMENSION_1, METRIC, METRIC_2)),
            new HashSet<>(Arrays.asList(DIMENSION_1)),
            Collections.singleton(DIMENSION_1), true);
  }

  @Test
  public void testAggregateMetrics() {
    String[] stringValues = new String[10];
    Float[] floatValues = new Float[10];
    Random random = new Random();
    for (int i = 0; i < stringValues.length; i++) {
      stringValues[i] = RandomStringUtils.random(10);
      floatValues[i] = random.nextFloat() * 10f;
    }

    Map<String, Long> expectedValues = new HashMap<>();
    Map<String, Float> expectedValuesFloat = new HashMap<>();
    StreamMessageMetadata defaultMetadata = new StreamMessageMetadata(System.currentTimeMillis());
    for (int i = 0; i < NUM_ROWS; i++) {
      int daysSinceEpoch = random.nextInt(10);
      GenericRow row = new GenericRow();
      row.putField(DIMENSION_1, random.nextInt(10));
      row.putField(DIMENSION_2, stringValues[random.nextInt(stringValues.length)]);
      row.putField(TIME_COLUMN, daysSinceEpoch);
      // Generate random int to prevent overflow
      long metricValue = random.nextInt();
      row.putField(METRIC, metricValue);
      float metricValueFloat = floatValues[random.nextInt(floatValues.length)];
      row.putField(METRIC_2, metricValueFloat);

      _mutableSegmentImpl.index(row, defaultMetadata);

      // Update expected values
      String key = buildKey(row);
      expectedValues.put(key, expectedValues.getOrDefault(key, 0L) + metricValue);
      expectedValuesFloat.put(key, expectedValuesFloat.getOrDefault(key, 0f) + metricValueFloat);
    }

    int numDocsIndexed = _mutableSegmentImpl.getNumDocsIndexed();
    Assert.assertEquals(numDocsIndexed, expectedValues.size());

    // Assert that aggregation happened.
    Assert.assertTrue(numDocsIndexed < NUM_ROWS);

    GenericRow reuse = new GenericRow();
    for (int docId = 0; docId < numDocsIndexed; docId++) {
      GenericRow row = _mutableSegmentImpl.getRecord(docId, reuse);
      String key = buildKey(row);
      Assert.assertEquals(row.getValue(METRIC), expectedValues.get(key));
      Assert.assertEquals(row.getValue(METRIC_2), expectedValuesFloat.get(key));
    }
  }

  private String buildKey(GenericRow row) {
    return String.valueOf(row.getValue(DIMENSION_1)) + KEY_SEPARATOR +
        row.getValue(DIMENSION_2) + KEY_SEPARATOR + row.getValue(TIME_COLUMN);
  }

  @AfterClass
  public void tearDown() {
    _mutableSegmentImpl.destroy();
  }
}
