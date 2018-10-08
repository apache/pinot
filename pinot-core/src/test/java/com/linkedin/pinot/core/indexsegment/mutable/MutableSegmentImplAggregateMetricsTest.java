/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.indexsegment.mutable;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MutableSegmentImplAggregateMetricsTest {
  private static final String DIMENSION_1 = "dim1";
  private static final String DIMENSION_2 = "dim2";
  private static final String METRIC = "metric";
  private static final String KEY_SEPARATOR = "\t\t";
  private static final int NUM_ROWS = 10001;

  private MutableSegmentImpl _mutableSegmentImpl;

  @BeforeClass
  public void setUp() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addSingleValueDimension(DIMENSION_1, FieldSpec.DataType.INT)
        .addSingleValueDimension(DIMENSION_2, FieldSpec.DataType.STRING)
        .addMetric(METRIC, FieldSpec.DataType.LONG)
        .build();
    _mutableSegmentImpl = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema,
        new HashSet<>(Arrays.asList(DIMENSION_1, METRIC)), Collections.singleton(DIMENSION_1), true);
  }

  @Test
  public void testAggregateMetrics() {
    String[] stringValues = new String[10];
    for (int i = 0; i < stringValues.length; i++) {
      stringValues[i] = RandomStringUtils.random(10);
    }

    Map<String, Long> expectedValues = new HashMap<>();
    Random random = new Random();
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putField(DIMENSION_1, random.nextInt(10));
      row.putField(DIMENSION_2, stringValues[random.nextInt(stringValues.length)]);
      // Generate random int to prevent overflow
      long metricValue = random.nextInt();
      row.putField(METRIC, metricValue);

      _mutableSegmentImpl.index(row);

      // Update expected values
      String key = buildKey(row);
      expectedValues.put(key, expectedValues.getOrDefault(key, 0L) + metricValue);
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
    }
  }

  private String buildKey(GenericRow row) {
    return String.valueOf(row.getValue(DIMENSION_1)) + KEY_SEPARATOR + row.getValue(DIMENSION_2);
  }

  @AfterClass
  public void tearDown() {
    _mutableSegmentImpl.destroy();
  }
}