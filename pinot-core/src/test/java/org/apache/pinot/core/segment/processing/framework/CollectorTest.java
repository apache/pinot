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
package org.apache.pinot.core.segment.processing.framework;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.pinot.core.segment.processing.collector.Collector;
import org.apache.pinot.core.segment.processing.collector.CollectorConfig;
import org.apache.pinot.core.segment.processing.collector.CollectorFactory;
import org.apache.pinot.core.segment.processing.collector.ConcatCollector;
import org.apache.pinot.core.segment.processing.collector.RollupCollector;
import org.apache.pinot.core.segment.processing.collector.ValueAggregatorFactory;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests for {@link Collector}
 */
public class CollectorTest {

  private final List<String> uniqueD = new ArrayList<>();
  private static final Random RANDOM = new Random(10);

  @BeforeClass
  public void before() {
    for (int i = 0; i < 20; i++) {
      uniqueD.add(RandomStringUtils.random(5));
    }
  }

  @Test
  public void testConcatCollector() {
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName("testSchema").addSingleValueDimension("d", FieldSpec.DataType.STRING)
            .build();
    CollectorConfig collectorConfig = new CollectorConfig.Builder().build();
    Collector collector = CollectorFactory.getCollector(collectorConfig, schema);
    assertEquals(collector.getClass(), ConcatCollector.class);

    for (int i = 0; i < 100; i++) {
      GenericRow row = new GenericRow();
      row.putValue("d", uniqueD.get(RandomUtils.nextInt(uniqueD.size())));
      collector.collect(row);
    }
    assertEquals(collector.size(), 100);
    Iterator<GenericRow> iterator = collector.iterator();
    while (iterator.hasNext()) {
      GenericRow next = iterator.next();
      assertTrue(uniqueD.contains(String.valueOf(next.getValue("d"))));
    }
    collector.reset();
    assertEquals(collector.size(), 0);
  }

  @Test
  public void testRollupCollectorWithNoMetrics() {
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName("testSchema").addSingleValueDimension("d", FieldSpec.DataType.STRING)
            .build();
    CollectorConfig collectorConfig =
        new CollectorConfig.Builder().setCollectorType(CollectorFactory.CollectorType.ROLLUP).build();
    Collector collector = CollectorFactory.getCollector(collectorConfig, schema);
    assertEquals(collector.getClass(), RollupCollector.class);

    Set<String> usedValues = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      GenericRow row = new GenericRow();
      String value = uniqueD.get(RANDOM.nextInt(uniqueD.size()));
      row.putValue("d", value);
      collector.collect(row);
      usedValues.add(value);
    }
    assertEquals(collector.size(), usedValues.size());
    Iterator<GenericRow> iterator = collector.iterator();
    while (iterator.hasNext()) {
      GenericRow next = iterator.next();
      assertTrue(uniqueD.contains(String.valueOf(next.getValue("d"))));
    }
    collector.reset();
    assertEquals(collector.size(), 0);
  }

  @Test
  public void testRollupCollectorWithDefaultAggregations() {
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName("testSchema").addSingleValueDimension("d", FieldSpec.DataType.STRING)
            .addMetric("m1", FieldSpec.DataType.INT). addMetric("m2", FieldSpec.DataType.LONG).build();
    CollectorConfig collectorConfig =
        new CollectorConfig.Builder().setCollectorType(CollectorFactory.CollectorType.ROLLUP).build();
    Collector collector = CollectorFactory.getCollector(collectorConfig, schema);

    Map<String, Integer> m1Map = new HashMap<>();
    Map<String, Long> m2Map = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      GenericRow row = new GenericRow();
      String value = uniqueD.get(RANDOM.nextInt(uniqueD.size()));
      row.putValue("d", value);
      int m1 = RandomUtils.nextInt(10);
      row.putValue("m1", m1);
      long m2 = RANDOM.nextLong();
      row.putValue("m2", m2);

      if (m1Map.containsKey(value)) {
        m1Map.put(value, m1Map.get(value) + m1);
        m2Map.put(value, m2Map.get(value) + m2);
      } else {
        m1Map.put(value, m1);
        m2Map.put(value, m2);
      }
      collector.collect(row);
    }
    assertEquals(collector.size(), m1Map.size());
    Iterator<GenericRow> iterator = collector.iterator();
    while (iterator.hasNext()) {
      GenericRow next = iterator.next();
      String d = String.valueOf(next.getValue("d"));
      assertTrue(uniqueD.contains(d));
      assertEquals(next.getValue("m1"), m1Map.get(d));
      assertEquals(next.getValue("m2"), m2Map.get(d));
    }
    collector.reset();
    assertEquals(collector.size(), 0);
  }

  @Test
  public void testRollupCollectorWithMinMaxAggregations() {
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName("testSchema").addSingleValueDimension("d", FieldSpec.DataType.STRING)
            .addMetric("m1", FieldSpec.DataType.INT). addMetric("m2", FieldSpec.DataType.LONG).build();
    Map<String, ValueAggregatorFactory.ValueAggregatorType> valueAggregatorMap = new HashMap<>();
    valueAggregatorMap.put("m1", ValueAggregatorFactory.ValueAggregatorType.MAX);
    valueAggregatorMap.put("m2", ValueAggregatorFactory.ValueAggregatorType.MIN);
    CollectorConfig collectorConfig =
        new CollectorConfig.Builder().setCollectorType(CollectorFactory.CollectorType.ROLLUP).setAggregatorTypeMap(valueAggregatorMap).build();
    Collector collector = CollectorFactory.getCollector(collectorConfig, schema);

    Map<String, Integer> m1Map = new HashMap<>();
    Map<String, Long> m2Map = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      GenericRow row = new GenericRow();
      String value = uniqueD.get(RANDOM.nextInt(uniqueD.size()));
      row.putValue("d", value);
      int m1 = RandomUtils.nextInt(10);
      row.putValue("m1", m1);
      long m2 = RANDOM.nextLong();
      row.putValue("m2", m2);

      if (m1Map.containsKey(value)) {
        m1Map.put(value, Math.max(m1Map.get(value), m1));
        m2Map.put(value, Math.min(m2Map.get(value), m2));
      } else {
        m1Map.put(value, m1);
        m2Map.put(value, m2);
      }
      collector.collect(row);
    }
    assertEquals(collector.size(), m1Map.size());
    Iterator<GenericRow> iterator = collector.iterator();
    while (iterator.hasNext()) {
      GenericRow next = iterator.next();
      String d = String.valueOf(next.getValue("d"));
      assertTrue(uniqueD.contains(d));
      assertEquals(next.getValue("m1"), m1Map.get(d));
      assertEquals(next.getValue("m2"), m2Map.get(d));
    }
    collector.reset();
    assertEquals(collector.size(), 0);
  }
}
