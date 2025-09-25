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
package org.apache.pinot.segment.local.segment.creator.impl.stats;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class MapColumnPreIndexStatsCollectorTest {

  private static StatsCollectorConfig newConfig(boolean optimiseNoDictStatsCollection) {
    TableConfig tableConfig = new TableConfigBuilder(org.apache.pinot.spi.config.table.TableType.OFFLINE)
        .setTableName("testTable")
        .setOptimiseNoDictStatsCollection(optimiseNoDictStatsCollection)
        .setSegmentPartitionConfig(new SegmentPartitionConfig(
            Collections.singletonMap("col", new ColumnPartitionConfig("murmur", 4))))
        .setNoDictionaryColumns(java.util.List.of("col"))
        .build();

    Map<String, FieldSpec> children = new HashMap<>();
    children.put("key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true));
    // Values of heterogeneous types will drive per-key collectors of different types
    children.put("value", new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true));
    Schema schema = new Schema();
    schema.addField(new ComplexFieldSpec("col", FieldSpec.DataType.MAP, true, children));
    return new StatsCollectorConfig(tableConfig, schema,
        tableConfig.getIndexingConfig().getSegmentPartitionConfig());
  }

  @Test
  public void testKeyCollectorsUseNoDictWhenEnabledAndMatchOutputs() {
    // Prepare mixed-type values across keys
    Map<String, Object> r1 = new HashMap<>();
    r1.put("kStr", "alpha");
    r1.put("kInt", 3);
    r1.put("kLong", 7L);
    r1.put("kFloat", 1.5f);
    r1.put("kDouble", 2.25d);
    r1.put("kBigDec", new BigDecimal("10.01"));

    Map<String, Object> r2 = new HashMap<>();
    r2.put("kStr", "beta");
    r2.put("kInt", 3); // duplicate for cardinality checks
    r2.put("kLong", 2L);
    r2.put("kFloat", 1.5f); // duplicate for cardinality checks
    r2.put("kDouble", 0.75d);
    r2.put("kBigDec", new BigDecimal("10.01")); // duplicate for cardinality checks

    Map<String, Object> r3 = new HashMap<>();
    r3.put("kStr", "alpha");
    r3.put("kInt", 3);
    r3.put("kLong", 10L);
    r3.put("kFloat", 3.5f);
    r3.put("kDouble", 0.75d);
    r3.put("kBigDec", new BigDecimal("5.25"));

    StatsCollectorConfig cfgNoDict = newConfig(true);
    StatsCollectorConfig cfgDict = newConfig(false);

    MapColumnPreIndexStatsCollector mapNoDict = new MapColumnPreIndexStatsCollector("col", cfgNoDict);
    MapColumnPreIndexStatsCollector mapDict = new MapColumnPreIndexStatsCollector("col", cfgDict);

    mapNoDict.collect(r1);
    mapNoDict.collect(r2);
    mapNoDict.collect(r3);
    mapNoDict.seal();

    mapDict.collect(r1);
    mapDict.collect(r2);
    mapDict.collect(r3);
    mapDict.seal();

    // Compare public outputs on the map collectors
    assertEquals(mapNoDict.getCardinality(), mapDict.getCardinality());
    assertEquals(mapNoDict.getMinValue(), mapDict.getMinValue());
    assertEquals(mapNoDict.getMaxValue(), mapDict.getMaxValue());
    assertEquals(mapNoDict.getTotalNumberOfEntries(), mapDict.getTotalNumberOfEntries());
    assertEquals(mapNoDict.getMaxNumberOfMultiValues(), mapDict.getMaxNumberOfMultiValues());
    assertEquals(mapNoDict.isSorted(), mapDict.isSorted());
    assertEquals(mapNoDict.getLengthOfShortestElement(), mapDict.getLengthOfShortestElement());
    assertEquals(mapNoDict.getLengthOfLargestElement(), mapDict.getLengthOfLargestElement());
    assertEquals(mapNoDict.getMaxRowLengthInBytes(), mapDict.getMaxRowLengthInBytes());

    // Partition metadata
    PartitionFunction pfNoDict = mapNoDict.getPartitionFunction();
    PartitionFunction pfDict = mapDict.getPartitionFunction();
    if (pfNoDict == null || pfDict == null) {
      assertNull(pfNoDict);
      assertNull(pfDict);
    } else {
      assertEquals(pfNoDict.getName(), pfDict.getName());
      assertEquals(mapNoDict.getNumPartitions(), mapDict.getNumPartitions());
      Set<Integer> partsNoDict = mapNoDict.getPartitions();
      Set<Integer> partsDict = mapDict.getPartitions();
      assertEquals(partsNoDict, partsDict);
    }

    // Compare per-key collectors exposed via getKeyStatistics
    for (String key : mapNoDict.getAllKeyFrequencies().keySet()) {
      AbstractColumnStatisticsCollector keyNoDict = mapNoDict.getKeyStatistics(key);
      AbstractColumnStatisticsCollector keyDict = mapDict.getKeyStatistics(key);
      assertNotNull(keyNoDict, "missing key in no-dict collector: " + key);
      assertNotNull(keyDict, "missing key in dict collector: " + key);

      assertEquals(keyNoDict.getCardinality(), keyDict.getCardinality(), "cardinality mismatch for key " + key);
      assertEquals(keyNoDict.getMinValue(), keyDict.getMinValue(), "min mismatch for key " + key);
      assertEquals(keyNoDict.getMaxValue(), keyDict.getMaxValue(), "max mismatch for key " + key);
      assertEquals(keyNoDict.getTotalNumberOfEntries(), keyDict.getTotalNumberOfEntries(),
          "entries mismatch for key " + key);
      assertEquals(keyNoDict.getMaxNumberOfMultiValues(), keyDict.getMaxNumberOfMultiValues(),
          "max MV mismatch for key " + key);
      assertEquals(keyNoDict.isSorted(), keyDict.isSorted(), "sorted mismatch for key " + key);
    }
  }
}
