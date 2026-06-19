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
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class StatsCollectorUtilFieldSpecTest {

  @Test
  public void testCreateIntCollectorFromFieldSpec() {
    DimensionFieldSpec spec = new DimensionFieldSpec("c", DataType.INT, true);
    AbstractColumnStatisticsCollector collector = StatsCollectorUtil.createStatsCollector(spec, null);
    collector.collect(5);
    collector.collect(5);
    collector.collect(2);
    collector.seal();
    assertEquals(collector.getCardinality(), 2);
    assertEquals(collector.getMinValue(), 2);
    assertEquals(collector.getMaxValue(), 5);
  }

  @Test
  public void testCreateBigDecimalCollectorFromFieldSpec() {
    DimensionFieldSpec spec = new DimensionFieldSpec("c", DataType.BIG_DECIMAL, true);
    AbstractColumnStatisticsCollector collector = StatsCollectorUtil.createStatsCollector(spec, null);
    collector.collect(new BigDecimal("1.0"));
    collector.collect(new BigDecimal("1.00"));
    collector.seal();
    // 1.0 and 1.00 are distinct under equals (BigDecimalColumnPreIndexStatsCollector uses ObjectOpenHashSet).
    assertEquals(collector.getCardinality(), 2);
  }
}
