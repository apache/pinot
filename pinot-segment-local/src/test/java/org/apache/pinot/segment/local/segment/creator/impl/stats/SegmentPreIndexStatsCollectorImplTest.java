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

import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class SegmentPreIndexStatsCollectorImplTest {
  private StatsCollectorConfig newConfig(Schema schema, TableConfig tableConfig) {
    return new StatsCollectorConfig(tableConfig, schema, null);
  }

  @Test
  public void testNoDictCollector() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("c1", FieldSpec.DataType.STRING).build();
    TableConfig tableConfig = new TableConfigBuilder(org.apache.pinot.spi.config.table.TableType.OFFLINE)
        .setTableName("t").setNoDictionaryColumns(java.util.List.of("c1")).build();
    SegmentPreIndexStatsCollectorImpl impl = new SegmentPreIndexStatsCollectorImpl(newConfig(schema, tableConfig));
    impl.init();
    assertTrue(impl.getColumnProfileFor("c1") instanceof NoDictColumnStatisticsCollector);
  }

  @Test
  public void testNoDictCollectorDisabled() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("c1", FieldSpec.DataType.STRING).build();
    TableConfig tableConfig = new TableConfigBuilder(org.apache.pinot.spi.config.table.TableType.OFFLINE)
        .setTableName("t").setNoDictionaryColumns(java.util.List.of("c1"))
        .setOptimiseNoDictStatsCollection(false).build();
    SegmentPreIndexStatsCollectorImpl impl = new SegmentPreIndexStatsCollectorImpl(newConfig(schema, tableConfig));
    impl.init();
    assertTrue(impl.getColumnProfileFor("c1") instanceof StringColumnPreIndexStatsCollector);
  }

  @Test
  public void testDictCollector() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("c1", FieldSpec.DataType.STRING).build();
    TableConfig tableConfig = new TableConfigBuilder(org.apache.pinot.spi.config.table.TableType.OFFLINE)
        .setTableName("t").build();
    SegmentPreIndexStatsCollectorImpl impl = new SegmentPreIndexStatsCollectorImpl(newConfig(schema, tableConfig));
    impl.init();
    assertTrue(impl.getColumnProfileFor("c1") instanceof StringColumnPreIndexStatsCollector);
  }
}
