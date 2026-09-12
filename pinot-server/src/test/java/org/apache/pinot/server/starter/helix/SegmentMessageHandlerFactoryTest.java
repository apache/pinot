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
package org.apache.pinot.server.starter.helix;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.messages.TableDeletionMessage;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.InOrder;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Covers the OPEN_STRUCT gauge keys resolved on table deletion. `OPEN_STRUCT_LAST_SEGMENT_KEY_DOC_COUNT` is a
/// keyed gauge, so the generic `removeTableGauge(table, gauge)` sweep cannot reach it; the handler
/// re-derives its keys from the table config instead.
public class SegmentMessageHandlerFactoryTest {

  private static Schema schema() {
    return new Schema.SchemaBuilder().setSchemaName("t")
        .addField(new ComplexFieldSpec("metrics", DataType.OPEN_STRUCT, true, Map.of()))
        .addField(new DimensionFieldSpec("plain", DataType.STRING, true))
        .build();
  }

  private static TableConfig tableConfig(List<FieldConfig> fieldConfigs) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("t").setFieldConfigList(fieldConfigs).build();
  }

  private static FieldConfig openStructField(String column, String denseKeysJson)
      throws Exception {
    JsonNode indexes = JsonUtils.stringToJsonNode("{\"open_struct\": {\"denseKeys\": " + denseKeysJson + "}}");
    return new FieldConfig.Builder(column).withIndexes(indexes).build();
  }

  @Test
  public void testResolvesOneMetricKeyPerConfiguredDenseKey()
      throws Exception {
    TableConfig tableConfig = tableConfig(List.of(openStructField("metrics", "[\"clicks\", \"views\"]")));

    List<String> metricKeys = SegmentMessageHandlerFactory.openStructMetricKeys(tableConfig, schema());

    assertEquals(metricKeys.size(), 2);
    assertTrue(metricKeys.contains("metrics$clicks"), metricKeys.toString());
    assertTrue(metricKeys.contains("metrics$views"), metricKeys.toString());
  }

  /// The emitted gauge key is escaped for JMX, so the key used to remove it has to be escaped the
  /// same way or the removal silently misses.
  @Test
  public void testMetricKeysAreEscapedLikeTheEmittedGauge()
      throws Exception {
    TableConfig tableConfig = tableConfig(List.of(openStructField("metrics", "[\"promo\\\"code\"]")));

    List<String> metricKeys = SegmentMessageHandlerFactory.openStructMetricKeys(tableConfig, schema());

    assertEquals(metricKeys, List.of("metrics$promo%22code"));
  }

  @Test
  public void testNoKeysWithoutConfiguredDenseKeys()
      throws Exception {
    // No configured denseKeys → nothing recoverable for removal. When perKeyMetricsEnabled is on,
    // discovered-key gauges are emitted but not reachable here; they survive until server restart.
    TableConfig tableConfig = tableConfig(List.of(openStructField("metrics", "[]")));

    assertTrue(SegmentMessageHandlerFactory.openStructMetricKeys(tableConfig, schema()).isEmpty());
  }

  @Test
  public void testNoKeysForTableWithoutOpenStructColumns() {
    Schema plainSchema = new Schema.SchemaBuilder().setSchemaName("t")
        .addField(new DimensionFieldSpec("plain", DataType.STRING, true))
        .build();

    assertTrue(SegmentMessageHandlerFactory.openStructMetricKeys(tableConfig(List.of()), plainSchema).isEmpty());
  }

  private static final String TABLE = "t_OFFLINE";

  private static void runDeletion(InstanceDataManager instanceDataManager, ServerMetrics serverMetrics)
      throws Exception {
    new SegmentMessageHandlerFactory(instanceDataManager, serverMetrics).createHandler(
        new TableDeletionMessage(TABLE), null).handleMessage();
  }

  private static InstanceDataManager instanceDataManagerReturning(Pair<TableConfig, Schema> cachedConfigAndSchema) {
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getCachedTableConfigAndSchema()).thenReturn(cachedConfigAndSchema);
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager(TABLE)).thenReturn(tableDataManager);
    return instanceDataManager;
  }

  /// The reason this change exists: the generic sweep in the handler composes only the unkeyed
  /// `<gauge>.<table>`, so without this targeted call the per-key gauge outlives the table. Asserts on
  /// the keyed `removeTableGauge` overload rather than on a registry, because reading back a gauge
  /// written by `setOrUpdateTableGauge` needs a metrics factory that pinot-server does not register.
  @Test
  public void testHandlerRemovesTheEmittedPerKeyGauge()
      throws Exception {
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    TableConfig tableConfig = tableConfig(List.of(openStructField("metrics", "[\"clicks\"]")));

    runDeletion(instanceDataManagerReturning(Pair.of(tableConfig, schema())), serverMetrics);

    verify(serverMetrics).removeTableGauge(TABLE, "metrics$clicks", ServerGauge.OPEN_STRUCT_LAST_SEGMENT_KEY_DOC_COUNT);
  }

  /// The keys have to be read off the table data manager before deleteTable discards it.
  @Test
  public void testKeysAreResolvedBeforeTheTableIsDeleted()
      throws Exception {
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    TableConfig tableConfig = tableConfig(List.of(openStructField("metrics", "[\"clicks\"]")));
    InstanceDataManager instanceDataManager = instanceDataManagerReturning(Pair.of(tableConfig, schema()));

    runDeletion(instanceDataManager, serverMetrics);

    InOrder inOrder = inOrder(instanceDataManager, serverMetrics);
    inOrder.verify(instanceDataManager).getTableDataManager(TABLE);
    inOrder.verify(instanceDataManager).deleteTable(eq(TABLE), anyLong());
    inOrder.verify(serverMetrics)
        .removeTableGauge(TABLE, "metrics$clicks", ServerGauge.OPEN_STRUCT_LAST_SEGMENT_KEY_DOC_COUNT);
  }

  /// Metric bookkeeping must never block table deletion.
  @Test
  public void testDeletionProceedsWhenKeysCannotBeResolved()
      throws Exception {
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager(TABLE)).thenThrow(new IllegalStateException("boom"));

    runDeletion(instanceDataManager, mock(ServerMetrics.class));

    verify(instanceDataManager, times(1)).deleteTable(eq(TABLE), anyLong());
  }

  @Test
  public void testDeletionProceedsWithoutATableDataManager()
      throws Exception {
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager(TABLE)).thenReturn(null);

    runDeletion(instanceDataManager, mock(ServerMetrics.class));

    verify(instanceDataManager, times(1)).deleteTable(eq(TABLE), anyLong());
  }
}
