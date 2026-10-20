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
package org.apache.pinot.plugin.minion.tasks;

import java.util.Set;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.utils.config.SchemaSerDeUtils;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.minion.event.EventObserverFactoryRegistry;
import org.apache.pinot.minion.event.MinionEventObserverFactory;
import org.apache.pinot.minion.executor.PinotTaskExecutorFactory;
import org.apache.pinot.minion.executor.TaskExecutorFactoryRegistry;
import org.apache.pinot.minion.metadata.MinionMetadataAccessor;
import org.apache.pinot.minion.taskfactory.TaskFactoryRegistry;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Verifies the legacy binding and alternate metadata reads without enabling unfenced task execution.
public class BaseTaskExecutorMetadataTest {
  private static final String TABLE = "baseballStats_OFFLINE";
  private static final String SEGMENT = "segment";

  @Test
  public void testLegacyPropertyStoreBinding()
      throws Exception {
    MinionContext context = MinionContext.getInstance();
    ZkHelixPropertyStore<ZNRecord> previousStore = context.getHelixPropertyStore();
    MinionMetadataAccessor previousAccessor = context.getMetadataAccessor();
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> store = mock(ZkHelixPropertyStore.class);
    TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("baseballStats")
        .addSingleValueDimension("playerID", DataType.STRING).build();
    SegmentZKMetadata segment = new SegmentZKMetadata(SEGMENT);
    segment.setCrc(123);
    ZNRecord tableRecord = TableConfigSerDeUtils.toZNRecord(table);
    ZNRecord schemaRecord = SchemaSerDeUtils.toZNRecord(schema);
    ZNRecord segmentRecord = segment.toZNRecord();
    when(store.get("/CONFIGS/TABLE/" + TABLE, null, AccessOption.PERSISTENT)).thenReturn(tableRecord);
    when(store.get("/SCHEMAS/baseballStats", null, AccessOption.PERSISTENT)).thenReturn(schemaRecord);
    when(store.get("/SEGMENTS/" + TABLE + "/" + SEGMENT, null, AccessOption.PERSISTENT)).thenReturn(segmentRecord);
    try {
      context.setHelixPropertyStore(store);
      ReadExecutor executor = new ReadExecutor();
      assertTrue(context.isLegacyTaskExecutionSupported());
      assertEquals(executor.getTableConfig(TABLE).getTableName(), TABLE);
      assertEquals(executor.getSchema(TABLE), schema);
      assertEquals(executor.getSegmentCrc(TABLE, SEGMENT), 123L);
      assertEquals(executor.getSegmentCrc(TABLE, "absent"), -1L);
    } finally {
      context.setHelixPropertyStore(previousStore);
      context.setMetadataAccessor(previousAccessor);
    }
  }

  @Test
  public void testAlternateAccessorDoesNotUseHelixOrEnableTasks() {
    MinionContext context = MinionContext.getInstance();
    ZkHelixPropertyStore<ZNRecord> previousStore = context.getHelixPropertyStore();
    MinionMetadataAccessor previousAccessor = context.getMetadataAccessor();
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> store = mock(ZkHelixPropertyStore.class);
    MinionMetadataAccessor accessor = mock(MinionMetadataAccessor.class);
    TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("baseballStats").build();
    SegmentZKMetadata segment = new SegmentZKMetadata(SEGMENT);
    segment.setCrc(456);
    when(accessor.getTableConfig(TABLE)).thenReturn(table);
    when(accessor.getTableSchema(TABLE)).thenReturn(schema);
    when(accessor.getSegmentMetadata(TABLE, SEGMENT)).thenReturn(segment);
    when(accessor.getSegmentMetadata(TABLE, "unavailable")).thenThrow(new IllegalStateException("No quorum"));
    try {
      context.setHelixPropertyStore(store);
      context.setMetadataAccessor(accessor);
      ReadExecutor executor = new ReadExecutor();
      assertSame(executor.getTableConfig(TABLE), table);
      assertSame(executor.getSchema(TABLE), schema);
      assertEquals(executor.getSegmentCrc(TABLE, SEGMENT), 456L);
      assertEquals(executor.getSegmentCrc(TABLE, "absent"), -1L);
      assertThrows(IllegalStateException.class, () -> executor.getSegmentCrc(TABLE, "unavailable"));
      verifyNoInteractions(store);
      assertFalse(context.isLegacyTaskExecutionSupported());

      PinotTaskExecutorFactory executorFactory = mock(PinotTaskExecutorFactory.class);
      TaskExecutorFactoryRegistry executors = mock(TaskExecutorFactoryRegistry.class);
      when(executors.getAllTaskTypes()).thenReturn(Set.of("test"));
      when(executors.getTaskExecutorFactory("test")).thenReturn(executorFactory);
      MinionEventObserverFactory observerFactory = mock(MinionEventObserverFactory.class);
      EventObserverFactoryRegistry observers = mock(EventObserverFactoryRegistry.class);
      when(observers.getEventObserverFactory("test")).thenReturn(observerFactory);
      TaskFactoryRegistry tasks = new TaskFactoryRegistry(executors, observers);
      TaskCallbackContext taskContext = mock(TaskCallbackContext.class);
      assertThrows(RuntimeException.class,
          () -> tasks.getTaskFactoryRegistry().get("test").createNewTask(taskContext));
      verify(executorFactory, never()).create();
      verify(observerFactory, never()).create();
    } finally {
      context.setHelixPropertyStore(previousStore);
      context.setMetadataAccessor(previousAccessor);
    }
  }

  private static final class ReadExecutor extends BaseTaskExecutor {
    @Override
    protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(
        PinotTaskConfig taskConfig, SegmentConversionResult result) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object executeTask(PinotTaskConfig taskConfig) {
      throw new UnsupportedOperationException();
    }
  }
}
