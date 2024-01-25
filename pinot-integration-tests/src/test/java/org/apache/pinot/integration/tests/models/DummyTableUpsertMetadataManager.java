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
package org.apache.pinot.integration.tests.models;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import org.apache.helix.HelixManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.upsert.BasePartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.BaseTableUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.RecordInfo;
import org.apache.pinot.segment.local.upsert.UpsertContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class DummyTableUpsertMetadataManager extends BaseTableUpsertMetadataManager {

  private TableConfig _tableConfig;
  private Schema _schema;

  public DummyTableUpsertMetadataManager() {
    super();
  }

  @Override
  public void init(TableConfig tableConfig, Schema schema, TableDataManager tableDataManager, HelixManager helixManager,
      @org.jetbrains.annotations.Nullable ExecutorService segmentPreloadExecutor) {
    super.init(tableConfig, schema, tableDataManager, helixManager, segmentPreloadExecutor);
    _tableConfig = tableConfig;
    _schema = schema;
  }

  @Override
  public PartitionUpsertMetadataManager getOrCreatePartitionManager(int partitionId) {
    UpsertContext context = new UpsertContext.Builder().setTableConfig(_tableConfig).setSchema(_schema)
        .setPrimaryKeyColumns(_schema.getPrimaryKeyColumns())
        .setComparisonColumns(Collections.singletonList(_tableConfig.getValidationConfig().getTimeColumnName()))
        .setHashFunction(HashFunction.NONE).setTableIndexDir(new File("/tmp/tableIndexDirDummy")).build();

    return new DummyPartitionUpsertMetadataManager("dummy", partitionId, context);
  }

  @Override
  public void stop() {
  }

  @Override
  public void close()
      throws IOException {
  }

  class DummyPartitionUpsertMetadataManager extends BasePartitionUpsertMetadataManager {
    public DummyPartitionUpsertMetadataManager(String tableNameWithType, int partitionId, UpsertContext context) {
      super(tableNameWithType, partitionId, context);
    }

    @Override
    protected long getNumPrimaryKeys() {
      return 0;
    }

    @Override
    protected void addOrReplaceSegment(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
        @org.jetbrains.annotations.Nullable ThreadSafeMutableRoaringBitmap queryableDocIds,
        Iterator<RecordInfo> recordInfoIterator, @org.jetbrains.annotations.Nullable IndexSegment oldSegment,
        @org.jetbrains.annotations.Nullable MutableRoaringBitmap validDocIdsForOldSegment) {
    }

    @Override
    protected boolean doAddRecord(MutableSegment segment, RecordInfo recordInfo) {
      return false;
    }

    @Override
    protected void removeSegment(IndexSegment segment, MutableRoaringBitmap validDocIds) {
    }

    @Override
    protected GenericRow doUpdateRecord(GenericRow record, RecordInfo recordInfo) {
      return null;
    }

    @Override
    protected void doRemoveExpiredPrimaryKeys() {
    }
  }
}
