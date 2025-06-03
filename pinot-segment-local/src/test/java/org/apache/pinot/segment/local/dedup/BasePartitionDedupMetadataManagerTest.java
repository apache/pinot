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
package org.apache.pinot.segment.local.dedup;

import java.io.IOException;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.TableConfig;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class BasePartitionDedupMetadataManagerTest {
  @Test
  public void testPreloadSegments()
      throws IOException {
    String realtimeTableName = "testTable_REALTIME";
    DedupContext dedupContext = mock(DedupContext.class);
    when(dedupContext.isPreloadEnabled()).thenReturn(true);
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(dedupContext.getTableDataManager()).thenReturn(tableDataManager);
    IndexLoadingConfig indexLoadingConfig = mock(IndexLoadingConfig.class);
    when(indexLoadingConfig.getTableConfig()).thenReturn(mock(TableConfig.class));

    try (DummyPartitionDedupMetadataManager dedupMetadataManager = new DummyPartitionDedupMetadataManager(
        realtimeTableName, 0, dedupContext)) {
      assertTrue(dedupMetadataManager.isPreloading());
      dedupMetadataManager.preloadSegments(indexLoadingConfig);
      assertFalse(dedupMetadataManager.isPreloading());
      dedupMetadataManager.stop();
    }
  }

  private static class DummyPartitionDedupMetadataManager extends BasePartitionDedupMetadataManager {

    protected DummyPartitionDedupMetadataManager(String tableNameWithType, int partitionId, DedupContext context) {
      super(tableNameWithType, partitionId, context);
    }

    @Override
    protected void doPreloadSegment(ImmutableSegment segment, Iterator<DedupRecordInfo> dedupRecordInfoIterator) {
    }

    @Override
    protected void doAddOrReplaceSegment(@Nullable IndexSegment oldSegment, IndexSegment newSegment,
        Iterator<DedupRecordInfo> dedupRecordInfoIteratorOfNewSegment) {
    }

    @Override
    protected void doRemoveSegment(IndexSegment segment, Iterator<DedupRecordInfo> dedupRecordInfoIterator) {
    }

    @Override
    protected void doRemoveExpiredPrimaryKeys() {
    }

    @Override
    protected long getNumPrimaryKeys() {
      return 0;
    }
  }
}
