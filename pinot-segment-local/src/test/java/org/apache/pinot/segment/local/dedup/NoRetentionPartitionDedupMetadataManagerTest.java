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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;


public class NoRetentionPartitionDedupMetadataManagerTest {
  private TestMetadataManager _metadataManager;

  @BeforeMethod
  public void setUp() {
    DedupContext.Builder dedupContextBuider = new DedupContext.Builder();
    dedupContextBuider
        .setTableConfig(mock(TableConfig.class))
        .setSchema(mock(Schema.class))
        .setPrimaryKeyColumns(List.of("primaryKeyColumn"))
        .setHashFunction(HashFunction.NONE)
        .setServerMetrics(mock(ServerMetrics.class));
    DedupContext dedupContext = dedupContextBuider.build();
    _metadataManager = new TestMetadataManager("tableName", 0, dedupContext);
  }

  @Test
  public void verifyAddRemoveSegment() {
    HashFunction hashFunction = HashFunction.NONE;
    Map<Object, IndexSegment> recordLocationMap = _metadataManager._primaryKeyToSegmentMap;

    // Add the first segment
    List<PrimaryKey> pkList1 = new ArrayList<>();
    pkList1.add(DedupTestUtils.getPrimaryKey(0));
    pkList1.add(DedupTestUtils.getPrimaryKey(1));
    pkList1.add(DedupTestUtils.getPrimaryKey(2));
    pkList1.add(DedupTestUtils.getPrimaryKey(0));
    pkList1.add(DedupTestUtils.getPrimaryKey(1));
    pkList1.add(DedupTestUtils.getPrimaryKey(0));
    _metadataManager._primaryKeyIterator = pkList1.iterator();
    ImmutableSegmentImpl segment1 = DedupTestUtils.mockSegment(1, 6);
    _metadataManager.addSegment(segment1);
    checkRecordLocation(recordLocationMap, 0, segment1, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment1, hashFunction);

    _metadataManager._primaryKeyIterator = pkList1.iterator();
    _metadataManager.removeSegment(segment1);
    Assert.assertEquals(recordLocationMap.size(), 0);
  }

  @Test
  public void verifyReloadSegment() {
    HashFunction hashFunction = HashFunction.NONE;
    Map<Object, IndexSegment> recordLocationMap = _metadataManager._primaryKeyToSegmentMap;

    // Add the first segment
    List<PrimaryKey> pkList1 = new ArrayList<>();
    pkList1.add(DedupTestUtils.getPrimaryKey(0));
    pkList1.add(DedupTestUtils.getPrimaryKey(1));
    pkList1.add(DedupTestUtils.getPrimaryKey(2));
    pkList1.add(DedupTestUtils.getPrimaryKey(0));
    pkList1.add(DedupTestUtils.getPrimaryKey(1));
    pkList1.add(DedupTestUtils.getPrimaryKey(0));
    _metadataManager._primaryKeyIterator = pkList1.iterator();
    ImmutableSegmentImpl segment1 = DedupTestUtils.mockSegment(1, 6);
    _metadataManager.addSegment(segment1);

    // Remove another segment with same PK rows
    _metadataManager._primaryKeyIterator = pkList1.iterator();
    ImmutableSegmentImpl segment2 = DedupTestUtils.mockSegment(1, 6);
    _metadataManager.removeSegment(segment2);
    Assert.assertEquals(recordLocationMap.size(), 3);

    // Keys should still exist
    checkRecordLocation(recordLocationMap, 0, segment1, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment1, hashFunction);
  }

  @Test
  public void verifyAddRow() {
    HashFunction hashFunction = HashFunction.NONE;
    Map<Object, IndexSegment> recordLocationMap = _metadataManager._primaryKeyToSegmentMap;

    // Add the first segment
    List<PrimaryKey> pkList1 = new ArrayList<>();
    pkList1.add(DedupTestUtils.getPrimaryKey(0));
    pkList1.add(DedupTestUtils.getPrimaryKey(1));
    pkList1.add(DedupTestUtils.getPrimaryKey(2));
    pkList1.add(DedupTestUtils.getPrimaryKey(0));
    pkList1.add(DedupTestUtils.getPrimaryKey(1));
    pkList1.add(DedupTestUtils.getPrimaryKey(0));
    _metadataManager._primaryKeyIterator = pkList1.iterator();
    ImmutableSegmentImpl segment1 = DedupTestUtils.mockSegment(1, 6);
    _metadataManager.addSegment(segment1);

    // Same PK exists
    ImmutableSegmentImpl segment2 = DedupTestUtils.mockSegment(2, 6);
    Assert.assertTrue(_metadataManager.checkRecordPresentOrUpdate(DedupTestUtils.getPrimaryKey(0), segment2));
    checkRecordLocation(recordLocationMap, 0, segment1, hashFunction);

    // New PK
    Assert.assertFalse(_metadataManager.checkRecordPresentOrUpdate(DedupTestUtils.getPrimaryKey(3), segment2));
    checkRecordLocation(recordLocationMap, 3, segment2, hashFunction);

    // Same PK as the one recently ingested
    Assert.assertTrue(_metadataManager.checkRecordPresentOrUpdate(DedupTestUtils.getPrimaryKey(3), segment2));
  }

  private static void checkRecordLocation(Map<Object, IndexSegment> recordLocationMap, int keyValue,
      IndexSegment segment, HashFunction hashFunction) {
    IndexSegment indexSegment =
        recordLocationMap.get(HashUtils.hashPrimaryKey(DedupTestUtils.getPrimaryKey(keyValue), hashFunction));
    assertNotNull(indexSegment);
    assertSame(indexSegment, segment);
  }

  private static class TestMetadataManager extends NoRetentionConcurrentMapPartitionDedupMetadataManager {
    Iterator<PrimaryKey> _primaryKeyIterator;

    TestMetadataManager(String tableNameWithType, int partitionId, DedupContext dedupContext) {
      super(tableNameWithType, partitionId, dedupContext);
    }

    @Override
    Iterator<PrimaryKey> getPrimaryKeyIterator(IndexSegment segment) {
      return _primaryKeyIterator;
    }
  }
}
