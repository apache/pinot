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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.segment.readers.PrimaryKeyReader;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;


public class ConcurrentMapPartitionDedupMetadataManagerWithoutTTLTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(),
      ConcurrentMapPartitionDedupMetadataManagerWithoutTTLTest.class.getSimpleName());
  private DedupContext.Builder _dedupContextBuilder;

  @BeforeMethod
  public void setUpContextBuilder()
      throws IOException {
    FileUtils.forceMkdir(TEMP_DIR);
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(TEMP_DIR);
    _dedupContextBuilder = new DedupContext.Builder()
        .setTableConfig(mock(TableConfig.class))
        .setSchema(mock(Schema.class))
        .setTableDataManager(tableDataManager)
        .setPrimaryKeyColumns(List.of("primaryKeyColumn"));
  }

  @AfterMethod
  public void cleanup() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @Test
  public void testAddRemoveSegment()
      throws IOException {
    verifyAddRemoveSegment(HashFunction.NONE);
    verifyAddRemoveSegment(HashFunction.MD5);
    verifyAddRemoveSegment(HashFunction.MURMUR3);
  }

  private void verifyAddRemoveSegment(HashFunction hashFunction)
      throws IOException {
    _dedupContextBuilder.setHashFunction(hashFunction);
    ConcurrentMapPartitionDedupMetadataManager metadataManager =
        new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0,
            _dedupContextBuilder.build());

    // Add the first segment
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = generateDedupRecordInfoReader();
    Iterator<DedupRecordInfo> dedupRecordInfoIterator = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 6);
    ImmutableSegmentImpl segment1 = DedupTestUtils.mockSegment(1, 6);
    metadataManager.doAddOrReplaceSegment(null, segment1, dedupRecordInfoIterator);
    Assert.assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.size(), 3);
    checkRecordLocation(metadataManager._primaryKeyToSegmentAndTimeMap, 0, segment1, hashFunction);
    checkRecordLocation(metadataManager._primaryKeyToSegmentAndTimeMap, 1, segment1, hashFunction);
    checkRecordLocation(metadataManager._primaryKeyToSegmentAndTimeMap, 2, segment1, hashFunction);

    // reset the iterator
    dedupRecordInfoIterator = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 6);
    metadataManager.doRemoveSegment(segment1, dedupRecordInfoIterator);
    Assert.assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.size(), 0);

    metadataManager.stop();
    metadataManager.close();
  }

  @Test
  public void testReloadSegment()
      throws IOException {
    verifyReloadSegment(HashFunction.NONE);
    verifyReloadSegment(HashFunction.MD5);
    verifyReloadSegment(HashFunction.MURMUR3);
  }

  private void verifyReloadSegment(HashFunction hashFunction)
      throws IOException {
    _dedupContextBuilder.setHashFunction(hashFunction);
    ConcurrentMapPartitionDedupMetadataManager metadataManager =
        new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0,
            _dedupContextBuilder.build());

    // Add the first segment
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = generateDedupRecordInfoReader();
    Iterator<DedupRecordInfo> dedupRecordInfoIterator = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 6);
    ImmutableSegmentImpl segment1 = DedupTestUtils.mockSegment(1, 6);
    metadataManager.doAddOrReplaceSegment(null, segment1, dedupRecordInfoIterator);

    // Remove another segment with same PK rows
    // reset the iterator
    dedupRecordInfoIterator = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 6);
    ImmutableSegmentImpl segment2 = DedupTestUtils.mockSegment(1, 6);
    metadataManager.doRemoveSegment(segment2, dedupRecordInfoIterator);
    Assert.assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.size(), 3);

    // Keys should still exist
    checkRecordLocation(metadataManager._primaryKeyToSegmentAndTimeMap, 0, segment1, hashFunction);
    checkRecordLocation(metadataManager._primaryKeyToSegmentAndTimeMap, 1, segment1, hashFunction);
    checkRecordLocation(metadataManager._primaryKeyToSegmentAndTimeMap, 2, segment1, hashFunction);

    metadataManager.stop();
    metadataManager.close();
  }

  @Test
  public void testAddRow()
      throws IOException {
    verifyAddRow(HashFunction.NONE);
    verifyAddRow(HashFunction.MD5);
    verifyAddRow(HashFunction.MURMUR3);
  }

  private void verifyAddRow(HashFunction hashFunction)
      throws IOException {
    _dedupContextBuilder.setHashFunction(hashFunction);
    ConcurrentMapPartitionDedupMetadataManager metadataManager =
        new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0,
            _dedupContextBuilder.build());

    // Add the first segment
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = generateDedupRecordInfoReader();
    Iterator<DedupRecordInfo> dedupRecordInfoIterator = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 6);
    ImmutableSegmentImpl segment1 = DedupTestUtils.mockSegment(1, 6);
    metadataManager.doAddOrReplaceSegment(null, segment1, dedupRecordInfoIterator);

    // Same PK exists
    // reset the iterator
    dedupRecordInfoIterator = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 6);
    ImmutableSegmentImpl segment2 = DedupTestUtils.mockSegment(2, 6);
    while (dedupRecordInfoIterator.hasNext()) {
      DedupRecordInfo dedupRecordInfo = dedupRecordInfoIterator.next();
      Assert.assertTrue(metadataManager.checkRecordPresentOrUpdate(dedupRecordInfo, segment2));
    }
    checkRecordLocation(metadataManager._primaryKeyToSegmentAndTimeMap, 0, segment1, hashFunction);
    checkRecordLocation(metadataManager._primaryKeyToSegmentAndTimeMap, 1, segment1, hashFunction);
    checkRecordLocation(metadataManager._primaryKeyToSegmentAndTimeMap, 2, segment1, hashFunction);

    // New PK
    Assert.assertFalse(
        metadataManager.checkRecordPresentOrUpdate(new DedupRecordInfo(DedupTestUtils.getPrimaryKey(3), 3000),
            segment2));
    checkRecordLocation(metadataManager._primaryKeyToSegmentAndTimeMap, 3, segment2, hashFunction);

    // Same PK as the one recently ingested
    Assert.assertTrue(
        metadataManager.checkRecordPresentOrUpdate(new DedupRecordInfo(DedupTestUtils.getPrimaryKey(3), 4000),
            segment2));

    metadataManager.stop();
    metadataManager.close();
  }

  private static DedupUtils.DedupRecordInfoReader generateDedupRecordInfoReader() {
    PrimaryKeyReader primaryKeyReader = Mockito.mock(PrimaryKeyReader.class);
    PinotSegmentColumnReader dedupTimeColumnReader = Mockito.mock(PinotSegmentColumnReader.class);
    Mockito.when(primaryKeyReader.getPrimaryKey(0)).thenReturn(DedupTestUtils.getPrimaryKey(0));
    Mockito.when(primaryKeyReader.getPrimaryKey(1)).thenReturn(DedupTestUtils.getPrimaryKey(1));
    Mockito.when(primaryKeyReader.getPrimaryKey(2)).thenReturn(DedupTestUtils.getPrimaryKey(2));
    Mockito.when(primaryKeyReader.getPrimaryKey(3)).thenReturn(DedupTestUtils.getPrimaryKey(0));
    Mockito.when(primaryKeyReader.getPrimaryKey(4)).thenReturn(DedupTestUtils.getPrimaryKey(1));
    Mockito.when(primaryKeyReader.getPrimaryKey(5)).thenReturn(DedupTestUtils.getPrimaryKey(0));
    for (int i = 0; i < 6; i++) {
      Mockito.when(dedupTimeColumnReader.getValue(i)).thenReturn(i * 1000);
    }
    return new DedupUtils.DedupRecordInfoReader(primaryKeyReader, dedupTimeColumnReader);
  }

  private static void checkRecordLocation(Map<Object, Pair<IndexSegment, Double>> recordLocationMap, int keyValue,
      IndexSegment segment, HashFunction hashFunction) {
    IndexSegment indexSegment =
        recordLocationMap.get(HashUtils.hashPrimaryKey(DedupTestUtils.getPrimaryKey(keyValue), hashFunction)).getLeft();
    assertNotNull(indexSegment);
    assertSame(indexSegment, segment);
  }
}
