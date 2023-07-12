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
package org.apache.pinot.queries;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.File;
import java.util.Comparator;
import java.util.List;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Queries test for FUNNEL_COUNT queries using sorted strategy.
 */
@SuppressWarnings("rawtypes")
public class FunnelCountQueriesPartitionedSortedTest extends BaseFunnelCountQueriesTest {

  @Override
  protected String getSettings() {
    return "SETTINGS('partitioned', 'sorted')";
  }

  @Override
  protected int getExpectedNumEntriesScannedInFilter() {
    return 0;
  }

  @Override
  protected int getExpectedInterSegmentMultiplier() {
    return 4;
  }

  @Override
  protected TableConfig getTableConfig() {
    return TABLE_CONFIG_BUILDER.setSortedColumn(ID_COLUMN).build();
  }

  @Override
  protected IndexSegment buildSegment(List<GenericRow> records)
      throws Exception {
    // Simulate PinotSegmentSorter
    records.sort(Comparator.comparingInt(rec -> (Integer) rec.getValue(ID_COLUMN)));

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(getTableConfig(), SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
  }

  @Override
  protected void assertIntermediateResult(Object intermediateResult, long[] expectedCounts) {
    assertTrue(intermediateResult instanceof LongArrayList);
    assertEquals(((LongArrayList)intermediateResult).elements(), expectedCounts);
  }
}
