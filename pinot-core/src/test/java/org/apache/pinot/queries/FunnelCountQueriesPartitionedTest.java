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
import java.util.Collections;
import java.util.List;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImplTestUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.readers.GenericRow;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Queries test for FUNNEL_COUNT queries.
 */
@SuppressWarnings("rawtypes")
public class FunnelCountQueriesPartitionedTest extends BaseFunnelCountQueriesTest {

  @Override
  protected String getSettings() {
    return "SETTINGS('partitioned')";
  }

  @Override
  protected int getExpectedNumEntriesScannedInFilter() {
    return NUM_RECORDS;
  }

  @Override
  protected int getExpectedInterSegmentMultiplier() {
    return 4;
  }

  @Override
  protected TableConfig getTableConfig() {
    return TABLE_CONFIG_BUILDER.build();
  }

  @Override
  protected IndexSegment buildSegment(List<GenericRow> records)
      throws Exception {
    MutableSegment mutableSegment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(SCHEMA, Collections.emptySet(), Collections.emptySet(),
            Collections.emptySet(), false);
    for (GenericRow record : records) {
      mutableSegment.index(record, null);
    }
    return mutableSegment;
  }

  @Override
  protected void assertIntermediateResult(Object intermediateResult, long[] expectedCounts) {
    assertTrue(intermediateResult instanceof LongArrayList);
    assertEquals(((LongArrayList) intermediateResult).elements(), expectedCounts);
  }
}
