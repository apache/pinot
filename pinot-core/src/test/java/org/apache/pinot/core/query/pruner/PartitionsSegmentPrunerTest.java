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
package org.apache.pinot.core.query.pruner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;


public class PartitionsSegmentPrunerTest {
  private final PartitionsSegmentPruner _partitionPruner = new PartitionsSegmentPruner();

  @Test
  public void testPruner() {
    List<IndexSegment> indexSegments =
        Arrays.asList(
            getIndexSegment(ImmutableMap.of("key1", ImmutableSet.of(1), "key2", ImmutableSet.of(1))),
            getIndexSegment(ImmutableMap.of("key1", ImmutableSet.of(2), "key2", ImmutableSet.of(2))),
            getIndexSegment(ImmutableMap.of("key1", ImmutableSet.of(3), "key2", ImmutableSet.of(3))));

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable");
    List<IndexSegment> result = _partitionPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 3);

    queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable OPTION (columnPartitionMap=key1:1)");
    result = _partitionPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 1);
    assertSame(result.get(0), indexSegments.get(0));

    queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable OPTION (columnPartitionMap=key1:1/key1:2)");
    result = _partitionPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 2);
    assertSame(result.get(0), indexSegments.get(0));
    assertSame(result.get(1), indexSegments.get(1));

    queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable OPTION (columnPartitionMap=key1:1/key2:2)");
    result = _partitionPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 2);
    assertSame(result.get(0), indexSegments.get(0));
    assertSame(result.get(1), indexSegments.get(1));

    queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable OPTION (columnPartitionMap=key1:1/key2:1)");
    result = _partitionPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 1);
    assertSame(result.get(0), indexSegments.get(0));
  }

  private IndexSegment getIndexSegment(Map<String, Set<Integer>> columnPartitionMap) {
    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getColumnNames()).thenReturn(ImmutableSet.copyOf(columnPartitionMap.keySet()));
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(indexSegment.getSegmentMetadata()).thenReturn(segmentMetadata);
    for (Map.Entry<String, Set<Integer>> entry : columnPartitionMap.entrySet()) {
      ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
      when(columnMetadata.getPartitions()).thenReturn(entry.getValue());
      when(segmentMetadata.getColumnMetadataFor(entry.getKey())).thenReturn(columnMetadata);
    }
    return indexSegment;
  }
}
