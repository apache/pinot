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
package org.apache.pinot.core.plan;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.CustomPredicate;
import org.apache.pinot.common.request.context.predicate.IsNotNullPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.operator.filter.custom.CustomFilterOperatorFactory;
import org.apache.pinot.core.operator.filter.custom.CustomFilterOperatorRegistry;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.upsert.UpsertUtils;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class FilterPlanNodeTest {

  @AfterMethod
  public void tearDown() {
    CustomFilterOperatorRegistry.clear();
  }

  @Test
  public void testConsistentSnapshot()
      throws Exception {
    IndexSegment segment = mock(IndexSegment.class);
    SegmentMetadata meta = mock(SegmentMetadata.class);
    when(segment.getSegmentMetadata()).thenReturn(meta);
    ThreadSafeMutableRoaringBitmap bitmap = new ThreadSafeMutableRoaringBitmap();
    when(segment.getValidDocIds()).thenReturn(bitmap);
    AtomicInteger numDocs = new AtomicInteger(0);
    when(meta.getTotalDocs()).then((Answer<Integer>) invocationOnMock -> numDocs.get());
    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.getFilter()).thenReturn(null);

    numDocs.set(3);
    bitmap.add(0);
    bitmap.add(1);
    bitmap.add(2);

    // Continuously update the last value by moving it one doc id forward
    // Follow the order of MutableIndexSegmentImpl: first add the row, update the doc count and then change the
    // validDocId bitmap
    Thread updater = new Thread(() -> {
      for (int i = 3; i < 10_000_000; i++) {
        numDocs.incrementAndGet();
        bitmap.replace(i - 2, i);
      }
    });
    updater.start();

    // Result should be invariant - always exactly 3 docs
    for (int i = 0; i < 10_000; i++) {
      SegmentContext segmentContext = new SegmentContext(segment);
      segmentContext.setQueryableDocIdsSnapshot(UpsertUtils.getQueryableDocIdsSnapshotFromSegment(segment));
      assertEquals(getNumberOfFilteredDocs(segmentContext, queryContext), 3);
    }

    updater.join();
  }

  @Test
  public void testCustomFilterReceivesMetadataFilterSignal() {
    TestCustomFilterOperatorFactory factory = new TestCustomFilterOperatorFactory();
    CustomFilterOperatorRegistry.register(factory);

    IndexSegment segment = mock(IndexSegment.class);
    SegmentMetadata meta = mock(SegmentMetadata.class);
    DataSource dataSource = mock(DataSource.class);
    when(segment.getSegmentMetadata()).thenReturn(meta);
    when(meta.getTotalDocs()).thenReturn(10);
    when(segment.getDataSource(Mockito.anyString(), Mockito.isNull())).thenReturn(dataSource);

    QueryContext queryContext = mock(QueryContext.class);
    FilterContext filter = FilterContext.forAnd(List.of(
        FilterContext.forPredicate(new TestCustomPredicate(ExpressionContext.forIdentifier("customCol"))),
        FilterContext.forPredicate(new IsNotNullPredicate(ExpressionContext.forIdentifier("metadataCol")))));
    when(queryContext.getFilter()).thenReturn(filter);
    when(queryContext.getSchema()).thenReturn(null);

    FilterPlanNode filterPlanNode = new FilterPlanNode(new SegmentContext(segment), queryContext);
    BaseFilterOperator operator = filterPlanNode.run();

    assertTrue(operator.isResultMatchingAll());
    assertTrue(factory._hasMetadataFilter);
  }

  private int getNumberOfFilteredDocs(SegmentContext segmentContext, QueryContext queryContext) {
    FilterPlanNode node = new FilterPlanNode(segmentContext, queryContext);
    BaseFilterOperator op = node.run();
    int numDocsFiltered = 0;
    FilterBlock block = op.nextBlock();
    BlockDocIdSet blockIds = block.getBlockDocIdSet();
    BlockDocIdIterator it = blockIds.iterator();
    while (it.next() != Constants.EOF) {
      numDocsFiltered++;
    }
    return numDocsFiltered;
  }

  private static final class TestCustomPredicate extends CustomPredicate {
    private TestCustomPredicate(ExpressionContext lhs) {
      super(lhs, "TEST_CUSTOM");
    }
  }

  private static final class TestCustomFilterOperatorFactory implements CustomFilterOperatorFactory {
    private boolean _hasMetadataFilter;

    @Override
    public String predicateName() {
      return "TEST_CUSTOM";
    }

    @Override
    public BaseFilterOperator createFilterOperator(IndexSegment indexSegment, QueryContext queryContext,
        Predicate predicate, @Nullable DataSource dataSource, int numDocs) {
      return new MatchAllFilterOperator(numDocs);
    }

    @Override
    public BaseFilterOperator createFilterOperator(IndexSegment indexSegment, QueryContext queryContext,
        Predicate predicate, @Nullable DataSource dataSource, int numDocs, boolean hasMetadataFilter) {
      _hasMetadataFilter = hasMetadataFilter;
      return new MatchAllFilterOperator(numDocs);
    }
  }
}
