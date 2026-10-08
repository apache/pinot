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
package org.apache.pinot.core.startree.operator;

import it.unimi.dsi.fastutil.objects.ObjectBooleanPair;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.operator.filter.FilterOperatorUtils;
import org.apache.pinot.core.operator.filter.InvertedIndexFilterOperator;
import org.apache.pinot.core.operator.filter.TestUtils;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.CompositePredicateEvaluator;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.index.startree.StarTree;
import org.apache.pinot.segment.spi.index.startree.StarTreeNode;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Tests star-tree document selection through the public filter result. Each invocation owns its fixture.
public class StarTreeFilterOperatorTest {
  @DataProvider(name = "nullHandling")
  public Object[][] nullHandling() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "nullHandling")
  public void testRangesAndAggregatedDocsWithResidualPredicate(boolean nullHandlingEnabled) {
    // Early leaves retain the dim1 predicate, while deeper dim1 nodes contribute aggregate document ids.
    // Keep the range order deliberately unsorted, overlap two ranges, and cross the 16-bit container boundary.
    StarTreeNode firstAggregate = leaf(1, 0, 20, 21, 50);
    StarTreeNode lastAggregate = leaf(1, 1, 30, 31, 70000);
    StarTreeNode branch = branch(0, 4, List.of(firstAggregate, lastAggregate));
    StarTreeNode root = branch(-1, StarTreeNode.ALL, List.of(
        leaf(0, 0, 65534, 65538, 40), leaf(0, 1, 3, 4, 41), leaf(0, 2, 2, 4, 42),
        leaf(0, 3, 10, 10, 43), branch));
    when(root.getAggregatedDocId()).thenReturn(70000);
    StarTree starTree = mock(StarTree.class);
    when(starTree.getRoot()).thenReturn(root);
    when(starTree.getDimensionNames()).thenReturn(List.of("dim0", "dim1"));
    StarTreeV2Metadata metadata = mock(StarTreeV2Metadata.class);
    when(metadata.getNumDocs()).thenReturn(70001);
    StarTreeV2 starTreeV2 = mock(StarTreeV2.class);
    when(starTreeV2.getStarTree()).thenReturn(starTree);
    when(starTreeV2.getMetadata()).thenReturn(metadata);
    QueryContext queryContext = new QueryContext.Builder().build();
    queryContext.setNullHandlingEnabled(nullHandlingEnabled);

    // A group-by on dim1 takes the same range/singleton paths without applying the residual predicate.
    StarTreeFilterOperator unfiltered = new StarTreeFilterOperator(queryContext, starTreeV2, Map.of(), Set.of("dim1"));
    assertEquals(TestUtils.getDocIds(unfiltered.nextBlock().getBlockDocIdSet()),
        List.of(2, 3, 50, 65534, 65535, 65536, 65537, 70000));

    StarTreeFilterOperator aggregateOnly = new StarTreeFilterOperator(queryContext, starTreeV2, Map.of(), null);
    assertEquals(TestUtils.getDocIds(aggregateOnly.nextBlock().getBlockDocIdSet()), List.of(70000));

    PredicateEvaluator predicateEvaluator = mock(PredicateEvaluator.class);
    when(predicateEvaluator.getPredicateType()).thenReturn(Predicate.Type.IN);
    when(predicateEvaluator.getMatchingDictIds()).thenReturn(new int[]{0, 1});
    CompositePredicateEvaluator compositePredicateEvaluator =
        new CompositePredicateEvaluator(List.of(ObjectBooleanPair.of(predicateEvaluator, false)));
    InvertedIndexReader<?> invertedIndex = mock(InvertedIndexReader.class);
    // Include a matching indexed document outside the selected ranges to prove the residual is conjoined with them.
    doReturn(MutableRoaringBitmap.bitmapOf(2, 50, 999, 65535, 65536)).when(invertedIndex).getDocIds(0);
    doReturn(MutableRoaringBitmap.bitmapOf(70000)).when(invertedIndex).getDocIds(1);
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSourceMetadata.isSingleValue()).thenReturn(true);
    when(dataSourceMetadata.isSorted()).thenReturn(false);
    NullValueVectorReader nullValueVector = mock(NullValueVectorReader.class);
    when(nullValueVector.getNullBitmap()).thenReturn(MutableRoaringBitmap.bitmapOf(65536));
    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getColumnName()).thenReturn("dim1");
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);
    when(dataSource.getDictionary()).thenReturn(null);
    when(dataSource.getRangeIndex()).thenReturn(null);
    doReturn(invertedIndex).when(dataSource).getInvertedIndex();
    when(dataSource.getNullValueVector()).thenReturn(nullValueVector);
    when(starTreeV2.getDataSource("dim1")).thenReturn(dataSource);
    assertTrue(FilterOperatorUtils.getLeafFilterOperator(queryContext, predicateEvaluator, dataSource, 70001)
        instanceof InvertedIndexFilterOperator);

    StarTreeFilterOperator filtered = new StarTreeFilterOperator(queryContext, starTreeV2,
        Map.of("dim1", List.of(compositePredicateEvaluator)), null);
    assertEquals(TestUtils.getDocIds(filtered.nextBlock().getBlockDocIdSet()),
        nullHandlingEnabled ? List.of(2, 50, 65535, 70000) : List.of(2, 50, 65535, 65536, 70000));
  }

  private static StarTreeNode leaf(int dimensionId, int dimensionValue, int startDocId, int endDocId,
      int aggregatedDocId) {
    StarTreeNode node = mock(StarTreeNode.class);
    when(node.getDimensionId()).thenReturn(dimensionId);
    when(node.getDimensionValue()).thenReturn(dimensionValue);
    when(node.isLeaf()).thenReturn(true);
    when(node.getStartDocId()).thenReturn(startDocId);
    when(node.getEndDocId()).thenReturn(endDocId);
    when(node.getAggregatedDocId()).thenReturn(aggregatedDocId);
    return node;
  }

  private static StarTreeNode branch(int dimensionId, int dimensionValue, List<StarTreeNode> children) {
    StarTreeNode node = mock(StarTreeNode.class);
    when(node.getDimensionId()).thenReturn(dimensionId);
    when(node.getDimensionValue()).thenReturn(dimensionValue);
    when(node.getNumChildren()).thenReturn(children.size());
    when(node.getChildrenIterator()).thenAnswer(invocation -> children.iterator());
    return node;
  }
}
