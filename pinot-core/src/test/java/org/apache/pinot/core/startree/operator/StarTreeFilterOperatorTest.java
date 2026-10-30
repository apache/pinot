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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.startree.CompositePredicateEvaluator;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.startree.StarTree;
import org.apache.pinot.segment.spi.index.startree.StarTreeNode;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/// Exercises residual leaf predicates through complete star-tree filters; all fixture state is local to each test.
public class StarTreeFilterOperatorTest {
  @Test
  @SuppressWarnings("unchecked") // The index reader mock supplies the same typed bitmap contract as the real reader.
  public void testNegatedResidualPredicateUsesTheComplementWithinTheLeafRange() {
    QueryContext query = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable");
    StarTreeNode root = mock(StarTreeNode.class);
    when(root.isLeaf()).thenReturn(true);
    when(root.getDimensionId()).thenReturn(-1);
    when(root.getStartDocId()).thenReturn(0);
    when(root.getEndDocId()).thenReturn(4);
    StarTree tree = mock(StarTree.class);
    when(tree.getRoot()).thenReturn(root);
    when(tree.getDimensionNames()).thenReturn(List.of("col"));
    StarTreeV2Metadata metadata = mock(StarTreeV2Metadata.class);
    when(metadata.getNumDocs()).thenReturn(4);
    StarTreeV2 index = mock(StarTreeV2.class);
    when(index.getStarTree()).thenReturn(tree);
    when(index.getMetadata()).thenReturn(metadata);

    MutableRoaringBitmap matches = new MutableRoaringBitmap();
    matches.add(0);
    matches.add(2);
    InvertedIndexReader<ImmutableRoaringBitmap> reader = mock(InvertedIndexReader.class);
    when(reader.getDocIds(0)).thenReturn(matches);
    DataSourceMetadata columnMetadata = mock(DataSourceMetadata.class);
    when(columnMetadata.isSingleValue()).thenReturn(true);
    DataSource column = mock(DataSource.class);
    when(column.getColumnName()).thenReturn("col");
    when(column.getDataSourceMetadata()).thenReturn(columnMetadata);
    doReturn(reader).when(column).getInvertedIndex();
    when(index.getDataSource("col")).thenReturn(column);
    PredicateEvaluator predicate = mock(PredicateEvaluator.class);
    when(predicate.getPredicateType()).thenReturn(Predicate.Type.EQ);
    when(predicate.getPredicate()).thenReturn(new EqPredicate(ExpressionContext.forIdentifier("col"), "1"));
    when(predicate.getMatchingDictIds()).thenReturn(new int[]{0});

    for (boolean negated : new boolean[]{false, true}) {
      CompositePredicateEvaluator residual =
          new CompositePredicateEvaluator(List.of(ObjectBooleanPair.of(predicate, negated)));
      StarTreeFilterOperator filter = new StarTreeFilterOperator(query, index, Map.of("col", List.of(residual)), null);
      BlockDocIdIterator iterator = filter.nextBlock().getBlockDocIdSet().iterator();
      List<Integer> actual = new ArrayList<>();
      int docId;
      while ((docId = iterator.next()) != Constants.EOF) {
        actual.add(docId);
      }
      assertEquals(actual, negated ? List.of(1, 3) : List.of(0, 2));
    }
  }
}
