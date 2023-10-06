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
package org.apache.pinot.core.operator.filter;

import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.H3Utils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.reader.H3IndexReader;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.locationtech.jts.geom.Geometry;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * A filter operator that uses H3 index for geospatial data inclusion
 */
public class H3InclusionIndexFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "INCLUSION_FILTER_H3_INDEX";
  private static final String LITERAL_H3_CELLS_CACHE_NAME = "st_contain_literal_h3_cells";

  private final IndexSegment _segment;
  private final QueryContext _queryContext;
  private final Predicate _predicate;
  private final H3IndexReader _h3IndexReader;
  private final Geometry _geometry;
  private final boolean _isPositiveCheck;

  public H3InclusionIndexFilterOperator(IndexSegment segment, QueryContext queryContext, Predicate predicate,
      int numDocs) {
    super(numDocs, false);
    _segment = segment;
    _queryContext = queryContext;
    _predicate = predicate;

    List<ExpressionContext> arguments = predicate.getLhs().getFunction().getArguments();
    EqPredicate eqPredicate = (EqPredicate) predicate;
    _isPositiveCheck = BooleanUtils.toBoolean(eqPredicate.getValue());

    if (arguments.get(0).getType() == ExpressionContext.Type.IDENTIFIER) {
      _h3IndexReader = segment.getDataSource(arguments.get(0).getIdentifier()).getH3Index();
      _geometry = GeometrySerializer.deserialize(arguments.get(1).getLiteral().getBytesValue());
    } else {
      _h3IndexReader = segment.getDataSource(arguments.get(1).getIdentifier()).getH3Index();
      _geometry = GeometrySerializer.deserialize(arguments.get(0).getLiteral().getBytesValue());
    }
    // must be some h3 index
    assert _h3IndexReader != null : "the column must have H3 index setup.";
  }

  @Override
  protected BlockDocIdSet getTrues() {
    // get the set of H3 cells at the specified resolution which completely cover the input shape and potential cover.
    Pair<LongSet, LongSet> fullCoverAndPotentialCoverCells = _queryContext
        .getOrComputeSharedValue(Pair.class, LITERAL_H3_CELLS_CACHE_NAME,
            k -> H3Utils.coverGeometryInH3(_geometry, _h3IndexReader.getH3IndexResolution().getLowestResolution()));

    LongSet fullyCoverH3Cells = fullCoverAndPotentialCoverCells.getLeft();
    LongSet potentialCoverH3Cells = fullCoverAndPotentialCoverCells.getRight();

    int i = 0;
    ImmutableRoaringBitmap[] fullMatchDocIds = new ImmutableRoaringBitmap[fullyCoverH3Cells.size()];
    LongIterator fullyCoverH3CellsIterator = fullyCoverH3Cells.iterator();
    while (fullyCoverH3CellsIterator.hasNext()) {
      fullMatchDocIds[i++] = _h3IndexReader.getDocIds(fullyCoverH3CellsIterator.nextLong());
    }
    MutableRoaringBitmap fullMatch = BufferFastAggregation.or(fullMatchDocIds);

    // remove full match from potential match to get potential not match cells.
    i = 0;
    potentialCoverH3Cells.removeAll(fullyCoverH3Cells);
    ImmutableRoaringBitmap[] potentialNotMatchDocIds = new ImmutableRoaringBitmap[potentialCoverH3Cells.size()];
    LongIterator potentialMatchH3CellsIterator = potentialCoverH3Cells.iterator();
    while (potentialMatchH3CellsIterator.hasNext()) {
      potentialNotMatchDocIds[i++] = _h3IndexReader.getDocIds(potentialMatchH3CellsIterator.nextLong());
    }
    MutableRoaringBitmap potentialMatch = BufferFastAggregation.or(potentialNotMatchDocIds);

    if (_isPositiveCheck) {
      return getFilterBlock(fullMatch, potentialMatch);
    } else {
      // potentialMatch also include potential not match docs.
      // full not match is first flip potentialMatch, then andNot full match.
      MutableRoaringBitmap fullNotMatch = potentialMatch.clone();
      fullNotMatch.flip(0L, _numDocs);
      fullNotMatch.andNot(fullMatch);
      return getFilterBlock(fullNotMatch, potentialMatch);
    }
  }

  /**
   * Returns the filter block document IDs based on the given the partial match doc ids.
   */
  private BlockDocIdSet getFilterBlock(MutableRoaringBitmap fullMatchDocIds, MutableRoaringBitmap partialMatchDocIds) {
    ExpressionFilterOperator expressionFilterOperator =
        new ExpressionFilterOperator(_segment, _queryContext, _predicate, _numDocs);
    ScanBasedDocIdIterator docIdIterator = (ScanBasedDocIdIterator) expressionFilterOperator.getTrues().iterator();
    MutableRoaringBitmap result = docIdIterator.applyAnd(partialMatchDocIds);
    result.or(fullMatchDocIds);
    return new BitmapDocIdSet(result, _numDocs) {
      @Override
      public long getNumEntriesScannedInFilter() {
        return docIdIterator.getNumEntriesScanned();
      }
    };
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(inclusionIndex:h3_index");
    stringBuilder.append(",operator:").append(_predicate.getType());
    stringBuilder.append(",predicate:").append(_predicate.toString());
    return stringBuilder.append(')').toString();
  }
}
