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

import it.unimi.dsi.fastutil.longs.LongSet;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.ExpressionContext.Type;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.H3Utils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.reader.H3IndexReader;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.BytesUtils;
import org.locationtech.jts.geom.Geometry;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * A filter operator that uses H3 index for geospatial data inclusion
 */
public class H3InclusionIndexFilterOperator extends BaseFilterOperator {

  private static final String EXPLAIN_NAME = "INCLUSION_FILTER_H3_INDEX";

  private static final String OPERATOR_NAME = "H3InclusionIndexFilterOperator";

  private final IndexSegment _segment;
  private final Predicate _predicate;
  private final int _numDocs;
  private final H3IndexReader _h3IndexReader;
  private final LongSet _fullyCoverH3Cells;
  private final LongSet _potentialCoverH3Cells;
  private final boolean _isPositiveCheck;

  public H3InclusionIndexFilterOperator(IndexSegment segment, Predicate predicate, int numDocs) {
    _segment = segment;
    _predicate = predicate;
    _numDocs = numDocs;

    List<ExpressionContext> arguments = predicate.getLhs().getFunction().getArguments();
    Geometry geometry;
    // Assume first argument is Literal, and second argument is IDENTIFIER for St_Contains.
    assert arguments.get(1).getType() == Type.IDENTIFIER;
    assert arguments.get(0).getType() == Type.LITERAL;
    EqPredicate eqPredicate = (EqPredicate) predicate;
    if (eqPredicate.getValue().equals("1")) {
      _isPositiveCheck = true;
    } else if (eqPredicate.getValue().equals("0")) {
      _isPositiveCheck = false;
    } else {
      throw new BadQueryRequestException("Expected value for ST_Contain is 0 or 1");
    }
    // look up arg1's h3 indices
    _h3IndexReader = segment.getDataSource(arguments.get(1).getIdentifier()).getH3Index();
    // arg0 is the literal
    geometry = GeometrySerializer.deserialize(BytesUtils.toBytes(arguments.get(0).getLiteral()));
    // must be some h3 index
    assert _h3IndexReader != null;

    // get the set of H3 cells at the specified resolution which completely cover the input shape and potential cover.
    Pair<LongSet, LongSet> fullCoverAndPotentialCoverCells =
        H3Utils.coverGeometryInH3(geometry, _h3IndexReader.getH3IndexResolution().getLowestResolution());

    _fullyCoverH3Cells = fullCoverAndPotentialCoverCells.getLeft();
    _potentialCoverH3Cells = fullCoverAndPotentialCoverCells.getRight();
  }

  @Override
  protected FilterBlock getNextBlock() {
    // have list of h3 cell ids for polygon provided
    // return filtered num_docs
    ImmutableRoaringBitmap[] potentialMatchDocIds = new ImmutableRoaringBitmap[_potentialCoverH3Cells.size()];
    int i = 0;

    for (long h3IndexId : _potentialCoverH3Cells) {
      potentialMatchDocIds[i++] = _h3IndexReader.getDocIds(h3IndexId);
    }

    MutableRoaringBitmap potentialMatchMutableRoaringBitmap = BufferFastAggregation.or(potentialMatchDocIds);
    if (_isPositiveCheck) {
      ImmutableRoaringBitmap[] fullMatchDocIds = new ImmutableRoaringBitmap[_fullyCoverH3Cells.size()];
      i = 0;
      for (long h3IndexId : _fullyCoverH3Cells) {
        fullMatchDocIds[i++] = _h3IndexReader.getDocIds(h3IndexId);
      }

      MutableRoaringBitmap fullMatchMutableRoaringBitmap = BufferFastAggregation.or(fullMatchDocIds);
      return getFilterBlock(fullMatchMutableRoaringBitmap, potentialMatchMutableRoaringBitmap);
    } else {
      i = 0;
      _potentialCoverH3Cells.removeAll(_fullyCoverH3Cells);
      ImmutableRoaringBitmap[] potentialNotMatchMutableRoaringBitmap =
          new ImmutableRoaringBitmap[_potentialCoverH3Cells.size()];
      for (long h3IndexId : _potentialCoverH3Cells) {
        potentialNotMatchMutableRoaringBitmap[i++] = _h3IndexReader.getDocIds(h3IndexId);
      }

      MutableRoaringBitmap potentialNotMatch = BufferFastAggregation.or(potentialNotMatchMutableRoaringBitmap);
      potentialMatchMutableRoaringBitmap.flip(0L, _numDocs);
      return getFilterBlock(potentialMatchMutableRoaringBitmap, potentialNotMatch);
    }
  }

  /**
   * Returns the filter block based on the given the partial match doc ids.
   */
  private FilterBlock getFilterBlock(MutableRoaringBitmap fullMatchDocIds, MutableRoaringBitmap partialMatchDocIds) {
    ExpressionFilterOperator expressionFilterOperator = new ExpressionFilterOperator(_segment, _predicate, _numDocs);
    ScanBasedDocIdIterator docIdIterator =
        (ScanBasedDocIdIterator) expressionFilterOperator.getNextBlock().getBlockDocIdSet().iterator();
    MutableRoaringBitmap result = docIdIterator.applyAnd(partialMatchDocIds);
    result.or(fullMatchDocIds);
    return new FilterBlock(new BitmapDocIdSet(result, _numDocs) {
      @Override
      public long getNumEntriesScannedInFilter() {
        return docIdIterator.getNumEntriesScanned();
      }
    });
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
