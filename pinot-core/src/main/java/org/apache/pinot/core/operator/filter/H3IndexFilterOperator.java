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

import com.uber.h3core.LengthUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.core.geospatial.serde.GeometrySerializer;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.operator.docidsets.MatchAllDocIdSet;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.predicate.Predicate;
import org.apache.pinot.core.query.request.context.predicate.RangePredicate;
import org.apache.pinot.core.segment.index.readers.H3IndexReader;
import org.apache.pinot.core.util.H3Utils;
import org.apache.pinot.spi.utils.BytesUtils;
import org.locationtech.jts.geom.Coordinate;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * A filter operator that uses H3 index for geospatial data retrieval
 */
public class H3IndexFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "H3IndexFilterOperator";

  private final IndexSegment _segment;
  private final Predicate _predicate;
  private final int _numDocs;
  private final H3IndexReader _h3IndexReader;
  private final long _h3Id;
  private final double _edgeLength;
  private final double _lowerBound;
  private final double _upperBound;

  public H3IndexFilterOperator(IndexSegment segment, Predicate predicate, int numDocs) {
    _segment = segment;
    _predicate = predicate;
    _numDocs = numDocs;

    List<ExpressionContext> arguments = predicate.getLhs().getFunction().getArguments();
    Coordinate coordinate;
    if (arguments.get(0).getType() == ExpressionContext.Type.IDENTIFIER) {
      _h3IndexReader = segment.getDataSource(arguments.get(0).getIdentifier()).getH3Index();
      coordinate = GeometrySerializer.deserialize(BytesUtils.toBytes(arguments.get(1).getLiteral())).getCoordinate();
    } else {
      _h3IndexReader = segment.getDataSource(arguments.get(1).getIdentifier()).getH3Index();
      coordinate = GeometrySerializer.deserialize(BytesUtils.toBytes(arguments.get(0).getLiteral())).getCoordinate();
    }
    assert _h3IndexReader != null;
    int resolution = _h3IndexReader.getH3IndexResolution().getLowestResolution();
    _h3Id = H3Utils.H3_CORE.geoToH3(coordinate.y, coordinate.x, resolution);
    _edgeLength = H3Utils.H3_CORE.edgeLength(resolution, LengthUnit.m);

    RangePredicate rangePredicate = (RangePredicate) predicate;
    if (!rangePredicate.getLowerBound().equals(RangePredicate.UNBOUNDED)) {
      _lowerBound = Double.parseDouble(rangePredicate.getLowerBound());
    } else {
      _lowerBound = Double.NaN;
    }
    if (!rangePredicate.getUpperBound().equals(RangePredicate.UNBOUNDED)) {
      _upperBound = Double.parseDouble(rangePredicate.getUpperBound());
    } else {
      _upperBound = Double.NaN;
    }
  }

  @Override
  protected FilterBlock getNextBlock() {
    if (_upperBound < 0 || _lowerBound > _upperBound) {
      // Invalid upper bound

      return new FilterBlock(EmptyDocIdSet.getInstance());
    }

    if (Double.isNaN(_lowerBound) || _lowerBound < 0) {
      // No lower bound

      if (Double.isNaN(_upperBound)) {
        // No upper bound
        return new FilterBlock(new MatchAllDocIdSet(_numDocs));
      }

      List<Long> fullMatchH3Ids = getFullMatchH3Ids(_upperBound);
      HashSet<Long> partialMatchH3Ids = new HashSet<>(getPartialMatchH3Ids(_upperBound));
      partialMatchH3Ids.removeAll(fullMatchH3Ids);

      MutableRoaringBitmap fullMatchDocIds = new MutableRoaringBitmap();
      for (long fullMatchH3Id : fullMatchH3Ids) {
        fullMatchDocIds.or(_h3IndexReader.getDocIds(fullMatchH3Id));
      }

      MutableRoaringBitmap partialMatchDocIds = new MutableRoaringBitmap();
      for (long partialMatchH3Id : partialMatchH3Ids) {
        partialMatchDocIds.or(_h3IndexReader.getDocIds(partialMatchH3Id));
      }

      return getFilterBlock(fullMatchDocIds, partialMatchDocIds);
    }

    if (Double.isNaN(_upperBound)) {
      // No upper bound

      List<Long> notMatchH3Ids = getFullMatchH3Ids(_lowerBound);
      Set<Long> partialMatchH3Ids = new HashSet<>(getPartialMatchH3Ids(_lowerBound));

      MutableRoaringBitmap fullMatchDocIds = new MutableRoaringBitmap();
      for (long partialMatchH3Id : partialMatchH3Ids) {
        fullMatchDocIds.or(_h3IndexReader.getDocIds(partialMatchH3Id));
      }
      fullMatchDocIds.flip(0L, _numDocs);

      partialMatchH3Ids.removeAll(notMatchH3Ids);
      MutableRoaringBitmap partialMatchDocIds = new MutableRoaringBitmap();
      for (long partialMatchH3Id : partialMatchH3Ids) {
        partialMatchDocIds.or(_h3IndexReader.getDocIds(partialMatchH3Id));
      }

      return getFilterBlock(fullMatchDocIds, partialMatchDocIds);
    }

    // Both lower bound and upper bound exist
    List<Long> lowerFullMatchH3Ids = getFullMatchH3Ids(_lowerBound);
    List<Long> lowerPartialMatchH3Ids = getPartialMatchH3Ids(_lowerBound);
    List<Long> upperFullMatchH3Ids = getFullMatchH3Ids(_upperBound);
    List<Long> upperPartialMatchH3Ids = getPartialMatchH3Ids(_upperBound);

    Set<Long> fullMatchH3Ids;
    if (upperFullMatchH3Ids.size() > lowerPartialMatchH3Ids.size()) {
      fullMatchH3Ids = new HashSet<>(upperFullMatchH3Ids);
      fullMatchH3Ids.removeAll(lowerPartialMatchH3Ids);
    } else {
      fullMatchH3Ids = Collections.emptySet();
    }
    MutableRoaringBitmap fullMatchDocIds = new MutableRoaringBitmap();
    for (long fullMatchH3Id : fullMatchH3Ids) {
      fullMatchDocIds.or(_h3IndexReader.getDocIds(fullMatchH3Id));
    }

    Set<Long> partialMatchH3Ids = new HashSet<>(upperPartialMatchH3Ids);
    partialMatchH3Ids.removeAll(lowerFullMatchH3Ids);
    partialMatchH3Ids.removeAll(fullMatchH3Ids);
    MutableRoaringBitmap partialMatchDocIds = new MutableRoaringBitmap();
    for (long partialMatchH3Id : partialMatchH3Ids) {
      partialMatchDocIds.or(_h3IndexReader.getDocIds(partialMatchH3Id));
    }

    return getFilterBlock(fullMatchDocIds, partialMatchDocIds);
  }

  /**
   * Returns the H3 ids that is ALWAYS fully covered by the circle with the given distance as the radius and a point
   * within the _h3Id hexagon as the center.
   */
  private List<Long> getFullMatchH3Ids(double distance) {
    // NOTE: Pick a constant slightly larger than sqrt(3) to be conservative
    int numRings = (int) Math.floor((distance / _edgeLength - 2) / 1.7321);
    if (numRings >= 0) {
      return H3Utils.H3_CORE.kRing(_h3Id, numRings);
    } else {
      return Collections.emptyList();
    }
  }

  /**
   * Returns the H3 ids that MIGHT BE partially/fully covered by the circle with the given distance as the radius and a
   * point within the _h3Id hexagon as the center.
   */
  private List<Long> getPartialMatchH3Ids(double distance) {
    // NOTE: Add a small delta (0.001) to be conservative
    int numRings = (int) Math.floor((distance / _edgeLength + 2) / 1.5 + 0.001);
    return H3Utils.H3_CORE.kRing(_h3Id, numRings);
  }

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
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
