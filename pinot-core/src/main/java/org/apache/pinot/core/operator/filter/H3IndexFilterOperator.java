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

import com.uber.h3core.H3Core;
import com.uber.h3core.LengthUnit;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.geospatial.serde.GeometrySerializer;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.FunctionContext;
import org.apache.pinot.core.query.request.context.predicate.Predicate;
import org.apache.pinot.core.query.request.context.predicate.RangePredicate;
import org.apache.pinot.core.segment.creator.impl.geospatial.H3IndexResolution;
import org.apache.pinot.core.segment.index.readers.H3IndexReader;
import org.apache.pinot.core.segment.index.readers.geospatial.ImmutableH3IndexReader;
import org.apache.pinot.spi.utils.BytesUtils;
import org.locationtech.jts.geom.Geometry;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * A filter operator that uses H3 index for geospatial data retrieval
 */
public class H3IndexFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "H3IndexFilterOperator";

  private Predicate _predicate;
  private final DataSource _dataSource;
  private final int _numDocs;
  private final H3Core _h3Core;
  private final IndexSegment _segment;
  private final H3IndexReader _h3IndexReader;
  private final H3IndexResolution _resolution;
  private Geometry _geometry;
  private double _distance;

  public H3IndexFilterOperator(Predicate predicate, IndexSegment indexSegment, int numDocs) {
    _predicate = predicate;
    _segment = indexSegment;
    FunctionContext function = predicate.getLhs().getFunction();
    String columnName;

    // TODO: handle composite function that contains ST_DISTANCE
    if (function.getArguments().get(0).getType() == ExpressionContext.Type.IDENTIFIER) {
      columnName = function.getArguments().get(0).getIdentifier();
      byte[] bytes = BytesUtils.toBytes(function.getArguments().get(1).getLiteral());
      _geometry = GeometrySerializer.deserialize(bytes);
    } else if (function.getArguments().get(1).getType() == ExpressionContext.Type.IDENTIFIER) {
      columnName = function.getArguments().get(1).getIdentifier();
      byte[] bytes = BytesUtils.toBytes(function.getArguments().get(0).getLiteral());
      _geometry = GeometrySerializer.deserialize(bytes);
    } else {
      throw new RuntimeException("Expecting one of the arguments of ST_DISTANCE to be an identifier");
    }
    _dataSource = indexSegment.getDataSource(columnName);
    _h3IndexReader = _dataSource.getH3Index();
    _resolution = _h3IndexReader.getH3IndexResolution();
    switch (predicate.getType()) {
      case RANGE:
        RangePredicate rangePredicate = (RangePredicate) predicate;
        _distance = Double.parseDouble(rangePredicate.getUpperBound());
        break;
      default:
        throw new RuntimeException(String.format("H3 index does not support predicate type %s", predicate.getType()));
    }
    _numDocs = numDocs;
    try {
      _h3Core = H3Core.newInstance();
    } catch (IOException e) {
      throw new RuntimeException("Unable to instantiate H3 instance", e);
    }
  }

  @Override
  protected FilterBlock getNextBlock() {
    int resolution = _resolution.getLowestResolution();
    long h3Id = _h3Core.geoToH3(_geometry.getCoordinate().x, _geometry.getCoordinate().y, resolution);
    assert _h3IndexReader != null;

    // find the number of rings based on distance for full match
    // use the edge of the hexagon to determine the rings are within the distance. This is calculated by (1) divide the
    // distance by edge length of the solution to get the number of contained rings (2) use the (floor of number - 1)
    // for fetching the rings since ring0 is the original hexagon
    double edgeLength = _h3Core.edgeLength(resolution, LengthUnit.m);
    int numFullMatchedRings = (int) Math.floor(_distance / edgeLength);
    MutableRoaringBitmap fullMatchedDocIds = new MutableRoaringBitmap();
    List<Long> fullMatchRings = new ArrayList<>();
    if (numFullMatchedRings > 0) {
      fullMatchRings = _h3Core.kRing(h3Id, numFullMatchedRings - 1);
      for (long id : fullMatchRings) {
        ImmutableRoaringBitmap docIds = _h3IndexReader.getDocIds(id);
        fullMatchedDocIds.or(docIds);
      }
    }

    // partial matchedRings
    // use the previous number + 1 to get the partial ring, which is the ceiling of the number
    int numPartialMatchedRings = numFullMatchedRings + 1;
    List<Long> partialMatchedRings = _h3Core.kRing(h3Id, numPartialMatchedRings - 1);
    partialMatchedRings.removeAll(fullMatchRings);
    final MutableRoaringBitmap partialMatchDocIds = new MutableRoaringBitmap();
    for (long id : partialMatchedRings) {
      ImmutableRoaringBitmap docIds = _h3IndexReader.getDocIds(id);
      partialMatchDocIds.or(docIds);
    }

    ExpressionFilterOperator expressionFilterOperator = new ExpressionFilterOperator(_segment, _predicate, _numDocs);
    FilterBlockDocIdSet filterBlockDocIdSet = expressionFilterOperator.getNextBlock().getBlockDocIdSet();
    MutableRoaringBitmap filteredPartialMatchDocIds =
        ((ScanBasedDocIdIterator) filterBlockDocIdSet.iterator()).applyAnd(partialMatchDocIds);

    MutableRoaringBitmap result = ImmutableRoaringBitmap.or(fullMatchedDocIds, filteredPartialMatchDocIds);
    return new FilterBlock(new BitmapDocIdSet(result, _numDocs) {

      // Override this method to reflect the entries scanned
      @Override
      public long getNumEntriesScannedInFilter() {
        return partialMatchDocIds.getCardinality();
      }
    });
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
