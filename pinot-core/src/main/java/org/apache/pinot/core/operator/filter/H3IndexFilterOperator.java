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
import java.util.List;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.geospatial.serde.GeometrySerializer;
import org.apache.pinot.core.geospatial.transform.function.StPointFunction;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.FunctionContext;
import org.apache.pinot.core.query.request.context.predicate.GeoPredicate;
import org.apache.pinot.core.query.request.context.predicate.Predicate;
import org.apache.pinot.core.query.request.context.predicate.RangePredicate;
import org.apache.pinot.core.segment.creator.impl.geospatial.H3IndexResolution;
import org.apache.pinot.core.segment.index.readers.geospatial.H3IndexReader;
import org.apache.pinot.spi.utils.BytesUtils;
import org.locationtech.jts.geom.Geometry;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * A filter operator that uses H3 index for geospatial data retrieval
 */
public class H3IndexFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "H3IndexFilterOperator";

  private final int _numDocs;
  private final H3Core _h3Core;
  private final H3IndexReader _h3IndexReader;
  private final H3IndexResolution _resolution;
  private Geometry _geometry;
  private double _distance;

  public H3IndexFilterOperator(Predicate predicate, IndexSegment indexSegment, int numDocs) {
    FunctionContext function = predicate.getLhs().getFunction();
    String columnName;

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
    DataSource dataSource = indexSegment.getDataSource(columnName);
    _h3IndexReader = dataSource.getH3Index();
    _resolution = _h3IndexReader.getH3IndexResolution();
    switch (predicate.getType()) {
      case EQ:
        break;
      case NOT_EQ:
        break;
      case IN:
        break;
      case NOT_IN:
        break;
      case RANGE:
        RangePredicate rangePredicate = (RangePredicate) predicate;
        _distance = Double.parseDouble(rangePredicate.getUpperBound());
        break;
      case REGEXP_LIKE:
        break;
      case TEXT_MATCH:
        break;
      case IS_NULL:
        break;
      case IS_NOT_NULL:
        break;
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

    //find the number of rings based on distance
    //FullMatch
    double edgeLength = _h3Core.edgeLength(resolution, LengthUnit.km);
    int numFullMatchedRings = (int) (_distance / edgeLength);
    List<Long> fullMatchRings = _h3Core.kRing(h3Id, numFullMatchedRings);
    fullMatchRings.add(h3Id);
    MutableRoaringBitmap fullMatchedDocIds = new MutableRoaringBitmap();
    for (long id : fullMatchRings) {
      ImmutableRoaringBitmap docIds = _h3IndexReader.getDocIds(id);
      fullMatchedDocIds.or(docIds);
    }

    //partial matchedRings
    int numPartialMatchedRings = (int) ((_distance + edgeLength) / edgeLength);
    List<Long> partialMatchedRings = _h3Core.kRing(h3Id, numPartialMatchedRings);
    partialMatchedRings.add(h3Id);
    final MutableRoaringBitmap partialMatchDocIds = new MutableRoaringBitmap();
    partialMatchedRings.removeAll(fullMatchRings);
    for (long id : partialMatchedRings) {
      ImmutableRoaringBitmap docIds = _h3IndexReader.getDocIds(id);
      partialMatchDocIds.or(docIds);
    }

    //TODO:evaluate the actual distance for the partial matched by scanning

    MutableRoaringBitmap result = ImmutableRoaringBitmap.or(fullMatchedDocIds, partialMatchDocIds);
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
