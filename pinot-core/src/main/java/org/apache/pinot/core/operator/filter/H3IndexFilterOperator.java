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
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.query.request.context.predicate.GeoPredicate;
import org.apache.pinot.core.segment.index.readers.geospatial.H3IndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class H3IndexFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "H3IndexFilterOperator";

  // NOTE: Range index can only apply to dictionary-encoded columns for now
  // TODO: Support raw index columns
  private final GeoPredicate _geoPredicate;
  private final DataSource _dataSource;
  private final int _numDocs;
  private final H3Core _h3Core;

  public H3IndexFilterOperator(GeoPredicate geoPredicate, DataSource dataSource, int numDocs) {
    _geoPredicate = geoPredicate;
    _dataSource = dataSource;
    _numDocs = numDocs;
    try {
      _h3Core = H3Core.newInstance();
    } catch (IOException e) {
      throw new RuntimeException("Unable to instantiate H3", e);      //todo:log error
    }
  }

  @Override
  protected FilterBlock getNextBlock() {
    H3IndexReader h3IndexReader = _dataSource.getH3Index();
    //todo: this needs to come from somewhere?
    int resolution = 5;
    long h3Id = _h3Core
        .geoToH3(_geoPredicate.getGeometry().getCoordinate().x, _geoPredicate.getGeometry().getCoordinate().y,
            resolution);
    assert h3IndexReader != null;

    //find the number of rings based on geopredicate.distance
    //FullMatch
    double edgeLength = _h3Core.edgeLength(resolution, LengthUnit.km);
    int numFullMatchedRings = (int) (_geoPredicate.getDistance() / edgeLength);
    List<Long> fullMatchRings = _h3Core.kRing(h3Id, numFullMatchedRings);
    fullMatchRings.add(h3Id);
    MutableRoaringBitmap fullMatchedDocIds = new MutableRoaringBitmap();
    for (long id : fullMatchRings) {
      ImmutableRoaringBitmap docIds = h3IndexReader.getDocIds(id);
      fullMatchedDocIds.or(docIds);
    }

    //partial matchedRings
    int numPartialMatchedRings = (int) (_geoPredicate.getDistance() / edgeLength);
    List<Long> partialMatchedRings = _h3Core.kRing(h3Id, numPartialMatchedRings);
    partialMatchedRings.add(h3Id);
    final MutableRoaringBitmap partialMatchDocIds = new MutableRoaringBitmap();
    partialMatchedRings.removeAll(fullMatchRings);
    for (long id : partialMatchedRings) {
      ImmutableRoaringBitmap docIds = h3IndexReader.getDocIds(id);
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
