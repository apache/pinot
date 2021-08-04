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

import java.util.*;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.geospatial.transform.function.ScalarFunctions;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.reader.H3IndexReader;
import org.apache.pinot.spi.utils.BytesUtils;
import org.locationtech.jts.geom.Coordinate;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * A filter operator that uses H3 index for geospatial data inclusion
 */
public class H3InclusionIndexFilterOperator extends BaseFilterOperator {
    private static final String OPERATOR_NAME = "H3IndexFilterOperator";

    private final IndexSegment _segment;
    private final Predicate _predicate;
    private final int _numDocs;
    private final H3IndexReader _h3IndexReader;
    private final List<Long> _h3Ids;

    public H3InclusionIndexFilterOperator(IndexSegment segment, Predicate predicate, int numDocs) {
        _segment = segment;
        _predicate = predicate;
        _numDocs = numDocs;

        List<ExpressionContext> arguments = predicate.getLhs().getFunction().getArguments();
        Coordinate[] coordinates;
        if (arguments.get(0).getType() == ExpressionContext.Type.IDENTIFIER) {
            // look up arg0's h3 indices
            _h3IndexReader = segment.getDataSource(arguments.get(0).getIdentifier()).getH3Index();
            // arg1 is the literal
            coordinates = GeometrySerializer.deserialize(BytesUtils.toBytes(arguments.get(1).getLiteral())).getCoordinates();
        } else {
            // look up arg1's h3 indices
            _h3IndexReader = segment.getDataSource(arguments.get(1).getIdentifier()).getH3Index();
            // arg0 is the literal
            coordinates = GeometrySerializer.deserialize(BytesUtils.toBytes(arguments.get(0).getLiteral())).getCoordinates();
        }
        // must be some h3 index
        assert _h3IndexReader != null;

        // look up all hexagons for provided coordinates
        List<Coordinate> coordinateList = Arrays.asList(coordinates);
        _h3Ids = ScalarFunctions.polygonToH3(coordinateList,
                ScalarFunctions.calcResFromMaxDist(ScalarFunctions.maxDist(coordinateList), 50));
    }

    @Override
    protected FilterBlock getNextBlock() {
        // have list of h3 hashes for polygon provided
        // return filtered num_docs
        MutableRoaringBitmap fullMatchDocIds = new MutableRoaringBitmap();
        for (long docId : _h3Ids) {
            fullMatchDocIds.or(_h3IndexReader.getDocIds(docId));
        }
        fullMatchDocIds.flip(0L, _numDocs);

        // when h3 implements polyfill parameters for including partial matches, we can expand to include partial matches
        return getFilterBlock(fullMatchDocIds, new MutableRoaringBitmap());
    }

    /**
     * Returns the filter block based on the given full match doc ids and the partial match doc ids.
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
    public String getOperatorName() {
        return OPERATOR_NAME;
    }
}
