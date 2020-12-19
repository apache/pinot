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

import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.query.request.context.predicate.GeoPredicate;
import org.apache.pinot.core.segment.index.readers.geospatial.H3IndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public class H3IndexFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "H3IndexFilterOperator";

  // NOTE: Range index can only apply to dictionary-encoded columns for now
  // TODO: Support raw index columns
  private final GeoPredicate _geoPredicate;
  private final DataSource _dataSource;
  private final int _numDocs;

  public H3IndexFilterOperator(GeoPredicate geoPredicate, DataSource dataSource, int numDocs) {
    _geoPredicate = geoPredicate;
    _dataSource = dataSource;
    _numDocs = numDocs;
  }

  @Override
  protected FilterBlock getNextBlock() {
    H3IndexReader h3IndexReader = (H3IndexReader) _dataSource.getRangeIndex();
    assert h3IndexReader != null;

    long h3Id = 1000;
    ImmutableRoaringBitmap docIds = h3IndexReader.getDocIds(h3Id);
    return new FilterBlock(new BitmapDocIdSet(docIds, _numDocs) {

      // Override this method to reflect the entries scanned
      @Override
      public long getNumEntriesScannedInFilter() {
        return 0; //TODO:Return the one from ScanBased
      }
    });
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
