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

import java.util.Collections;
import java.util.List;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public class BitmapBasedFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_BITMAP";

  private final ImmutableRoaringBitmap _docIds;
  private final boolean _exclusive;

  public BitmapBasedFilterOperator(ImmutableRoaringBitmap docIds, boolean exclusive, int numDocs) {
    super(numDocs, false);
    _docIds = docIds;
    _exclusive = exclusive;
  }

  @Override
  protected BlockDocIdSet getTrues() {
    if (_exclusive) {
      return new BitmapDocIdSet(ImmutableRoaringBitmap.flip(_docIds, 0L, _numDocs), _numDocs);
    } else {
      return new BitmapDocIdSet(_docIds, _numDocs);
    }
  }

  @Override
  public boolean canOptimizeCount() {
    return true;
  }

  @Override
  public int getNumMatchingDocs() {
    int count = _docIds.getCardinality();
    return _exclusive ? _numDocs - count : count;
  }

  @Override
  public boolean canProduceBitmaps() {
    return true;
  }

  @Override
  public BitmapCollection getBitmaps() {
    return new BitmapCollection(_numDocs, _exclusive, _docIds);
  }


  @Override
  @SuppressWarnings("rawtypes")
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }
}
