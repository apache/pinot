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

import javax.annotation.Nullable;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public abstract class BaseColumnFilterOperator extends BaseFilterOperator {
  protected final QueryContext _queryContext;
  protected final DataSource _dataSource;

  protected BaseColumnFilterOperator(QueryContext queryContext, DataSource dataSource, int numDocs) {
    super(numDocs, queryContext.isNullHandlingEnabled());
    _queryContext = queryContext;
    _dataSource = dataSource;
  }

  protected abstract BlockDocIdSet getNextBlockWithoutNullHandling();

  @Override
  protected BlockDocIdSet getTrues() {
    if (_nullHandlingEnabled) {
      ImmutableRoaringBitmap nullBitmap = getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        return FilterOperatorUtils.excludeNulls(_queryContext, _numDocs, getNextBlockWithoutNullHandling(), nullBitmap);
      }
    }
    return getNextBlockWithoutNullHandling();
  }

  @Override
  protected BlockDocIdSet getNulls() {
    ImmutableRoaringBitmap nullBitmap = getNullBitmap();
    if (nullBitmap != null && !nullBitmap.isEmpty()) {
      return new BitmapDocIdSet(nullBitmap, _numDocs);
    } else {
      return EmptyDocIdSet.getInstance();
    }
  }

  @Nullable
  private ImmutableRoaringBitmap getNullBitmap() {
    NullValueVectorReader nullValueVector = _dataSource.getNullValueVector();
    if (nullValueVector != null) {
      return nullValueVector.getNullBitmap();
    } else {
      return null;
    }
  }
}
