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

import java.util.Arrays;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.AndDocIdSet;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public abstract class SingleColumnFilterOperator extends BaseFilterOperator {
  protected final QueryContext _queryContext;
  protected final DataSource _dataSource;

  protected SingleColumnFilterOperator(QueryContext queryContext, DataSource dataSource, int numDocs) {
    super(numDocs, queryContext.isNullHandlingEnabled());
    _queryContext = queryContext;
    _dataSource = dataSource;
  }

  @Override
  protected FilterBlock getNextBlock() {
    if (_nullHandlingEnabled) {
      ImmutableRoaringBitmap nullBitmap = getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        return new FilterBlock(excludeNulls(getNextBlockWithoutNullHandling(), nullBitmap));
      }
    }
    return new FilterBlock(getNextBlockWithoutNullHandling());
  }

  protected abstract BlockDocIdSet getNextBlockWithoutNullHandling();

  @Override
  protected BlockDocIdSet getNulls() {
    return new BitmapDocIdSet(getNullBitmap(), _numDocs);
  }

  private BlockDocIdSet excludeNulls(BlockDocIdSet blockDocIdSet, ImmutableRoaringBitmap nullBitmap) {
    return new AndDocIdSet(Arrays.asList(blockDocIdSet,
        new BitmapDocIdSet(ImmutableRoaringBitmap.flip(nullBitmap, 0, (long) _numDocs), _numDocs)),
        _queryContext.getQueryOptions());
  }

  private ImmutableRoaringBitmap getNullBitmap() {
    NullValueVectorReader nullValueVector = _dataSource.getNullValueVector();
    if (nullValueVector != null) {
      return nullValueVector.getNullBitmap();
    } else {
      return null;
    }
  }
}
