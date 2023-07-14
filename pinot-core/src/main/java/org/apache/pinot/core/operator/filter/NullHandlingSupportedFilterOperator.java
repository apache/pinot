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
import org.apache.pinot.core.operator.docidsets.MatchAllDocIdSet;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;


public abstract class NullHandlingSupportedFilterOperator extends BaseFilterOperator {
  protected final QueryContext _queryContext;
  protected final DataSource _dataSource;
  protected final int _numDocs;

  protected NullHandlingSupportedFilterOperator(QueryContext queryContext, DataSource dataSource, int numDocs) {
    _queryContext = queryContext;
    _dataSource = dataSource;
    _numDocs = numDocs;
  }

  @Override
  protected FilterBlock getNextBlock() {
    if (_queryContext.isNullHandlingEnabled()) {
      return new FilterBlock(excludeNulls(getNextBlockWithoutNullHandling()));
    } else {
      return new FilterBlock(getNextBlockWithoutNullHandling());
    }
  }

  protected abstract BlockDocIdSet getNextBlockWithoutNullHandling();

  private BlockDocIdSet excludeNulls(BlockDocIdSet blockDocIdSet) {
    return new AndDocIdSet(
        Arrays.asList(blockDocIdSet, fromNonNullBitmap()),
        _queryContext.getQueryOptions());
  }

  private BlockDocIdSet fromNonNullBitmap() {
    NullValueVectorReader nullValueVector = _dataSource.getNullValueVector();
    if (nullValueVector != null) {
      return new BitmapDocIdSet(nullValueVector.getNullBitmap(), _numDocs, true);
    } else {
      return new MatchAllDocIdSet(_numDocs);
    }
  }
}
