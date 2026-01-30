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
package org.apache.pinot.core.operator.blocks.results;

import java.io.IOException;
import java.util.List;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * Results block for distinct queries.
 */
public class DistinctResultsBlock extends BaseResultsBlock {
  private final QueryContext _queryContext;

  private DistinctTable _distinctTable;
  private boolean _liteLeafLimitReached;
  private String _leafTruncationReason;

  public DistinctResultsBlock(DistinctTable distinctTable, QueryContext queryContext) {
    _distinctTable = distinctTable;
    _queryContext = queryContext;
  }

  public DistinctTable getDistinctTable() {
    return _distinctTable;
  }

  public void setDistinctTable(DistinctTable distinctTable) {
    _distinctTable = distinctTable;
  }

  public void setLiteLeafLimitReached(boolean liteLeafLimitReached) {
    _liteLeafLimitReached = liteLeafLimitReached;
  }

  public void setLeafTruncationReason(String reason) {
    _leafTruncationReason = reason;
  }

  @Override
  public int getNumRows() {
    return _distinctTable.size();
  }

  @Override
  public QueryContext getQueryContext() {
    return _queryContext;
  }

  @Override
  public DataSchema getDataSchema() {
    return _distinctTable.getDataSchema();
  }

  @Override
  public List<Object[]> getRows() {
    return _distinctTable.getRows();
  }

  @Override
  public DataTable getDataTable()
      throws IOException {
    return _distinctTable.toDataTable();
  }

  @Override
  public java.util.Map<String, String> getResultsMetadata() {
    java.util.Map<String, String> metadata = super.getResultsMetadata();
    if (_liteLeafLimitReached && "LITE_CAP".equals(_leafTruncationReason)) {
      metadata.put(DataTable.MetadataKey.LITE_LEAF_CAP_TRUNCATION.getName(), "true");
    }
    if (_leafTruncationReason != null) {
      metadata.put(DataTable.MetadataKey.LEAF_TRUNCATION_REASON.getName(), _leafTruncationReason);
    }
    return metadata;
  }
}
