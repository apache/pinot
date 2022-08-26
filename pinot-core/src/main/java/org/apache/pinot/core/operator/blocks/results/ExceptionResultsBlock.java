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

import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.core.query.request.context.QueryContext;


public class ExceptionResultsBlock extends BaseResultsBlock {

  public ExceptionResultsBlock(ProcessingException processingException, Exception e) {
    addToProcessingExceptions(QueryException.getException(processingException, e));
  }

  public ExceptionResultsBlock(Exception e) {
    this(QueryException.QUERY_EXECUTION_ERROR, e);
  }

  @Override
  public DataTable getDataTable(QueryContext queryContext)
      throws Exception {
    DataTable dataTable = DataTableFactory.getEmptyDataTable();
    attachMetadataToDataTable(dataTable);
    return dataTable;
  }
}
