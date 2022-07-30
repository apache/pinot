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
package org.apache.pinot.core.operator;

import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.combine.BaseCombineOperator;
import org.apache.pinot.core.operator.streaming.StreamingResponseUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;


public class StreamingInstanceResponseOperator extends InstanceResponseOperator {

  private final StreamObserver<Server.ServerResponse> _streamObserver;

  public StreamingInstanceResponseOperator(BaseCombineOperator combinedOperator, List<IndexSegment> indexSegments,
      List<FetchContext> fetchContexts, StreamObserver<Server.ServerResponse> streamObserver,
      QueryContext queryContext, ServerMetrics serverMetrics) {
    super(combinedOperator, indexSegments, fetchContexts, queryContext, serverMetrics);
    _streamObserver = streamObserver;
  }

  @Override
  protected InstanceResponseBlock getNextBlock() {
    InstanceResponseBlock nextBlock = super.getNextBlock();
    DataTable instanceResponseDataTable = nextBlock.getInstanceResponseDataTable();
    DataTable metadataOnlyDataTable;
    try {
      metadataOnlyDataTable = instanceResponseDataTable.toMetadataOnlyDataTable();
      _streamObserver.onNext(StreamingResponseUtils.getDataResponse(
          instanceResponseDataTable.toDataOnlyDataTable()));
    } catch (IOException e) {
      // when exception occurs in streaming, we return an error-only metadata block.
      metadataOnlyDataTable = DataTableFactory.getEmptyDataTable();
      metadataOnlyDataTable.addException(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
    }
    // return a metadata-only block.
    return new InstanceResponseBlock(metadataOnlyDataTable);
  }
}
