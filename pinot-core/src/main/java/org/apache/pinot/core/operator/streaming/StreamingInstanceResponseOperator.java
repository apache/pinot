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
package org.apache.pinot.core.operator.streaming;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.InstanceResponseOperator;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.combine.BaseCombineOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;


public class StreamingInstanceResponseOperator extends InstanceResponseOperator {

  private final StreamObserver<Server.ServerResponse> _streamObserver;

  public StreamingInstanceResponseOperator(BaseCombineOperator<?> combinedOperator, List<IndexSegment> indexSegments,
      List<FetchContext> fetchContexts, StreamObserver<Server.ServerResponse> streamObserver,
      QueryContext queryContext) {
    super(combinedOperator, indexSegments, fetchContexts, queryContext);
    _streamObserver = streamObserver;
  }

  @Override
  protected InstanceResponseBlock getNextBlock() {
    prefetchAll();
    BaseResultsBlock combinedResult;
    try {
      _combineOperator.start();
      combinedResult = _combineOperator.nextBlock();
      while (!(combinedResult instanceof MetadataResultsBlock)) {
        if (combinedResult instanceof ExceptionResultsBlock) {
          return new InstanceResponseBlock(combinedResult, _queryContext);
        } else {
          sendBlock(combinedResult);
        }
        combinedResult = _combineOperator.nextBlock();
      }
    } catch (IOException e) {
      InstanceResponseBlock exceptionResultBlock = new InstanceResponseBlock();
      exceptionResultBlock.addException(
          QueryException.getException(QueryException.DATA_TABLE_SERIALIZATION_ERROR, e));
      return exceptionResultBlock;
    } finally {
      _combineOperator.stop();
      releaseAll();
    }
    // return a metadata-only block.
    return new InstanceResponseBlock(combinedResult, _queryContext);
  }

  private void sendBlock(BaseResultsBlock baseResultBlock)
      throws IOException {
    DataSchema dataSchema = baseResultBlock.getDataSchema(_queryContext);
    Collection<Object[]> rows = baseResultBlock.getRows(_queryContext);
    Preconditions.checkState(dataSchema != null && rows != null, "Malformed data block");
    DataTable dataTable = baseResultBlock.getDataTable(_queryContext);
    _streamObserver.onNext(StreamingResponseUtils.getDataResponse(dataTable));
  }
}
