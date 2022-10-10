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

import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.core.operator.InstanceResponseOperator;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
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
    InstanceResponseBlock responseBlock = super.getNextBlock();
    InstanceResponseBlock metadataOnlyResponseBlock = responseBlock.toMetadataOnlyResponseBlock();
    try {
      _streamObserver.onNext(StreamingResponseUtils.getDataResponse(responseBlock.toDataOnlyDataTable()));
    } catch (IOException e) {
      metadataOnlyResponseBlock.addException(
          QueryException.getException(QueryException.DATA_TABLE_SERIALIZATION_ERROR, e));
    }
    // return a metadata-only block.
    return metadataOnlyResponseBlock;
  }
}
