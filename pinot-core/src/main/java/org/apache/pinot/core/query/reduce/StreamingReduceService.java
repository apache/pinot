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
package org.apache.pinot.core.query.reduce;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>StreamingReduceService</code> class provides service to reduce grpc response gathered from multiple servers
 * to {@link BrokerResponseNative}.
 */
@ThreadSafe
public class StreamingReduceService extends BaseReduceService {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingReduceService.class);

  public StreamingReduceService(PinotConfiguration config) {
    super(config);
  }

  public BrokerResponseNative reduceOnStreamResponse(BrokerRequest brokerRequest,
      Map<ServerRoutingInstance, Iterator<Server.ServerResponse>> serverResponseMap, long reduceTimeOutMs,
      @Nullable BrokerMetrics brokerMetrics)
      throws IOException {
    if (serverResponseMap.isEmpty()) {
      // Empty response.
      return BrokerResponseNative.empty();
    }

    // prepare contextual info for reduce.
    PinotQuery pinotQuery = brokerRequest.getPinotQuery();
    Map<String, String> queryOptions = pinotQuery.getQueryOptions();
    boolean enableTrace =
        queryOptions != null && Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.TRACE));

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    String tableName = queryContext.getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);

    // initialize empty response.
    ExecutionStatsAggregator aggregator = new ExecutionStatsAggregator(enableTrace);

    // Process server response.
    DataTableReducerContext dataTableReducerContext =
        new DataTableReducerContext(_reduceExecutorService, _maxReduceThreadsPerQuery, reduceTimeOutMs,
            _groupByTrimThreshold, _minGroupTrimSize);
    StreamingReducer streamingReducer = ResultReducerFactory.getStreamingReducer(queryContext);

    streamingReducer.init(dataTableReducerContext);

    try {
      processIterativeServerResponse(streamingReducer, _reduceExecutorService, serverResponseMap, reduceTimeOutMs,
          aggregator);
    } catch (Exception e) {
      LOGGER.error("Unable to process streaming query response!", e);
      throw new IOException("Unable to process streaming query response!", e);
    }

    // seal the streaming response.
    BrokerResponseNative brokerResponseNative = streamingReducer.seal();

    // Set execution statistics and Update broker metrics.
    aggregator.setStats(rawTableName, brokerResponseNative, brokerMetrics);

    updateAlias(queryContext, brokerResponseNative);
    return brokerResponseNative;
  }

  @VisibleForTesting
  static void processIterativeServerResponse(StreamingReducer reducer, ExecutorService executorService,
      Map<ServerRoutingInstance, Iterator<Server.ServerResponse>> serverResponseMap, long reduceTimeOutMs,
      ExecutionStatsAggregator aggregator)
      throws Exception {
    int cnt = 0;
    CompletableFuture<Void>[] futures = new CompletableFuture[serverResponseMap.size()];
    // based on ideas from on https://stackoverflow.com/questions/19348248/waiting-on-a-list-of-future
    // and https://stackoverflow.com/questions/23301598/transform-java-future-into-a-completablefuture
    // Future created via ExecutorService.submit() can be created by CompletableFuture.supplyAsync()
    for (Map.Entry<ServerRoutingInstance, Iterator<Server.ServerResponse>> entry : serverResponseMap.entrySet()) {
      futures[cnt++] = CompletableFuture.runAsync(() -> {
        Iterator<Server.ServerResponse> streamingResponses = entry.getValue();
        try {
          while (streamingResponses.hasNext()) {
            Server.ServerResponse streamingResponse = streamingResponses.next();
            DataTable dataTable = DataTableFactory.getDataTable(streamingResponse.getPayload().asReadOnlyByteBuffer());
            // null dataSchema is a metadata-only block.
            if (dataTable.getDataSchema() != null) {
              reducer.reduce(entry.getKey(), dataTable);
            } else {
              aggregator.aggregate(entry.getKey(), dataTable);
            }
          }
        } catch (Exception e) {
          LOGGER.error("Unable to process streaming response. Failure occurred!", e);
          throw new RuntimeException("Unable to process streaming response. Failure occurred!", e);
        }
      }, executorService);
    }
    CompletableFuture<Void> syncWaitPoint = CompletableFuture.allOf(futures);
    try {
      syncWaitPoint.get(reduceTimeOutMs, TimeUnit.MILLISECONDS);
    } catch (Exception ex) {
      syncWaitPoint.cancel(true);
      throw ex;
    }
  }

  public void shutDown() {
    _reduceExecutorService.shutdownNow();
  }
}
