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

import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.BrokerRequestToQueryContextConverter;
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
      @Nullable BrokerMetrics brokerMetrics) {
    if (serverResponseMap.isEmpty()) {
      // Empty response.
      return BrokerResponseNative.empty();
    }

    // prepare contextual info for reduce.
    PinotQuery pinotQuery = brokerRequest.getPinotQuery();
    Map<String, String> queryOptions =
        pinotQuery != null ? pinotQuery.getQueryOptions() : brokerRequest.getQueryOptions();
    boolean enableTrace =
        queryOptions != null && Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.TRACE));

    QueryContext queryContext = BrokerRequestToQueryContextConverter.convert(brokerRequest);

    String tableName = brokerRequest.getQuerySource().getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);

    // initialize empty response.
    BrokerMetricsAggregator aggregator = new BrokerMetricsAggregator(enableTrace);

    // Process server response.
    DataTableReducerContext dataTableReducerContext =
        new DataTableReducerContext(_reduceExecutorService, _maxReduceThreadsPerQuery, reduceTimeOutMs,
            _groupByTrimThreshold);
    StreamingReducer streamingReducer = ResultReducerFactory.getStreamingReducer(queryContext);

    streamingReducer.init(dataTableReducerContext);

    // TODO: use concurrent process instead of determine-order for-loop.
    for (Map.Entry<ServerRoutingInstance, Iterator<Server.ServerResponse>> entry: serverResponseMap.entrySet()) {
      Iterator<Server.ServerResponse> streamingResponses = entry.getValue();
      while (streamingResponses.hasNext()) {
        Server.ServerResponse streamingResponse = streamingResponses.next();
        try {
          DataTable dataTable = DataTableFactory.getDataTable(streamingResponse.getPayload().asReadOnlyByteBuffer());
          streamingReducer.reduce(entry.getKey(), dataTable);
          aggregator.addMetrics(entry.getKey(), dataTable);
        } catch (Exception e) {
          // TODO: throw meaningful exception and handle exception correctly.
          LOGGER.error("Unable to parse streamingResponse, move on to the next.");
        }
      }
    }

    // seal the streaming response.
    BrokerResponseNative brokerResponseNative = streamingReducer.seal();

    // Set execution statistics and Update broker metrics.
    aggregator.setBrokerMetrics(rawTableName, brokerResponseNative, brokerMetrics);

    updateAlias(queryContext, brokerResponseNative);
    return brokerResponseNative;
  }

  public void shutDown() {
    _reduceExecutorService.shutdownNow();
  }
}
