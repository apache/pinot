/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.server.request;

import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.metrics.ServerQueryPhase;
import com.linkedin.pinot.common.request.BrokerRequest;
import io.netty.buffer.ByteBuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.Callable;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.response.InstanceResponse;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.DataTableBuilder;
import com.linkedin.pinot.serde.SerDe;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandler;


/**
 * A simple implementation of RequestHandler.
 *
 * @author xiafu
 *
 */
public class SimpleRequestHandler implements RequestHandler {

  private static Logger LOGGER = LoggerFactory.getLogger(SimpleRequestHandler.class);

  private ServerMetrics _serverMetrics;

  QueryExecutor _queryExecutor = null;

  public SimpleRequestHandler(QueryExecutor queryExecutor, ServerMetrics serverMetrics) {
    _queryExecutor = queryExecutor;
    _serverMetrics = serverMetrics;
  }

  @Override
  public byte[] processRequest(ByteBuf request) {
    _serverMetrics.addMeteredValue(null, ServerMeter.QUERIES, 1);

    LOGGER.debug("processing request : " + request);

    DataTable instanceResponse = null;
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    byte[] byteArray = new byte[request.readableBytes()];
    request.readBytes(byteArray);
    SerDe serDe = new SerDe(new TCompactProtocol.Factory());
    try {
      final InstanceRequest queryRequest = new InstanceRequest();
      serDe.deserialize(queryRequest, byteArray);
      LOGGER.info("instance request : " + queryRequest);

      final BrokerRequest brokerRequest = queryRequest.getQuery();

      instanceResponse = _serverMetrics.timePhase(queryRequest.getQuery(), ServerQueryPhase.TOTAL_QUERY_TIME, new Callable<DataTable>() {
        @Override
        public DataTable call() throws Exception {
          return _queryExecutor.processQuery(queryRequest);
        }
      });
      LOGGER.debug("******************************");
      if (instanceResponse != null) {
        LOGGER.debug("instanceResponse : " + instanceResponse.toString());
      } else {
        LOGGER.debug("instanceResponse : null");
      }
      LOGGER.debug("******************************");
    } catch (Exception e) {
      LOGGER.error("Got exception while processing request. Returning error response", e);
      _serverMetrics.addMeteredValue(null, ServerMeter.UNCAUGHT_EXCEPTIONS, 1);
      DataTableBuilder dataTableBuilder = new DataTableBuilder(null);
      List<ProcessingException> exceptions = new ArrayList<ProcessingException>();
      ProcessingException exception = QueryException.INTERNAL_ERROR.deepCopy();
      exception.setMessage(e.getMessage());
      exceptions.add(exception);
      instanceResponse = dataTableBuilder.buildExceptions();
    }
    try {
      if (instanceResponse == null) {
        return new byte[0];
      } else {
        return instanceResponse.toBytes();
      }
    } catch (Exception e) {
      _serverMetrics.addMeteredValue(null, ServerMeter.RESPONSE_SERIALIZATION_EXCEPTIONS, 1);
      LOGGER.error("Got exception while serializing response.", e);
      return null;
    }
  }

}
