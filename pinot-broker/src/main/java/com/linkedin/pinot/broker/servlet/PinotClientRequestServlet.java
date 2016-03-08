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
package com.linkedin.pinot.broker.servlet;

import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.metrics.BrokerQueryPhase;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.BrokerResponseFactory;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import com.linkedin.pinot.requestHandler.BrokerRequestHandler;
import com.linkedin.pinot.transport.common.BucketingSelection;
import com.linkedin.pinot.transport.common.SegmentId;
import com.linkedin.pinot.transport.scattergather.ScatterGatherStats;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotClientRequestServlet extends HttpServlet {
  private static final Pql2Compiler REQUEST_COMPILER = new Pql2Compiler();

  private static final long serialVersionUID = -3516093545255816357L;
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotClientRequestServlet.class);

  private BrokerRequestHandler broker;
  private BrokerMetrics brokerMetrics;
  private AtomicLong requestIdGenerator;

  @Override
  public void init(ServletConfig config)
      throws ServletException {
    broker = (BrokerRequestHandler) config.getServletContext().getAttribute(BrokerRequestHandler.class.toString());
    brokerMetrics = (BrokerMetrics) config.getServletContext().getAttribute(BrokerMetrics.class.toString());
    requestIdGenerator = new AtomicLong(0);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    try {
      resp.setCharacterEncoding("UTF-8");
      resp.getOutputStream().print(handleRequest(new JSONObject(req.getParameter("bql"))).toJsonString());
      resp.getOutputStream().flush();
      resp.getOutputStream().close();
    } catch (final Exception e) {
      resp.getOutputStream().print(e.getMessage());
      resp.getOutputStream().flush();
      resp.getOutputStream().close();
      LOGGER.error("Caught exception while processing GET request", e);
      brokerMetrics.addMeteredValue(null, BrokerMeter.UNCAUGHT_GET_EXCEPTIONS, 1);
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    try {
      resp.setCharacterEncoding("UTF-8");
      resp.getOutputStream().print(handleRequest(extractJSON(req)).toJsonString());
      resp.getOutputStream().flush();
      resp.getOutputStream().close();
    } catch (final Exception e) {
      resp.getOutputStream().print(e.getMessage());
      resp.getOutputStream().flush();
      resp.getOutputStream().close();
      LOGGER.error("Caught exception while processing POST request", e);
      brokerMetrics.addMeteredValue(null, BrokerMeter.UNCAUGHT_POST_EXCEPTIONS, 1);
    }
  }

  private BrokerResponse handleRequest(JSONObject request)
      throws Exception {
    final String pql = request.getString("pql");
    final long requestId = requestIdGenerator.incrementAndGet();
    LOGGER.info("Query string for requestId {}: {}", requestId, pql);
    boolean isTraceEnabled = false;

    final BrokerResponseFactory.ResponseType responseType =
        BrokerResponseFactory.getResponseType(request);

    if (request.has("trace")) {
      try {
        isTraceEnabled = Boolean.parseBoolean(request.getString("trace"));
        LOGGER.info("Trace is set to: {}", isTraceEnabled);
      } catch (Exception e) {
        LOGGER.warn("Invalid trace value: {}", request.getString("trace"), e);
      }
    } else {
      // ignore, trace is disabled by default
    }

    final long startTime = System.nanoTime();
    final BrokerRequest brokerRequest;
    try {
      brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(pql);
      if (isTraceEnabled) {
        brokerRequest.setEnableTrace(true);
      }
    } catch (Exception e) {
      BrokerResponse brokerResponse = BrokerResponseFactory.get(responseType);
      brokerResponse.setExceptions(Arrays.asList(QueryException.getException(QueryException.PQL_PARSING_ERROR, e)));
      brokerMetrics.addMeteredValue(null, BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
      return brokerResponse;
    }

    brokerMetrics.addMeteredValue(brokerRequest, BrokerMeter.QUERIES, 1);

    final long requestCompilationTime = System.nanoTime() - startTime;
    brokerMetrics.addPhaseTiming(brokerRequest, BrokerQueryPhase.REQUEST_COMPILATION, requestCompilationTime);
    final ScatterGatherStats scatterGatherStats = new ScatterGatherStats();

    final BrokerResponse resp =
        brokerMetrics.timePhase(brokerRequest, BrokerQueryPhase.QUERY_EXECUTION, new Callable<BrokerResponse>() {
              @Override
              public BrokerResponse call()
                  throws Exception {
                final BucketingSelection bucketingSelection = getBucketingSelection(brokerRequest);
                return (BrokerResponse) broker
                    .processBrokerRequest(brokerRequest, responseType, bucketingSelection, scatterGatherStats,
                        requestId);
              }
            });

    // Query processing time is the total time spent in all steps include query parsing, scatter/gather, ser/de etc.
    long queryProcessingTimeInNanos = System.nanoTime() - startTime;
    long queryProcessingTimeInMillis = TimeUnit.MILLISECONDS.convert(queryProcessingTimeInNanos, TimeUnit.NANOSECONDS);
    resp.setTimeUsedMs(queryProcessingTimeInMillis);

    LOGGER.info("BrokerResponse type : {}", responseType);
    LOGGER.debug("Broker Response : {}", resp);
    LOGGER.info("ResponseTimes for {} {}", requestId, scatterGatherStats);
    LOGGER.info("Total query processing time : {} ms", queryProcessingTimeInMillis);

    return resp;
  }

  private BucketingSelection getBucketingSelection(BrokerRequest brokerRequest) {
    final Map<SegmentId, ServerInstance> bucketMap = new HashMap<>();
    return new BucketingSelection(bucketMap);
  }

  private JSONObject extractJSON(HttpServletRequest req)
      throws IOException, JSONException {
    final StringBuilder requestStr = new StringBuilder();
    String line;
    final BufferedReader reader = req.getReader();
    while ((line = reader.readLine()) != null) {
      requestStr.append(line);
    }
    return new JSONObject(requestStr.toString());
  }
}
