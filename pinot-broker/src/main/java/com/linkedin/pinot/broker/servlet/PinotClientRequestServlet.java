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

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.metrics.BrokerQueryPhase;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.pql.parsers.PQLCompiler;
import com.linkedin.pinot.requestHandler.BrokerRequestHandler;
import com.linkedin.pinot.transport.common.BucketingSelection;
import com.linkedin.pinot.transport.common.SegmentId;


public class PinotClientRequestServlet extends HttpServlet {
  private static final PQLCompiler requestCompiler = new PQLCompiler(new HashMap<String, String[]>());

  private static final long serialVersionUID = -3516093545255816357L;
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotClientRequestServlet.class);

  private BrokerRequestHandler broker;
  private BrokerMetrics brokerMetrics;

  @Override
  public void init(ServletConfig config) throws ServletException {
    broker = (BrokerRequestHandler) config.getServletContext().getAttribute(BrokerRequestHandler.class.toString());
    brokerMetrics = (BrokerMetrics) config.getServletContext().getAttribute(BrokerMetrics.class.toString());
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    try {
      resp.getOutputStream().print(handleRequest(new JSONObject(req.getParameter("bql"))).toJson().toString());
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
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    try {
      resp.getOutputStream().print(handleRequest(extractJSON(req)).toJson().toString());
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

  private BrokerResponse handleRequest(JSONObject request) throws Exception {
    final String pql = request.getString("pql");
    boolean isTraceEnabled = false;

    if(request.has("trace")){
      try {
        isTraceEnabled = Boolean.parseBoolean(request.getString("trace"));
        LOGGER.info("Trace is set to: "+ isTraceEnabled);
      } catch (Exception e) {
        LOGGER.warn("Invalid trace value: {}", request.getString("trace"), e);
      }
    } else {
      LOGGER.warn("Request does not contain a key called \"trace\", so trace is disabled.");
    }

    final long startTime = System.nanoTime();
    final BrokerRequest brokerRequest;
    try {
      final JSONObject compiled = requestCompiler.compile(pql);
      brokerRequest = convertToBrokerRequest(compiled);
      if (isTraceEnabled) brokerRequest.setEnableTrace(true);
    } catch (Exception e) {
      BrokerResponse brokerResponse = new BrokerResponse();
      brokerResponse.setExceptions(Arrays.asList(QueryException.getException(QueryException.PQL_PARSING_ERROR, e)));
      brokerMetrics.addMeteredValue(null, BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
      return brokerResponse;
    }

    brokerMetrics.addMeteredValue(brokerRequest, BrokerMeter.QUERIES, 1);

    final long requestCompilationTime = System.nanoTime() - startTime;
    brokerMetrics.addPhaseTiming(brokerRequest, BrokerQueryPhase.REQUEST_COMPILATION,
            requestCompilationTime);

    final BrokerResponse resp = brokerMetrics.timePhase(brokerRequest, BrokerQueryPhase.QUERY_EXECUTION,
            new Callable<BrokerResponse>() {
              @Override
              public BrokerResponse call()
                      throws Exception {
                final BucketingSelection bucketingSelection = getBucketingSelection(brokerRequest);
                return (BrokerResponse) broker.processBrokerRequest(brokerRequest, bucketingSelection);
              }
            });

    LOGGER.info("Broker Response : " + resp);
    return resp;
  }

  private BucketingSelection getBucketingSelection(BrokerRequest brokerRequest) {
    final Map<SegmentId, ServerInstance> bucketMap = new HashMap<SegmentId, ServerInstance>();
    return new BucketingSelection(bucketMap);
  }

  private BrokerRequest convertToBrokerRequest(JSONObject compiled) throws Exception {
    return com.linkedin.pinot.common.client.request.RequestConverter.fromJSON(compiled);
  }

  private JSONObject extractJSON(HttpServletRequest req) throws IOException, JSONException {
    final StringBuilder requestStr = new StringBuilder();
    String line;
    final BufferedReader reader = req.getReader();
    while ((line = reader.readLine()) != null) {
      requestStr.append(line);
    }
    return new JSONObject(requestStr.toString());
  }
}
