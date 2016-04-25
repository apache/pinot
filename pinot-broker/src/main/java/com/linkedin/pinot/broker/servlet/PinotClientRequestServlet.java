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

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.broker.cache.BrokerCache;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.requestHandler.BrokerRequestHandler;


public class PinotClientRequestServlet extends HttpServlet {

  private static final long serialVersionUID = -3516093545255816357L;
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotClientRequestServlet.class);

  private BrokerRequestHandler broker;
  private BrokerMetrics brokerMetrics;
  private BrokerCache brokerCache;

  @Override
  public void init(ServletConfig config) throws ServletException {
    broker = (BrokerRequestHandler) config.getServletContext().getAttribute(BrokerRequestHandler.class.toString());
    brokerMetrics = (BrokerMetrics) config.getServletContext().getAttribute(BrokerMetrics.class.toString());
    brokerCache = (BrokerCache) config.getServletContext().getAttribute(BrokerCache.class.toString());
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    try {
      JSONObject jsonRequest = new JSONObject(req.getParameter("bql"));
      BrokerResponse brokerResponse = getBrokerResponse(jsonRequest);
      setBrokerResponse(resp, brokerResponse.toJsonString());
    } catch (final Exception e) {
      setErrorResponse(resp, e);
      LOGGER.error("Caught exception while processing GET request", e);
      brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_GET_EXCEPTIONS, 1);
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    try {
      JSONObject jsonRequest = extractJSON(req);
      BrokerResponse brokerResponse = getBrokerResponse(jsonRequest);
      setBrokerResponse(resp, brokerResponse.toJsonString());
    } catch (final Exception e) {
      setErrorResponse(resp, e);
      LOGGER.error("Caught exception while processing POST request", e);
      brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_POST_EXCEPTIONS, 1);
    }
  }

  private void setBrokerResponse(HttpServletResponse resp, String jsonString) throws IOException {
    resp.setCharacterEncoding("UTF-8");
    resp.getOutputStream().print(jsonString);
    resp.getOutputStream().flush();
    resp.getOutputStream().close();
  }

  private void setErrorResponse(HttpServletResponse resp, final Exception e) throws IOException {
    resp.setCharacterEncoding("UTF-8");
    resp.getOutputStream().print(e.getMessage());
    resp.getOutputStream().flush();
    resp.getOutputStream().close();
  }

  private BrokerResponse getBrokerResponse(JSONObject jsonRequest) throws Exception {
    BrokerResponse brokerResponse;
    if (brokerCache == null) {
      brokerResponse = this.broker.handleRequest(jsonRequest);
    } else {
      brokerResponse = this.brokerCache.handleRequest(jsonRequest);
    }
    return brokerResponse;
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
