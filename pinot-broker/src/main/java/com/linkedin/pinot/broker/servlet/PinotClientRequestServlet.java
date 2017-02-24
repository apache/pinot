/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.broker.requesthandler.BrokerRequestHandler;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.response.BrokerResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
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
    resp.setCharacterEncoding("UTF-8");
    PrintWriter writer = resp.getWriter();
    try {
      BrokerResponse brokerResponse = broker.handleRequest(new JSONObject(req.getParameter("bql")));
      String jsonString = brokerResponse.toJsonString();
      writer.print(jsonString);
      writer.flush();
      writer.close();
    } catch (final Exception e) {
      writer.print(e.getMessage());
      writer.flush();
      writer.close();
      LOGGER.error("Caught exception while processing GET request", e);
      brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_GET_EXCEPTIONS, 1);
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    resp.setCharacterEncoding("UTF-8");
    PrintWriter writer = resp.getWriter();
    try {
      BrokerResponse brokerResponse = broker.handleRequest(extractJSON(req));
      String jsonString = brokerResponse.toJsonString();
      writer.print(jsonString);
      writer.flush();
      writer.close();
    } catch (final Exception e) {
      writer.print(e.getMessage());
      writer.flush();
      writer.close();
      LOGGER.error("Caught exception while processing POST request", e);
      brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_POST_EXCEPTIONS, 1);
    }
  }

  private JSONObject extractJSON(HttpServletRequest req) throws IOException, JSONException {
    req.setCharacterEncoding("UTF-8");
    final StringBuilder requestStr = new StringBuilder();
    String line;
    final BufferedReader reader = req.getReader();
    while ((line = reader.readLine()) != null) {
      requestStr.append(line);
    }
    return new JSONObject(requestStr.toString());
  }
}
