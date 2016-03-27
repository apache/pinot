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

import java.io.IOException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.broker.broker.BrokerServerBuilder;
import com.linkedin.pinot.broker.broker.BrokerServerBuilder.State;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;

public class PinotBrokerHealthCheckServlet extends HttpServlet {

  private static final long serialVersionUID = -3516093545255816357L;
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotBrokerHealthCheckServlet.class);

  private BrokerServerBuilder brokerServerBuilder;
  private BrokerMetrics brokerMetrics;

  @Override
  public void init(ServletConfig config) throws ServletException {
    brokerServerBuilder = (BrokerServerBuilder) config.getServletContext().getAttribute(BrokerServerBuilder.class.toString());
    brokerMetrics = (BrokerMetrics) config.getServletContext().getAttribute(BrokerMetrics.class.toString());
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    if (this.brokerServerBuilder != null && this.brokerServerBuilder.getCurrentState() == State.RUNNING) {
      resp.getWriter().write("OK");
      resp.getWriter().close();
      brokerMetrics.addMeteredGlobalValue(BrokerMeter.HEALTHCHECK_OK_CALLS, 1);
    } else {
      resp.setStatus(503);
      resp.getWriter().write("Pinot broker is disabled");
      resp.getWriter().close();
      brokerMetrics.addMeteredGlobalValue(BrokerMeter.HEALTHCHECK_BAD_CALLS, 1);
    }
  }

}
