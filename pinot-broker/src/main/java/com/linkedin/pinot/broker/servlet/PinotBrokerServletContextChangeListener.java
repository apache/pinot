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
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.routing.TimeBoundaryService;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;


public class PinotBrokerServletContextChangeListener implements ServletContextListener {
  private BrokerRequestHandler requestHandler;
  private BrokerMetrics _brokerMetrics;
  private TimeBoundaryService _timeBoundaryService;

  public PinotBrokerServletContextChangeListener(BrokerRequestHandler handler, BrokerMetrics brokerMetrics, TimeBoundaryService timeBoundaryService) {
    this.requestHandler = handler;
    _brokerMetrics = brokerMetrics;
    _timeBoundaryService = timeBoundaryService;
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    // nothing to do here for now
  }

  @Override
  public void contextInitialized(ServletContextEvent sce) {
    sce.getServletContext().setAttribute(BrokerRequestHandler.class.toString(), requestHandler);
    sce.getServletContext().setAttribute(BrokerMetrics.class.toString(), _brokerMetrics);
    sce.getServletContext().setAttribute(TimeBoundaryService.class.toString(), _timeBoundaryService);
  }

}
