package com.linkedin.pinot.broker.servlet;

import com.linkedin.pinot.common.metrics.BrokerMetrics;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.linkedin.pinot.requestHandler.BrokerRequestHandler;


public class PinotBrokerServletContextChangeListener implements ServletContextListener {
  private BrokerRequestHandler requestHandler;
  private BrokerMetrics _brokerMetrics;

  public PinotBrokerServletContextChangeListener(BrokerRequestHandler handler, BrokerMetrics brokerMetrics) {
    this.requestHandler = handler;
    _brokerMetrics = brokerMetrics;
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    // nothing to do here for now
  }

  @Override
  public void contextInitialized(ServletContextEvent sce) {
    sce.getServletContext().setAttribute(BrokerRequestHandler.class.toString(), requestHandler);
    sce.getServletContext().setAttribute(BrokerMetrics.class.toString(), _brokerMetrics);
  }

}
