package com.linkedin.pinot.broker.servlet;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.linkedin.pinot.requestHandler.BrokerRequestHandler;


public class PinotBrokerServletContextChangeListener implements ServletContextListener {
  private BrokerRequestHandler requestHandler;

  public PinotBrokerServletContextChangeListener(BrokerRequestHandler handler) {
    this.requestHandler = handler;
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    // nothing to do here for now
  }

  @Override
  public void contextInitialized(ServletContextEvent sce) {
    sce.getServletContext().setAttribute(BrokerRequestHandler.class.toString(), requestHandler);
  }

}
