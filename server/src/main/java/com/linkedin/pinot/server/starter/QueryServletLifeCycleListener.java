package com.linkedin.pinot.server.starter;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class QueryServletLifeCycleListener implements ServletContextListener {

  InteractiveBroker b;
  
  public QueryServletLifeCycleListener(InteractiveBroker broker) {
    b = broker;
  }

  @Override
  public void contextInitialized(ServletContextEvent sce) {
    sce.getServletContext().setAttribute(InteractiveBroker.class.toString(), b);
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    
  }
  
}
