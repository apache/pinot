package com.linkedin.pinot.integration.tests.helpers;

/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 13, 2014
 */

public interface Generator {

  public void init();
  public Object next();
}
