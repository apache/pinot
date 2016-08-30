package com.linkedin.thirdeye.hadoop.wait;

import java.util.Properties;


/**
 * Simple interface to check completeness of input folder
 */
public interface WaitUDF {

  /**
   * Initializes by providing input configs.
   * @param inputConfig
   */
  void init(Properties inputConfig);

  /**
   * @return completeness status
   * @throws IOException
   * @throws
   */
   boolean checkCompleteness() throws Exception;
}
