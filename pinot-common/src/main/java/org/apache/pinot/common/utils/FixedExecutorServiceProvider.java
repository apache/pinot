package org.apache.pinot.common.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.executor.ExecutorServiceProvider;
import org.apache.pinot.spi.utils.CommonConstants;


public class FixedExecutorServiceProvider implements ExecutorServiceProvider {

  @Override
  public ExecutorService create(PinotConfiguration conf, String confPrefix, String baseName) {
    String defaultFixedThreadsStr = conf.getProperty(
        CommonConstants.CONFIG_OF_EXECUTORS_FIXED_NUM_THREADS, CommonConstants.DEFAULT_EXECUTORS_FIXED_NUM_THREADS);
    int defaultFixedThreads = Integer.parseInt(defaultFixedThreadsStr);
    if (defaultFixedThreads < 0) {
      defaultFixedThreads = Runtime.getRuntime().availableProcessors();
    }
    int numThreads = conf.getProperty(confPrefix + ".numThreads", defaultFixedThreads);
    return Executors.newFixedThreadPool(numThreads, new NamedThreadFactory(baseName));
  }
}
