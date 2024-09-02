package org.apache.pinot.common.utils;

import com.google.auto.service.AutoService;
import org.apache.pinot.spi.executor.ExecutorServicePlugin;
import org.apache.pinot.spi.executor.ExecutorServiceProvider;

/**
 * This is the plugin for the fixed executor service.
 *
 * @see org.apache.pinot.spi.executor.ExecutorServiceUtils
 */
@AutoService(ExecutorServicePlugin.class)
public class FixedExecutorServicePlugin implements ExecutorServicePlugin {
  @Override
  public String id() {
    return "fixed";
  }

  @Override
  public ExecutorServiceProvider provider() {
    return new FixedExecutorServiceProvider();
  }
}
