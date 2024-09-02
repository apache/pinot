package org.apache.pinot.spi.executor;

public interface ExecutorServicePlugin {

  /**
   * An id that identifies this specific provider.
   * <p>
   * This id is used to select the provider in the configuration.
   */
  String id();

  /**
   * Returns the provider that will create the {@link java.util.concurrent.ExecutorService} instances.
   */
  ExecutorServiceProvider provider();
}
