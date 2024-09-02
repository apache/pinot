/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.executor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A utility class to create {@link ExecutorService} instances.
 *
 * In order to create a new executor, the {@code create} methods should be called.
 * These methods take an executor type as an argument.
 *
 * Pinot includes two executor service plugins:
 * <ul>
 *   <li>{@code cached}: creates a new cached thread pool</li>
 *   <li>{@code fixed}: creates a new fixed thread pool.</li>
 * </ul>
 *
 * The cached executor service plugin is the default provider. It creates a new cached thread pool, which is the
 * recommended executor service for cases where the tasks are short-lived and not CPU bound.
 * If that is not the case, this executor may create a large number of threads that will be competing for CPU resources,
 * which may lead to performance degradation and even system instability.
 *
 * The fixed executor service plugin creates a new fixed thread pool. The number of threads is defined in the
 * configuration. This executor service is recommended for cases where the tasks are long-lived or CPU bound,
 * but it may need changes to the code to avoid deadlocks.
 *
 * Other plugins can be added by implementing the {@link ExecutorServicePlugin} interface and declaring them as services
 * in the classpath. Although it is not included in the Pinot distribution, a custom plugin can be added to create a
 * virtual thread executor service, which in cases where threads are not pinned to a specific CPU core, can provide
 * the same safety as the cached executor service, but without competing for CPU resources when the tasks are CPU bound.
 *
 * @see ServiceLoader
 */
public class ExecutorServiceUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorServiceUtils.class);
  private static final long DEFAULT_TERMINATION_MILLIS = 30_000;

  public final static String DEFAULT_PROVIDER = "cached";

  private static final Map<String, ExecutorServiceProvider> PROVIDERS;

  static {
    PROVIDERS = new HashMap<>();
    for (ExecutorServicePlugin plugin : ServiceLoader.load(ExecutorServicePlugin.class)) {
      ExecutorServiceProvider provider = plugin.provider();
      ExecutorServiceProvider old = PROVIDERS.put(plugin.id(), provider);
      if (old != null) {
        LOGGER.warn("Duplicate provider for id '{}': {} and {}", plugin.id(), old, provider);
      }
    }
    if (!PROVIDERS.containsKey(DEFAULT_PROVIDER)) {
      throw new IllegalStateException("The default executor " + DEFAULT_PROVIDER + " is not available");
    }
    LOGGER.info("Default executor service provider: {}", DEFAULT_PROVIDER);
  }

  private ExecutorServiceUtils() {
  }

  public static ExecutorService create(PinotConfiguration conf, String confPrefix, String baseName) {
    return create(conf, confPrefix, baseName, DEFAULT_PROVIDER);
  }

  public static ExecutorService create(PinotConfiguration conf, String confPrefix, String baseName, String defType) {
    String type = conf.getProperty(confPrefix + ".type", defType);
    ExecutorServiceProvider provider = PROVIDERS.get(type);
    if (provider == null) {
      throw new IllegalArgumentException("Unknown executor service provider: " + type);
    }
    return provider.create(conf, confPrefix, baseName);
  }

  /**
   * Shuts down the given executor service.
   *
   * This method blocks a default number of millis in order to wait for termination. In case the executor doesn't
   * terminate in that time, the code continues with a logging.
   *
   * @throws RuntimeException if this threads is interrupted when waiting for termination.
   */
  public static void close(ExecutorService executorService) {
    close(executorService, DEFAULT_TERMINATION_MILLIS);
  }

  /**
   * Shuts down the given executor service.
   *
   * This method blocks up to the given millis in order to wait for termination. In case the executor doesn't terminate
   * in that time, the code continues with a logging.
   *
   * @throws RuntimeException if this threads is interrupted when waiting for termination.
   */
  public static void close(ExecutorService executorService, long terminationMillis) {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(terminationMillis, TimeUnit.SECONDS)) {
        List<Runnable> runnables = executorService.shutdownNow();
        LOGGER.warn("Around {} didn't finish in time after a shutdown", runnables.size());
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
