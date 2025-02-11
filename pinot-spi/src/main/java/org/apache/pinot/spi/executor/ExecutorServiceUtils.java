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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
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
 * @see ServiceLoader
 */
public class ExecutorServiceUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorServiceUtils.class);
  private static final long DEFAULT_TERMINATION_MILLIS = 30_000;

  private static final Map<String, ExecutorServiceProvider> PROVIDERS;

  static {
    PROVIDERS = new HashMap<>();
    forEachExecutorThatLoads(plugin -> {
      ExecutorServiceProvider provider = plugin.provider();
      ExecutorServiceProvider old = PROVIDERS.put(plugin.id(), provider);
      if (old != null) {
        LOGGER.warn("Duplicate executor provider for id '{}': {} and {}", plugin.id(), old, provider);
      } else {
        LOGGER.info("Registered executor provider for id '{}': {}", plugin.id(), provider);
      }
    });
  }

  private static void forEachExecutorThatLoads(Consumer<ExecutorServicePlugin> consumer) {
    Iterator<ExecutorServicePlugin> iterator = ServiceLoader.load(ExecutorServicePlugin.class).iterator();
    while (hasNextOrSkip(iterator)) {
      ExecutorServicePlugin next;
      try {
        next = iterator.next();
      } catch (ServiceConfigurationError e) {
        LOGGER.warn("Skipping executor service plugin that doesn't load", e);
        continue;
      }
      consumer.accept(next);
    }
  }

  private static boolean hasNextOrSkip(Iterator<ExecutorServicePlugin> loader) {
    while (true) {
      try {
        return loader.hasNext();
      } catch (ServiceConfigurationError e) {
        LOGGER.warn("Skipping executor service plugin", e);
      }
    }
  }

  private ExecutorServiceUtils() {
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
