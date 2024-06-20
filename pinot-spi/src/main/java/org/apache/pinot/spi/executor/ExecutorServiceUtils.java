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
import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExecutorServiceUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorServiceUtils.class);
  private static final long DEFAULT_TERMINATION_MILLIS = 30_000;

  private static final String DEFAULT_PROVIDER;
  private static final Map<String, ExecutorServiceProvider> PROVIDERS;

  static {
    PROVIDERS = new HashMap<>();
    for (ExecutorServiceProvider provider : ServiceLoader.load(ExecutorServiceProvider.class)) {
      ExecutorServiceProvider old = PROVIDERS.put(provider.id(), provider);
      if (old != null) {
        LOGGER.warn("Duplicate provider for id '{}': {} and {}", provider.id(), old, provider);
      }
    }
    if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_21) && PROVIDERS.containsKey("virtual")) {
      DEFAULT_PROVIDER = "virtual";
    } else if (PROVIDERS.containsKey("cached")) {
      DEFAULT_PROVIDER = "cached";
    } else if (!PROVIDERS.isEmpty()) {
      DEFAULT_PROVIDER = PROVIDERS.keySet().iterator().next();
    } else {
      throw new IllegalStateException("No executor service providers found");
    }
    LOGGER.info("Default executor service provider: {}", DEFAULT_PROVIDER);
  }

  private ExecutorServiceUtils() {
  }

  public static ExecutorService createDefault(String baseName) {
    return create(DEFAULT_PROVIDER, new PinotConfiguration(), baseName, "executor.default");
  }

  public static ExecutorService create(PinotConfiguration conf, String confPrefix, String baseName) {
    return create(DEFAULT_PROVIDER, conf, confPrefix, baseName);
  }

  public static ExecutorService create(String defaultType, PinotConfiguration conf, String confPrefix,
      String baseName) {
    String type = conf.getProperty(confPrefix + ".type", defaultType);
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
