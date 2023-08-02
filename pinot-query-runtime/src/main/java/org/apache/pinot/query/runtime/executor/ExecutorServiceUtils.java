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
package org.apache.pinot.query.runtime.executor;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExecutorServiceUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorServiceUtils.class);

  private ExecutorServiceUtils() {
  }

  public static ExecutorService createDefault(String baseName) {
    return Executors.newCachedThreadPool(new NamedThreadFactory(baseName));
  }

  public static ExecutorService create(PinotConfiguration conf, String confPrefix, String baseName) {
    //TODO: make this configurable
    return Executors.newCachedThreadPool(new NamedThreadFactory(baseName));
  }

  public static void close(ExecutorService executorService) {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
        List<Runnable> runnables = executorService.shutdownNow();
        LOGGER.warn("Around " + runnables.size() + " didn't finish in time after a shutdown");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
