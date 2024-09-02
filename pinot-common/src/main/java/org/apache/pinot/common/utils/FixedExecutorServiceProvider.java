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
