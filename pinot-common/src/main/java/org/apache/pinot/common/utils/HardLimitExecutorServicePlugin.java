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

import com.google.auto.service.AutoService;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.spi.executor.ExecutorServicePlugin;
import org.apache.pinot.spi.executor.ExecutorServiceProvider;
import org.apache.pinot.spi.executor.ExecutorServiceUtils;
import org.apache.pinot.spi.executor.HardLimitExecutor;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * This is the plugin for the cached executor service with a hard limit for the amount of created threads.
 *
 * The provider included in this plugin creates cached thread pools, which are the recommended executor service for
 * cases where the tasks are short-lived and not CPU bound.
 *
 * If that is not the case, this executor may create a large number of threads that will be competing for CPU resources,
 * which may lead to performance degradation and even system instability.
 * In that case {@link FixedExecutorServicePlugin} could be used, but it may need changes to the code to avoid
 * deadlocks. Deployments using Java 21 or above could consider using a virtual thread executor service plugin.
 *
 * @see org.apache.pinot.spi.executor.ExecutorServiceUtils
 */
@AutoService(ExecutorServicePlugin.class)
public class HardLimitExecutorServicePlugin implements ExecutorServicePlugin {
  @Override
  public String id() {
    return "hardlimit";
  }

  @Override
  public ExecutorServiceProvider provider() {
    return (conf, confPrefix, baseName) -> {
      int limit = ExecutorServiceUtils.getMultiStageExecutorHardLimit(conf);
      ExecutorService executorService = ExecutorServiceUtils.create(
          conf, CommonConstants.Server.MULTISTAGE_EXECUTOR_CONFIG_PREFIX, baseName,
          CommonConstants.Server.DEFAULT_MULTISTAGE_EXECUTOR_TYPE);
      if (limit > 0) {
        executorService = new HardLimitExecutor(limit, executorService);
      }
      return executorService;
    };
  }
}
