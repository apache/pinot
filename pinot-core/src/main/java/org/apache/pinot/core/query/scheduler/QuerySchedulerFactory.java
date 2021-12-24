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
package org.apache.pinot.core.query.scheduler;

import com.google.common.base.Preconditions;
import java.lang.reflect.Constructor;
import java.util.concurrent.atomic.LongAccumulator;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.scheduler.fcfs.BoundedFCFSScheduler;
import org.apache.pinot.core.query.scheduler.fcfs.FCFSQueryScheduler;
import org.apache.pinot.core.query.scheduler.tokenbucket.TokenPriorityScheduler;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.plugin.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory class to initialize query scheduler
 */
public class QuerySchedulerFactory {
  private QuerySchedulerFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(QuerySchedulerFactory.class);

  public static final String FCFS_ALGORITHM = "fcfs";
  public static final String TOKEN_BUCKET_ALGORITHM = "tokenbucket";
  public static final String BOUNDED_FCFS_ALGORITHM = "bounded_fcfs";
  public static final String ALGORITHM_NAME_CONFIG_KEY = "name";
  public static final String DEFAULT_QUERY_SCHEDULER_ALGORITHM = FCFS_ALGORITHM;

  /**
   * Static factory to instantiate query scheduler based on scheduler configuration.
   * 'name' configuration in the scheduler will decide which scheduler instance to create
   * Besides known instances, 'name' can be a classname
   * @param schedulerConfig scheduler specific configuration
   * @param queryExecutor QueryExecutor to use
   * @return returns an instance of query scheduler
   */
  public static QueryScheduler create(PinotConfiguration schedulerConfig, QueryExecutor queryExecutor,
      ServerMetrics serverMetrics, LongAccumulator latestQueryTime) {
    Preconditions.checkNotNull(schedulerConfig);
    Preconditions.checkNotNull(queryExecutor);

    String schedulerName = schedulerConfig.getProperty(ALGORITHM_NAME_CONFIG_KEY, DEFAULT_QUERY_SCHEDULER_ALGORITHM);
    QueryScheduler scheduler;
    switch (schedulerName.toLowerCase()) {
      case FCFS_ALGORITHM:
        scheduler = new FCFSQueryScheduler(schedulerConfig, queryExecutor, serverMetrics, latestQueryTime);
        break;
      case TOKEN_BUCKET_ALGORITHM:
        scheduler = TokenPriorityScheduler.create(schedulerConfig, queryExecutor, serverMetrics, latestQueryTime);
        break;
      case BOUNDED_FCFS_ALGORITHM:
        scheduler = BoundedFCFSScheduler.create(schedulerConfig, queryExecutor, serverMetrics, latestQueryTime);
        break;
      default:
        scheduler =
            getQuerySchedulerByClassName(schedulerName, schedulerConfig, queryExecutor, serverMetrics, latestQueryTime);
        break;
    }

    if (scheduler != null) {
      LOGGER.info("Using {} scheduler", scheduler.name());
      return scheduler;
    }

    // if we don't find the configured algorithm we warn and use the default one
    // because it's better to execute with poor algorithm than completely fail.
    // Failure on bad configuration will cause outage vs an inferior algorithm that
    // will provide degraded service
    LOGGER.warn("Scheduler {} not found. Using default FCFS query scheduler", schedulerName);
    return new FCFSQueryScheduler(schedulerConfig, queryExecutor, serverMetrics, latestQueryTime);
  }

  @Nullable
  private static QueryScheduler getQuerySchedulerByClassName(String className, PinotConfiguration schedulerConfig,
      QueryExecutor queryExecutor, ServerMetrics serverMetrics, LongAccumulator latestQueryTime) {
    try {
      Constructor<?> constructor = PluginManager.get().loadClass(className)
          .getDeclaredConstructor(PinotConfiguration.class, QueryExecutor.class, ServerMetrics.class,
              LongAccumulator.class);
      constructor.setAccessible(true);
      return (QueryScheduler) constructor.newInstance(schedulerConfig, queryExecutor, serverMetrics, latestQueryTime);
    } catch (Exception e) {
      LOGGER.error("Failed to instantiate scheduler class by name: {}", className, e);
      return null;
    }
  }
}
