/*
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

package org.apache.pinot.thirdeye.anomaly.monitor;

import org.apache.pinot.thirdeye.anomaly.utils.AnomalyUtils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datasource.DAORegistry;

public class MonitorJobScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(MonitorJobScheduler.class);
  private ScheduledExecutorService scheduledExecutorService;
  private TaskManager anomalyTaskDAO;
  private MonitorConfiguration monitorConfiguration;
  private MonitorJobRunner monitorJobRunner;
  private MonitorJobContext monitorJobContext;
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  public MonitorJobScheduler(MonitorConfiguration monitorConfiguration) {
    this.anomalyTaskDAO = DAO_REGISTRY.getTaskDAO();
    this.monitorConfiguration = monitorConfiguration;
    scheduledExecutorService = Executors.newScheduledThreadPool(10);
  }

  public void start() {
    LOG.info("Starting monitor service");

    monitorJobContext = new MonitorJobContext();
    monitorJobContext.setTaskDAO(anomalyTaskDAO);
    monitorJobContext.setMonitorConfiguration(monitorConfiguration);

    monitorJobRunner = new MonitorJobRunner(monitorJobContext);
    scheduledExecutorService
      .scheduleWithFixedDelay(monitorJobRunner, 0, monitorConfiguration.getMonitorFrequency().getSize(),
          monitorConfiguration.getMonitorFrequency().getUnit());
  }

  public void shutdown() {
    LOG.info("Stopping monitor service");
    AnomalyUtils.safelyShutdownExecutionService(scheduledExecutorService, this.getClass());
  }
}
