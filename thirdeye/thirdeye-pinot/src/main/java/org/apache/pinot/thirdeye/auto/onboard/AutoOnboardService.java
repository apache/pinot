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

package org.apache.pinot.thirdeye.auto.onboard;

import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a service to onboard datasets automatically to thirdeye from the different data sources
 * This service runs periodically and runs auto load for each data source
 */
public class AutoOnboardService implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(AutoOnboardService.class);

  private ScheduledExecutorService scheduledExecutorService;

  private List<AutoOnboard> autoOnboardServices = new ArrayList<>();
  private TimeGranularity runFrequency;

  /**
   * Reads data sources configs and instantiates the constructors for auto load of all data sources, if availble
   * @param config
   */
  public AutoOnboardService(ThirdEyeAnomalyConfiguration config) {
    this.runFrequency = config.getAutoOnboardConfiguration().getRunFrequency();
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    Map<String, List<AutoOnboard>> dataSourceToOnboardMap = AutoOnboardUtility.getDataSourceToAutoOnboardMap(
        config.getDataSourcesAsUrl());
    for (List<AutoOnboard> autoOnboards : dataSourceToOnboardMap.values()) {
      autoOnboardServices.addAll(autoOnboards);
    }
  }

  public void start() {
    scheduledExecutorService.scheduleAtFixedRate(this, 0, runFrequency.getSize(), runFrequency.getUnit());
  }

  public void shutdown() {
    LOG.info("Shutting down AutoOnboardService");
    scheduledExecutorService.shutdown();
  }

  @Override
  public void run() {
    for (AutoOnboard autoOnboard : autoOnboardServices) {
      LOG.info("Running auto load for {}", autoOnboard.getClass().getSimpleName());
      try {
        autoOnboard.run();
      } catch (Throwable t) {
        LOG.error("Uncaught exception is detected while running AutoOnboard for {}", autoOnboard.getClass().getSimpleName());
        t.printStackTrace();
      }
    }
  }
}
