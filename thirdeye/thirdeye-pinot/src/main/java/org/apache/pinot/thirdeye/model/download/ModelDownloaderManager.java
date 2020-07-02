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
 *
 */

package org.apache.pinot.thirdeye.model.download;

import java.lang.reflect.Constructor;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pinot.thirdeye.auto.onboard.AutoOnboardService;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The model downloader manager. This class manages the life cycle of the model downloader.
 * It constructs the model downloader, and then schedules the model downloader to run periodically and downloads the
 * models into a local destination path. The class names, the run frequency and the download path can be configured.
 */
public class ModelDownloaderManager {
  private static final Logger LOG = LoggerFactory.getLogger(AutoOnboardService.class);

  private final List<ModelDownloaderConfiguration> configs;
  private final Map<String, ModelDownloader> modelDownloaders;
  private ScheduledExecutorService scheduledExecutorService;

  public ModelDownloaderManager(List<ModelDownloaderConfiguration> modelDownloaderConfigs) {
    this.configs = modelDownloaderConfigs;
    this.modelDownloaders = new HashMap<>();
    this.scheduledExecutorService = Executors.newScheduledThreadPool(5);

    constructModelDownloaders();
  }

  private void constructModelDownloaders() {
    for (ModelDownloaderConfiguration config : this.configs) {
      try {
        Constructor<?> constructor = Class.forName(config.getClassName()).getConstructor(Map.class);
        ModelDownloader downloader = (ModelDownloader) constructor.newInstance(config.getProperties());
        this.modelDownloaders.put(config.getClassName(), downloader);
      } catch (Exception e) {
        LOG.warn("Failed to initialize model downloader {}", config.getClassName(), e);
      }
    }
  }

  /**
   * start the model downloader manager
   */
  public void start() {
    for (ModelDownloaderConfiguration config : this.configs) {
      TimeGranularity runFrequency = config.getRunFrequency();
      this.scheduledExecutorService.scheduleAtFixedRate(() -> {
        LOG.info("running the model downloader: {}", config.getClassName());
        this.modelDownloaders.get(config.getClassName()).fetchModel(Paths.get(config.getDestinationPath()));
      }, 0L, runFrequency.getSize(), runFrequency.getUnit());
    }
  }

  /**
   * shut down the manager
   */
  public void shutdown() {
    LOG.info("Shutting down model downloader manager");
    scheduledExecutorService.shutdown();
  }
}
