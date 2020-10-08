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

package org.apache.pinot.thirdeye.anomaly;

import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.pinot.thirdeye.anomaly.detection.trigger.DataAvailabilityEventListenerDriver;
import org.apache.pinot.thirdeye.anomaly.detection.trigger.DataAvailabilityTaskScheduler;
import org.apache.pinot.thirdeye.anomaly.events.HolidayEventResource;
import org.apache.pinot.thirdeye.anomaly.events.HolidayEventsLoader;
import org.apache.pinot.thirdeye.anomaly.events.MockEventsLoader;
import org.apache.pinot.thirdeye.anomaly.monitor.MonitorJobScheduler;
import org.apache.pinot.thirdeye.anomaly.task.TaskDriver;
import org.apache.pinot.thirdeye.common.restclient.ThirdEyeRestClientConfiguration;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.auto.onboard.AutoOnboardService;
import org.apache.pinot.thirdeye.common.BaseThirdEyeApplication;
import org.apache.pinot.thirdeye.common.ThirdEyeSwaggerBundle;
import org.apache.pinot.thirdeye.common.utils.SessionUtils;
import org.apache.pinot.thirdeye.datalayer.dto.SessionDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.pinot.resources.PinotDataSourceResource;
import org.apache.pinot.thirdeye.model.download.ModelDownloaderManager;
import org.apache.pinot.thirdeye.scheduler.DetectionCronScheduler;
import org.apache.pinot.thirdeye.scheduler.SubscriptionCronScheduler;
import org.apache.pinot.thirdeye.tracking.RequestStatisticsLogger;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class ThirdEyeAnomalyApplication
    extends BaseThirdEyeApplication<ThirdEyeAnomalyConfiguration> {

  private TaskDriver taskDriver = null;
  private MonitorJobScheduler monitorJobScheduler = null;
  private AutoOnboardService autoOnboardService = null;
  private HolidayEventsLoader holidayEventsLoader = null;
  private MockEventsLoader mockEventsLoader = null;
  private RequestStatisticsLogger requestStatisticsLogger = null;
  private DataAvailabilityEventListenerDriver dataAvailabilityEventListenerDriver = null;
  private DataAvailabilityTaskScheduler dataAvailabilityTaskScheduler = null;
  private DetectionCronScheduler detectionScheduler = null;
  private SubscriptionCronScheduler subscriptionScheduler = null;
  private ModelDownloaderManager modelDownloaderManager = null;

  public static void main(final String[] args) throws Exception {
    List<String> argList = new ArrayList<>(Arrays.asList(args));
    if (argList.isEmpty()) {
      argList.add("./config");
    }

    if (argList.size() <= 1) {
      argList.add(0, "server");
    }

    int lastIndex = argList.size() - 1;
    String thirdEyeConfigDir = argList.get(lastIndex);
    System.setProperty("dw.rootDir", thirdEyeConfigDir);
    String detectorApplicationConfigFile = thirdEyeConfigDir + "/" + "detector.yml";
    argList.set(lastIndex, detectorApplicationConfigFile); // replace config dir with the
                                                           // actual config file
    new ThirdEyeAnomalyApplication().run(argList.toArray(new String[argList.size()]));
  }

  @Override
  public String getName() {
    return "Thirdeye Controller";
  }

  @Override
  public void initialize(final Bootstrap<ThirdEyeAnomalyConfiguration> bootstrap) {
    bootstrap.addBundle(new AssetsBundle("/assets/", "/", "index.html"));
    bootstrap.addBundle(new ThirdEyeSwaggerBundle());
  }

  @Override
  public void run(final ThirdEyeAnomalyConfiguration config, final Environment env)
      throws Exception {
    LOG.info("Starting ThirdeyeAnomalyApplication : Scheduler {} Worker {}", config.isScheduler(), config.isWorker());
    super.initDAOs();
    try {
      ThirdEyeCacheRegistry.initializeCaches(config);
    } catch (Exception e) {
      LOG.error("Exception while loading caches", e);
    }

    env.getObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    env.getObjectMapper().registerModule(makeMapperModule());

    env.lifecycle().manage(lifecycleManager(config, env));
  }

  private Managed lifecycleManager(ThirdEyeAnomalyConfiguration config, Environment env) {
    return new Managed() {
      @Override
      public void start() throws Exception {

        requestStatisticsLogger = new RequestStatisticsLogger(new TimeGranularity(1, TimeUnit.DAYS));
        requestStatisticsLogger.start();

        if (config.isWorker()) {
          taskDriver = new TaskDriver(config, false);
          taskDriver.start();
        }

        if (config.isOnlineWorker()) {
          taskDriver = new TaskDriver(config, true);
          taskDriver.start();
        }

        if (config.isMonitor()) {
          monitorJobScheduler = new MonitorJobScheduler(config.getMonitorConfiguration());
          monitorJobScheduler.start();
        }
        if (config.isAutoload()) {
          autoOnboardService = new AutoOnboardService(config);
          autoOnboardService.start();
        }
        if (config.isHolidayEventsLoader()) {
          holidayEventsLoader =
              new HolidayEventsLoader(config.getHolidayEventsLoaderConfiguration(), config.getCalendarApiKeyPath(),
                  DAORegistry.getInstance().getEventDAO());
          holidayEventsLoader.start();
          env.jersey().register(new HolidayEventResource(holidayEventsLoader));
        }
        if (config.isMockEventsLoader()) {
          mockEventsLoader = new MockEventsLoader(config.getMockEventsLoaderConfiguration(), DAORegistry.getInstance().getEventDAO());
          mockEventsLoader.run();
        }
        if (config.isPinotProxy()) {
          env.jersey().register(new PinotDataSourceResource());
        }
        if (config.isDetectionPipeline()) {
          detectionScheduler = new DetectionCronScheduler(DAORegistry.getInstance().getDetectionConfigManager());
          detectionScheduler.start();
        }
        if (config.isDetectionAlert()) {
          subscriptionScheduler = new SubscriptionCronScheduler();
          subscriptionScheduler.start();
        }
        if (config.isDataAvailabilityEventListener()) {
          dataAvailabilityEventListenerDriver = new DataAvailabilityEventListenerDriver(config.getDataAvailabilitySchedulingConfiguration());
          dataAvailabilityEventListenerDriver.start();
        }
        if (config.isDataAvailabilityTaskScheduler()) {
          dataAvailabilityTaskScheduler = new DataAvailabilityTaskScheduler(
              config.getDataAvailabilitySchedulingConfiguration().getSchedulerDelayInSec(),
              config.getDataAvailabilitySchedulingConfiguration().getTaskTriggerFallBackTimeInSec(),
              config.getDataAvailabilitySchedulingConfiguration().getSchedulingWindowInSec(),
              config.getDataAvailabilitySchedulingConfiguration().getScheduleDelayInSec());
          dataAvailabilityTaskScheduler.start();
        }
        if (config.getModelDownloaderConfig() != null) {
          modelDownloaderManager = new ModelDownloaderManager(config.getModelDownloaderConfig());
          modelDownloaderManager.start();
        }
        if (config.getThirdEyeRestClientConfiguration() != null) {
          ThirdEyeRestClientConfiguration restClientConfig = config.getThirdEyeRestClientConfiguration();
          updateAdminSession(restClientConfig.getAdminUser(), restClientConfig.getSessionKey());
        }
      }

      @Override
      public void stop() throws Exception {
        if (requestStatisticsLogger != null) {
          requestStatisticsLogger.shutdown();
        }
        if (taskDriver != null) {
          taskDriver.shutdown();
        }
        if (monitorJobScheduler != null) {
          monitorJobScheduler.shutdown();
        }
        if (holidayEventsLoader != null) {
          holidayEventsLoader.shutdown();
        }
        if (autoOnboardService != null) {
          autoOnboardService.shutdown();
        }
        if (detectionScheduler != null) {
          detectionScheduler.shutdown();
        }
        if (dataAvailabilityEventListenerDriver != null) {
          dataAvailabilityEventListenerDriver.shutdown();
        }
        if (modelDownloaderManager != null) {
          modelDownloaderManager.shutdown();
        }
      }
    };
  }

  private void updateAdminSession(String adminUser, String sessionKey) {
    SessionDTO savedSession = DAO_REGISTRY.getSessionDAO().findBySessionKey(sessionKey);
    long expiryMillis = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(365);
    if (savedSession == null) {
      SessionDTO sessionDTO = SessionUtils.buildServiceAccount(adminUser, sessionKey, expiryMillis);
      DAO_REGISTRY.getSessionDAO().save(sessionDTO);
    } else {
      savedSession.setExpirationTime(expiryMillis);
      DAO_REGISTRY.getSessionDAO().update(savedSession);
    }
  }
}
