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
import org.apache.pinot.thirdeye.anomaly.alert.v2.AlertJobSchedulerV2;
import org.apache.pinot.thirdeye.anomaly.classification.ClassificationJobScheduler;
import org.apache.pinot.thirdeye.anomaly.classification.classifier.AnomalyClassifierFactory;
import org.apache.pinot.thirdeye.anomaly.detection.DetectionJobScheduler;
import org.apache.pinot.thirdeye.anomaly.events.HolidayEventResource;
import org.apache.pinot.thirdeye.anomaly.events.HolidayEventsLoader;
import org.apache.pinot.thirdeye.anomaly.monitor.MonitorJobScheduler;
import org.apache.pinot.thirdeye.anomaly.task.TaskDriver;
import org.apache.pinot.thirdeye.anomalydetection.alertFilterAutotune.AlertFilterAutotuneFactory;
import org.apache.pinot.thirdeye.api.TimeGranularity;
import org.apache.pinot.thirdeye.auto.onboard.AutoOnboardService;
import org.apache.pinot.thirdeye.common.BaseThirdEyeApplication;
import org.apache.pinot.thirdeye.common.ThirdEyeSwaggerBundle;
import org.apache.pinot.thirdeye.completeness.checker.DataCompletenessScheduler;
import org.apache.pinot.thirdeye.dashboard.resources.DetectionJobResource;
import org.apache.pinot.thirdeye.dashboard.resources.EmailResource;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.pinot.resources.PinotDataSourceResource;
import org.apache.pinot.thirdeye.detection.DetectionPipelineScheduler;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertScheduler;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionAlertRegistry;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detector.email.filter.AlertFilterFactory;
import org.apache.pinot.thirdeye.detector.function.AnomalyFunctionFactory;
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

  private DetectionJobScheduler detectionJobScheduler = null;
  private TaskDriver taskDriver = null;
  private MonitorJobScheduler monitorJobScheduler = null;
  private AlertJobSchedulerV2 alertJobSchedulerV2;
  private AnomalyFunctionFactory anomalyFunctionFactory = null;
  private AutoOnboardService autoOnboardService = null;
  private DataCompletenessScheduler dataCompletenessScheduler = null;
  private AlertFilterFactory alertFilterFactory = null;
  private AnomalyClassifierFactory anomalyClassifierFactory = null;
  private AlertFilterAutotuneFactory alertFilterAutotuneFactory = null;
  private ClassificationJobScheduler classificationJobScheduler = null;
  private EmailResource emailResource = null;
  private HolidayEventsLoader holidayEventsLoader = null;
  private RequestStatisticsLogger requestStatisticsLogger = null;
  private DetectionPipelineScheduler detectionPipelineScheduler = null;
  private DetectionAlertScheduler detectionAlertScheduler = null;

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
  public void run(final ThirdEyeAnomalyConfiguration config, final Environment environment)
      throws Exception {
    LOG.info("Starting ThirdeyeAnomalyApplication : Scheduler {} Worker {}", config.isScheduler(), config.isWorker());
    super.initDAOs();
    try {
      ThirdEyeCacheRegistry.initializeCaches(config);
    } catch (Exception e) {
      LOG.error("Exception while loading caches", e);
    }

    environment.getObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    environment.getObjectMapper().registerModule(makeMapperModule());

    environment.lifecycle().manage(new Managed() {
      @Override
      public void start() throws Exception {

        requestStatisticsLogger = new RequestStatisticsLogger(new TimeGranularity(1, TimeUnit.DAYS));
        requestStatisticsLogger.start();

        if (config.isWorker()) {
          initAnomalyFunctionFactory(config.getFunctionConfigPath());
          initAlertFilterFactory(config.getAlertFilterConfigPath());
          initAnomalyClassifierFactory(config.getAnomalyClassifierConfigPath());

          taskDriver = new TaskDriver(config, anomalyFunctionFactory, alertFilterFactory, anomalyClassifierFactory);
          taskDriver.start();
        }
        if (config.isScheduler()) {
          initAnomalyFunctionFactory(config.getFunctionConfigPath());
          initAlertFilterFactory(config.getAlertFilterConfigPath());
          initAlertFilterAutotuneFactory(config.getFilterAutotuneConfigPath());

          emailResource = new EmailResource(config);
          detectionJobScheduler = new DetectionJobScheduler();
          detectionJobScheduler.start();
          environment.jersey().register(
              new DetectionJobResource(detectionJobScheduler, alertFilterFactory, alertFilterAutotuneFactory, emailResource));
        }
        if (config.isMonitor()) {
          monitorJobScheduler = new MonitorJobScheduler(config.getMonitorConfiguration());
          monitorJobScheduler.start();
        }
        if (config.isAlert()) {
          // start alert scheduler v2
          alertJobSchedulerV2 = new AlertJobSchedulerV2();
          alertJobSchedulerV2.start();
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
          environment.jersey().register(new HolidayEventResource(holidayEventsLoader));
        }
        if (config.isDataCompleteness()) {
          dataCompletenessScheduler = new DataCompletenessScheduler();
          dataCompletenessScheduler.start();
        }
        if (config.isClassifier()) {
          classificationJobScheduler = new ClassificationJobScheduler();
          classificationJobScheduler.start();
        }
        if (config.isPinotProxy()) {
          environment.jersey().register(new PinotDataSourceResource());
        }
        if (config.isDetectionPipeline()) {
          detectionPipelineScheduler = new DetectionPipelineScheduler(DAORegistry.getInstance().getDetectionConfigManager());
          detectionPipelineScheduler.start();
        }
        if (config.isDetectionAlert()) {
          detectionAlertScheduler = new DetectionAlertScheduler();
          detectionAlertScheduler.start();
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
        if (detectionJobScheduler != null) {
          detectionJobScheduler.shutdown();
        }
        if (monitorJobScheduler != null) {
          monitorJobScheduler.shutdown();
        }
        if (holidayEventsLoader != null) {
          holidayEventsLoader.shutdown();
        }
        if (alertJobSchedulerV2 != null) {
          alertJobSchedulerV2.shutdown();
        }
        if (autoOnboardService != null) {
          autoOnboardService.shutdown();
        }
        if (dataCompletenessScheduler != null) {
          dataCompletenessScheduler.shutdown();
        }
        if (classificationJobScheduler != null) {
          classificationJobScheduler.shutdown();
        }
        if (detectionPipelineScheduler != null) {
          detectionPipelineScheduler.shutdown();
        }
      }
    });
  }

  private void initAnomalyFunctionFactory(String functoinConfigPath) {
    if (anomalyFunctionFactory == null) {
      anomalyFunctionFactory = new AnomalyFunctionFactory(functoinConfigPath);
    }
  }

  private void initAlertFilterFactory(String alertFilterConfigPath) {
    if (alertFilterFactory == null) {
      alertFilterFactory = new AlertFilterFactory(alertFilterConfigPath);
    }
  }

  private void initAnomalyClassifierFactory(String anomalyClassifierConfigPath) {
    if (anomalyClassifierFactory == null) {
      anomalyClassifierFactory = new AnomalyClassifierFactory(anomalyClassifierConfigPath);
    }
  }

  private void initAlertFilterAutotuneFactory(String filterAutotuneConfigPath) {
    if (alertFilterAutotuneFactory == null) {
      alertFilterAutotuneFactory = new AlertFilterAutotuneFactory(filterAutotuneConfigPath);
    }
  }
}
