/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller;

import org.apache.log4j.Logger;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Protocol;

import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.linkedin.pinot.common.metrics.ValidationMetrics;
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.realtime.PinotRealtimeSegmentsManager;
import com.linkedin.pinot.controller.helix.core.retention.RetentionManager;
import com.linkedin.pinot.controller.validation.ValidationManager;
import com.yammer.metrics.core.MetricsRegistry;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 24, 2014
 */

public class ControllerStarter {
  private static final Logger logger = Logger.getLogger(ControllerStarter.class);
  private final ControllerConf config;

  private final Component component;
  private final Application controllerRestApp;
  private final PinotHelixResourceManager helixResourceManager;
  private final RetentionManager retentionManager;
  private final ValidationManager validationManager;
  private final MetricsRegistry _metricsRegistry;
  private final PinotRealtimeSegmentsManager realtimeSegmentsManager;

  public ControllerStarter(ControllerConf conf) {
    config = conf;
    component = new Component();
    controllerRestApp = new ControllerRestApplication(config.getQueryConsole());
    helixResourceManager =
        new PinotHelixResourceManager(config.getZkStr(), config.getHelixClusterName(), config.getControllerHost() + "_"
            + config.getControllerPort(), config.getDataDir());
    retentionManager = new RetentionManager(helixResourceManager, config.getRetentionControllerFrequencyInSeconds());
    _metricsRegistry = new MetricsRegistry();
    ValidationMetrics validationMetrics = new ValidationMetrics(_metricsRegistry);
    validationManager = new ValidationManager(validationMetrics, helixResourceManager, config);
    realtimeSegmentsManager = new PinotRealtimeSegmentsManager(helixResourceManager);
  }

  public void start() {
    component.getServers().add(Protocol.HTTP, Integer.parseInt(config.getControllerPort()));
    component.getClients().add(Protocol.FILE);
    component.getClients().add(Protocol.JAR);
    component.getClients().add(Protocol.WAR);

    final Context applicationContext = component.getContext().createChildContext();

    logger.info("injecting conf and resource manager to the api context");
    applicationContext.getAttributes().put(ControllerConf.class.toString(), config);
    applicationContext.getAttributes().put(PinotHelixResourceManager.class.toString(), helixResourceManager);

    controllerRestApp.setContext(applicationContext);

    component.getDefaultHost().attach(controllerRestApp);

    try {
      logger.info("starting pinot helix resource manager");
      helixResourceManager.start();
      logger.info("starting api component");
      component.start();
      logger.info("starting retention manager");
      retentionManager.start();
      logger.info("starting validation manager");
      validationManager.start();
      logger.info("starting realtime segments manager");
      realtimeSegmentsManager.start();

    } catch (final Exception e) {
      logger.error(e);
      throw new RuntimeException(e);
    }

    MetricsHelper.initializeMetrics(config.subset("pinot.controller.metrics"));
    MetricsHelper.registerMetricsRegistry(_metricsRegistry);
  }

  public void stop() {
    try {
      logger.info("stopping validation manager");
      validationManager.stop();

      logger.info("stopping retention manager");
      retentionManager.stop();

      logger.info("stopping api component");
      component.stop();

      logger.info("stopping realtime segments manager");
      realtimeSegmentsManager.stop();

      logger.info("stopping resource manager");
      helixResourceManager.stop();

    } catch (final Exception e) {
      logger.error(e);
    }
  }

  public static void main(String[] args) throws InterruptedException {
    final ControllerConf conf = new ControllerConf();
    conf.setControllerHost("localhost");
    conf.setControllerPort("9000");
    conf.setDataDir("/tmp/PinotController");
    conf.setZkStr("localhost:2121");
    conf.setHelixClusterName("sprintDemoClusterOne");
    conf.setControllerVipHost("localhost");
    conf.setRetentionControllerFrequencyInSeconds(3600 * 6);
    conf.setValidationControllerFrequencyInSeconds(3600);

    final ControllerStarter starter = new ControllerStarter(conf);

    System.out.println(conf.getQueryConsole());
    starter.start();

  }
}
