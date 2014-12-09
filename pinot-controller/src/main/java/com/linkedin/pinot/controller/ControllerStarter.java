package com.linkedin.pinot.controller;

import org.apache.log4j.Logger;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Protocol;

import com.linkedin.pinot.controller.api.ControllerRestApplication;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;


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

  public ControllerStarter(ControllerConf conf) {
    config = conf;
    component = new Component();
    controllerRestApp = new ControllerRestApplication(config.getQueryConsole());
    helixResourceManager =
        new PinotHelixResourceManager(config.getZkStr(), config.getHelixClusterName(), config.getControllerHost() + "_"
            + config.getControllerPort());
  }

  public void start() {
    component.getServers().add(Protocol.HTTP, Integer.parseInt(config.getControllerPort()));
    component.getClients().add(Protocol.FILE);

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

    } catch (final Exception e) {
      logger.error(e);
      throw new RuntimeException(e);
    }
  }

  public void stop() {
    try {
      logger.info("stopping api component");
      component.stop();

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
    conf.setControllerVipHost("localhost:8998");

    final ControllerStarter starter = new ControllerStarter(conf);
    starter.start();

  }
}
