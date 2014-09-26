package com.linkedin.pinot.controller;

import org.apache.log4j.Logger;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Protocol;

import com.linkedin.pinot.controller.api.ControllerRestApplication;
import com.linkedin.pinot.controller.api.resources.PinotResource;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 24, 2014
 */

public class ControllerStarter {
  private static final Logger logger = Logger.getLogger(ControllerStarter.class);
  private final int port;
  private Component component;
  private Application controllerRestApp;

  public ControllerStarter() {
    port = 8998;
  }

  public void init() {
    component = new Component();
    controllerRestApp = new ControllerRestApplication();
  }

  public void start() {
    component.getServers().add(Protocol.HTTP, port);
    final Context applicationContext = component.getContext().createChildContext();

    // this is how you inject stuff
    applicationContext.getAttributes().put("pinotresrouceManbager", new PinotResource());

    component.getDefaultHost().attach(controllerRestApp);
    try {
      component.start();
    } catch (final Exception e) {
      logger.error(e);
    }
  }

  public void stop() {
    try {
      component.stop();
    } catch (final Exception e) {
      logger.error(e);
    }
  }

  public static void main(String[] args) throws InterruptedException {
    final ControllerStarter starter = new ControllerStarter();
    starter.init();
    starter.start();

    Thread.sleep(60000);
    starter.stop();

  }
}
