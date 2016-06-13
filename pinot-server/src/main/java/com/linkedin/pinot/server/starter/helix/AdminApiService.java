/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.server.starter.helix;

import com.linkedin.pinot.server.api.restlet.PinotAdminEndpointApplication;
import com.linkedin.pinot.server.starter.ServerInstance;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AdminApiService {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdminApiService.class);
  ServerInstance serverInstance;
  Component adminApiComponent;
  boolean started = false;

  public AdminApiService(ServerInstance serverInstance) {
    this.serverInstance = serverInstance;
    adminApiComponent = new Component();
  }

  public boolean start(int httpPort) {
    if (httpPort <= 0 ) {
      LOGGER.warn("Invalid admin API port: {}. Not starting admin service", httpPort);
      return false;
    }
    adminApiComponent.getServers().add(Protocol.HTTP, httpPort);
    adminApiComponent.getClients().add(Protocol.FILE);
    adminApiComponent.getClients().add(Protocol.JAR);
    adminApiComponent.getClients().add(Protocol.WAR);

    PinotAdminEndpointApplication adminEndpointApplication = new PinotAdminEndpointApplication();
    final Context applicationContext = adminApiComponent.getContext().createChildContext();
    adminEndpointApplication.setContext(applicationContext);
    applicationContext.getAttributes().put(ServerInstance.class.toString(), serverInstance);

    adminApiComponent.getDefaultHost().attach(adminEndpointApplication);
    LOGGER.info("Will start admin API endpoint on port {}", httpPort);
    try {
      adminApiComponent.start();
      started = true;
      return true;
    } catch (Exception e) {
      LOGGER.warn("Failed to start admin service. Continuing with errors", e);
      return false;
    }
  }

  public void stop(){
    if (!started) {
      return;
    }
    try {
      adminApiComponent.stop();
    } catch (Exception e) {
      LOGGER.warn("Failed to stop admin API component. Continuing with errors", e);
    }
  }
}
