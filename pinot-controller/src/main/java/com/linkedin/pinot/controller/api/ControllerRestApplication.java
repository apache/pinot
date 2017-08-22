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
package com.linkedin.pinot.controller.api;

import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.restlet.PinotRestletApplication;
import org.restlet.resource.ServerResource;
import org.restlet.routing.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControllerRestApplication extends PinotRestletApplication {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerRestApplication.class);
  private static final String ONE_HOUR_IN_MILLIS = Long.toString(3600L * 1000L);

  private static String CONSOLE_WEBAPP_ROOT_PATH;

  private static Router router;

  private static ControllerMetrics metrics;

  public ControllerRestApplication(String queryConsolePath) {
    super();
    CONSOLE_WEBAPP_ROOT_PATH = queryConsolePath;
  }

  public static void setControllerMetrics(ControllerMetrics controllerMetrics) {
    metrics = controllerMetrics;
  }

  public static ControllerMetrics getControllerMetrics() { return metrics; }

  protected void configureRouter(Router router) {
  }

  @Override
  protected void attachRoute(Router router, String routePath, Class<? extends ServerResource> clazz) {
  }

  /*

  private class AddHeaderFilter extends Filter {
    public AddHeaderFilter(Context context, Restlet next) {
      super(context, next);
    }

    @Override
    protected int beforeHandle(Request request, Response response) {
      BasePinotControllerRestletResource.addExtraHeaders(response);
      return Filter.CONTINUE;
    }
  }
   */

  public static Router getRouter() {
    return router;
  }

}
