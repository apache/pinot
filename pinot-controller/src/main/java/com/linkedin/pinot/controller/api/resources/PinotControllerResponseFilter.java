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

package com.linkedin.pinot.controller.api.resources;

import java.io.IOException;
import java.util.Map;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.utils.NetUtil;
import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;


// A class to add the controller host and version in the response headers for all APIs.
@Singleton
@Provider
public class PinotControllerResponseFilter implements ContainerResponseFilter {
  private final static String CONTROLLER_COMPONENT = "pinot-controller";
  private static final String HDR_CONTROLLER_VERSION = "Pinot-Controller-Version";
  private final static String HDR_CONTROLLER_HOST = "Pinot-Controller-Host";

  private volatile static String controllerHostName = null;
  private volatile static String controllerVersion = null;

  @Override
  public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
      throws IOException {
    responseContext.getHeaders().putSingle(HDR_CONTROLLER_HOST, getControllerHostName());
    responseContext.getHeaders().putSingle(HDR_CONTROLLER_VERSION, getHdrControllerVersion());
  }

  private String getControllerHostName() {
    if (controllerHostName != null) {
      return controllerHostName;
    }

    synchronized (this) {
      if (controllerHostName != null) {
        return controllerHostName;
      }
      controllerHostName = NetUtil.getHostnameOrAddress();
      if (controllerHostName == null) {
        // In case of a temporary failure, we will go back to getting the right value again.
        return "unknown";
      }
      return controllerHostName;
    }
  }

  private String getHdrControllerVersion() {
    if (controllerVersion != null) {
      return controllerVersion;
    }
    synchronized (this) {
      if (controllerVersion != null) {
        return controllerVersion;
      }
      Map<String, String> versions = Utils.getComponentVersions();
      controllerVersion = versions.get(CONTROLLER_COMPONENT);
      if (controllerVersion == null) {
        controllerVersion = "Unknown";
      }
      return controllerVersion;
    }
  }
}
