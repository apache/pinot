/**
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
package org.apache.pinot.common.swagger;

import io.swagger.jaxrs.config.BeanConfig;
import java.net.URL;
import java.net.URLClassLoader;
import org.apache.pinot.common.utils.PinotStaticHttpHandler;
import org.apache.pinot.spi.utils.CommonConstants;
import org.glassfish.grizzly.http.server.CLStaticHttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;


public class SwaggerSetupUtils {
  private SwaggerSetupUtils() {
  }

  public static void setupSwagger(String componentType, String resourcePackage, boolean useHttps, String basePath,
      HttpServer httpServer) {
    BeanConfig beanConfig = new BeanConfig();
    beanConfig.setTitle(String.format("Pinot %s API", componentType));
    beanConfig.setDescription(String.format("APIs for accessing Pinot %s information", componentType));
    beanConfig.setContact("https://github.com/apache/pinot");
    beanConfig.setVersion("1.0");
    beanConfig.setExpandSuperTypes(false);
    if (useHttps) {
      beanConfig.setSchemes(new String[]{CommonConstants.HTTPS_PROTOCOL});
    } else {
      beanConfig.setSchemes(new String[]{CommonConstants.HTTP_PROTOCOL, CommonConstants.HTTPS_PROTOCOL});
    }
    beanConfig.setBasePath(basePath);
    beanConfig.setResourcePackage(resourcePackage);
    beanConfig.setScan(true);

    ClassLoader classLoader = SwaggerSetupUtils.class.getClassLoader();
    CLStaticHttpHandler staticHttpHandler = new CLStaticHttpHandler(classLoader, "/api/");
    // map both /api and /help to swagger docs. /api because it looks nice. /help for backward compatibility
    httpServer.getServerConfiguration().addHttpHandler(staticHttpHandler, "/api/", "/help/");

    URL swaggerDistLocation = classLoader.getResource(CommonConstants.CONFIG_OF_SWAGGER_RESOURCES_PATH);
    CLStaticHttpHandler swaggerDist = new PinotStaticHttpHandler(new URLClassLoader(new URL[]{swaggerDistLocation}));
    httpServer.getServerConfiguration().addHttpHandler(swaggerDist, "/swaggerui-dist/");
  }
}
