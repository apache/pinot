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

package org.apache.pinot.thirdeye.dashboard;


import com.google.inject.Provider;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;
import org.apache.pinot.thirdeye.dashboard.resources.v2.RootCauseResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.rootcause.DefaultEntityFormatter;
import org.apache.pinot.thirdeye.dashboard.resources.v2.rootcause.FormatterLoader;
import org.apache.pinot.thirdeye.rootcause.RCAFramework;
import org.apache.pinot.thirdeye.rootcause.impl.RCAFrameworkLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RootCauseResourceProvider implements Provider<RootCauseResource> {

  private static final Logger LOG = LoggerFactory.getLogger(RootCauseResourceProvider.class);
  private final ThirdEyeDashboardConfiguration config;

  public RootCauseResourceProvider(
      final ThirdEyeDashboardConfiguration config) {
    this.config = config;
  }

  private static File getRootCauseDefinitionsFile(ThirdEyeDashboardConfiguration config) {
    if (config.getRootCause().getDefinitionsPath() == null) {
      throw new IllegalArgumentException("definitionsPath must not be null");
    }
    File rcaConfigFile = new File(config.getRootCause().getDefinitionsPath());
    if (!rcaConfigFile.isAbsolute()) {
      return new File(config.getRootDir() + File.separator + rcaConfigFile);
    }
    return rcaConfigFile;
  }

  private static Map<String, RCAFramework> makeRootCauseFrameworks(RootCauseConfiguration config,
      File definitionsFile) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(config.getParallelism());
    return RCAFrameworkLoader.getFrameworksFromConfig(definitionsFile, executor);
  }

  private static List<RootCauseEntityFormatter> makeRootCauseFormatters(
      RootCauseConfiguration config) throws Exception {
    List<RootCauseEntityFormatter> formatters = new ArrayList<>();
    if (config.getFormatters() != null) {
      for (String className : config.getFormatters()) {
        try {
          formatters.add(FormatterLoader.fromClassName(className));
        } catch (ClassNotFoundException e) {
          LOG.warn("Could not find formatter class '{}'. Skipping.", className, e);
        }
      }
    }
    formatters.add(new DefaultEntityFormatter());
    return formatters;
  }

  private RootCauseResource makeRootCauseResource() throws Exception {
    File definitionsFile = getRootCauseDefinitionsFile(config);
    if (!definitionsFile.exists()) {
      throw new IllegalArgumentException(
          String.format("Could not find definitions file '%s'", definitionsFile));
    }

    RootCauseConfiguration rcConfig = config.getRootCause();
    return new RootCauseResource(
        makeRootCauseFrameworks(rcConfig, definitionsFile),
        makeRootCauseFormatters(rcConfig));
  }

  @Override
  public RootCauseResource get() {
    try {
      return makeRootCauseResource();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }
}
