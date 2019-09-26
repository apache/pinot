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

package org.apache.pinot.thirdeye.anomaly.alert.util;

import org.apache.pinot.thirdeye.common.ThirdEyeConfiguration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AlertScreenshotHelper {
  private static final Logger LOG = LoggerFactory.getLogger(AlertScreenshotHelper.class);

  private static final String TEMP_PATH = "/tmp/graph";
  private static final String SCREENSHOT_FILE_SUFFIX = ".png";
  private static final String GRAPH_SCREENSHOT_GENERATOR_SCRIPT = "/getGraphPnj.js";
  private static final ExecutorService executorService = Executors.newCachedThreadPool();

  public static String takeGraphScreenShot(final String anomalyId, final ThirdEyeConfiguration configuration) throws JobExecutionException {
    return takeGraphScreenShot(anomalyId, configuration.getDashboardHost(), configuration.getRootDir(),
        configuration.getPhantomJsPath());
  }

  public static String takeGraphScreenShot(final String anomalyId, final String dashboardHost, final String rootDir,
      final String phantomJsPath) {
    Callable<String> callable = () -> takeScreenshot(anomalyId, dashboardHost, rootDir, phantomJsPath);
    Future<String> task = executorService.submit(callable);
    String result = null;
    try {
      result = task.get(3, TimeUnit.MINUTES);
      LOG.info("Finished with result: {}", result);
    } catch (Exception e) {
      LOG.error("Exception in fetching screenshot for anomaly id {}", anomalyId, e);
    }
    return result;
  }

  private static String takeScreenshot(String anomalyId, String dashboardHost, String rootDir, String phantomJsPath) throws Exception {
    String imgRoute = dashboardHost + "/app/#/screenshot/" + anomalyId;
    LOG.info("imgRoute {}", imgRoute);
    String phantomScript = rootDir + GRAPH_SCREENSHOT_GENERATOR_SCRIPT;
    LOG.info("Phantom JS script {}", phantomScript);
    String imgPath = TEMP_PATH + anomalyId + SCREENSHOT_FILE_SUFFIX;
    LOG.info("imgPath {}", imgPath);
    Process proc = Runtime.getRuntime().exec(new String[]{phantomJsPath, "phantomjs", "--ssl-protocol=any", "--ignore-ssl-errors=true",
        phantomScript, imgRoute, imgPath});
    LOG.info("Waiting for phantomjs...");
    boolean isComplete = proc.waitFor(2, TimeUnit.MINUTES);
    LOG.info("phantomjs complete status after waiting: {}", isComplete);
    if (!isComplete) {
      proc.destroyForcibly();
      throw new Exception("PhantomJS process timeout");
    }
    return imgPath;
  }
}
