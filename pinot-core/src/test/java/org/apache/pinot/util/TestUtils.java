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
package org.apache.pinot.util;

import com.google.common.base.Function;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;


/**
 * Various utilities for unit tests.
 *
 */
public class TestUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);
  private static final int testThreshold = 1000;

  public static final double hllEstimationThreshold = 0.5;
  public static final double digestEstimationThreshold = 0.1;

  public static String getFileFromResourceUrl(@Nonnull URL resourceUrl) {
    // For maven cross package use case, we need to extract the resource from jar to a temporary directory.
    String resourceUrlStr = resourceUrl.toString();
    if (resourceUrlStr.contains("jar!")) {
      try {
        String extension = resourceUrlStr.substring(resourceUrlStr.lastIndexOf('.'));
        File tempFile = File.createTempFile("pinot-test-temp", extension);
        String tempFilePath = tempFile.getAbsolutePath();
        LOGGER.info("Extracting from " + resourceUrlStr + " to " + tempFilePath);
        FileUtils.copyURLToFile(resourceUrl, tempFile);
        return tempFilePath;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      return resourceUrl.getFile();
    }
  }

  /**
   * assert estimation in error range
   * @param estimate
   * @param actual
   */
  public static void assertApproximation(double estimate, double actual, double precision) {
    estimate = Math.abs(estimate);
    actual = Math.abs(actual);

    if (estimate < testThreshold && actual < testThreshold) {
      double errorDiff = Math.abs(actual - estimate);
      LOGGER.debug("estimate: " + estimate + " actual: " + actual + " error (in difference): " + errorDiff);
      LOGGER.debug("small value comparison ignored!");
      //Assert.assertEquals(error < 3, true);
    } else {
      double errorRate = 1;
      if (actual > 0) {
        errorRate = Math.abs((actual - estimate) / actual);
      }
      LOGGER.debug("estimate: " + estimate + " actual: " + actual + " error (in rate): " + errorRate);
      Assert.assertTrue(errorRate < precision);
    }
  }

  public static void assertGroupByResultsApproximation(List<GroupByResult> estimateValues,
      List<GroupByResult> actualValues, double precision) {
    LOGGER.info("====== assertGroupByResultsApproximation ======");
    // estimation should not affect number of groups formed
    Assert.assertEquals(estimateValues.size(), actualValues.size());

    Map<List<String>, Double> mapEstimate = new HashMap<>();
    Map<List<String>, Double> mapActual = new HashMap<>();
    for (GroupByResult gby : estimateValues) {
      mapEstimate.put(gby.getGroup(), Double.parseDouble(gby.getValue().toString()));
    }
    for (GroupByResult gby : actualValues) {
      mapActual.put(gby.getGroup(), Double.parseDouble(gby.getValue().toString()));
    }

    LOGGER.info("estimate: " + mapEstimate.keySet());
    LOGGER.info("actual: " + mapActual.keySet());

    int cnt = 0;
    for (List<String> key : mapEstimate.keySet()) {
      // Not strictly enforced, since in quantile, top 100 groups from accurate maybe not be top 100 from estimate
      // Assert.assertEquals(mapActual.keySet().contains(key), true);
      if (mapActual.keySet().contains(key)) {
        assertApproximation(mapEstimate.get(key), mapActual.get(key), precision);
        cnt += 1;
      }
    }
    LOGGER.info("group overlap rate: " + (cnt + 0.0) / mapEstimate.keySet().size());
  }

  /**
   * Ensure the given directories exist and are empty.
   *
   * @param dirs Directories to be cleared
   * @throws IOException
   */
  public static void ensureDirectoriesExistAndEmpty(@Nonnull File... dirs)
      throws IOException {
    for (File dir : dirs) {
      FileUtils.deleteDirectory(dir);
      Assert.assertTrue(dir.mkdirs());
    }
  }

  /**
   * Wait for a condition to be met.
   *
   * @param condition Condition to be met
   * @param checkIntervalMs Check interval in milliseconds
   * @param timeoutMs Timeout in milliseconds
   * @param errorMessage Error message if condition is not met before timed out
   */
  public static void waitForCondition(Function<Void, Boolean> condition, long checkIntervalMs, long timeoutMs,
      @Nullable String errorMessage) {
    long endTime = System.currentTimeMillis() + timeoutMs;
    String errorMessageSuffix = errorMessage != null ? ", error message: " + errorMessage : "";
    while (System.currentTimeMillis() < endTime) {
      try {
        if (Boolean.TRUE.equals(condition.apply(null))) {
          return;
        }
        Thread.sleep(checkIntervalMs);
      } catch (Exception e) {
        Assert.fail("Caught exception while checking the condition" + errorMessageSuffix, e);
      }
    }
    Assert.fail("Failed to meet condition in " + timeoutMs + "ms" + errorMessageSuffix);
  }

  public static void waitForCondition(Function<Void, Boolean> condition, long timeoutMs,
      @Nullable String errorMessage) {
    waitForCondition(condition, 1000L, timeoutMs, errorMessage);
  }
}
