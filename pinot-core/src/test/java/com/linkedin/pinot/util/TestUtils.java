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
package com.linkedin.pinot.util;

import com.google.common.base.Function;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.response.broker.GroupByResult;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.RecordReader;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.json.JSONArray;
import org.json.JSONException;
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

  public static void assertGroupByResultsApproximation(
      List<GroupByResult> estimateValues, List<GroupByResult> actualValues, double precision) {
    LOGGER.info("====== assertGroupByResultsApproximation ======");
    // estimation should not affect number of groups formed
    Assert.assertEquals(estimateValues.size(), actualValues.size());

    Map<List<String>, Double> mapEstimate = new HashMap<>();
    Map<List<String>, Double> mapActual = new HashMap<>();
    for (GroupByResult gby: estimateValues) {
      mapEstimate.put(gby.getGroup(), Double.parseDouble(gby.getValue().toString()));
    }
    for (GroupByResult gby: actualValues) {
      mapActual.put(gby.getGroup(), Double.parseDouble(gby.getValue().toString()));
    }

    LOGGER.info("estimate: " + mapEstimate.keySet());
    LOGGER.info("actual: " + mapActual.keySet());

    int cnt = 0;
    for (List<String> key: mapEstimate.keySet()) {
      // Not strictly enforced, since in quantile, top 100 groups from accurate maybe not be top 100 from estimate
      // Assert.assertEquals(mapActual.keySet().contains(key), true);
      if (mapActual.keySet().contains(key)) {
        assertApproximation(mapEstimate.get(key), mapActual.get(key), precision);
        cnt += 1;
      }
    }
    LOGGER.info("group overlap rate: " + (cnt+0.0)/mapEstimate.keySet().size());
  }

  public static void assertJSONArrayApproximation(JSONArray jsonArrayEstimate, JSONArray jsonArrayActual, double precision) {
    LOGGER.info("====== assertJSONArrayApproximation ======");
    try {
      HashMap<String, Double> mapEstimate = genMapFromJSONArray(jsonArrayEstimate);
      HashMap<String, Double> mapActual = genMapFromJSONArray(jsonArrayActual);

      // estimation should not affect number of groups formed
      Assert.assertEquals(mapEstimate.keySet().size(), mapActual.keySet().size());
      LOGGER.info("estimate: " + mapEstimate.keySet());
      LOGGER.info("actual: " + mapActual.keySet());
      int cnt = 0;
      for (String key: mapEstimate.keySet()) {
        // Not strictly enforced, since in quantile, top 100 groups from accurate maybe not be top 100 from estimate
        // Assert.assertEquals(mapActual.keySet().contains(key), true);
        if (mapActual.keySet().contains(key)) {
          assertApproximation(mapEstimate.get(key), mapActual.get(key), precision);
          cnt += 1;
        }
      }
      LOGGER.info("group overlap rate: " + (cnt+0.0)/mapEstimate.keySet().size());
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

  private static HashMap<String, Double> genMapFromJSONArray(JSONArray array) throws JSONException {
    HashMap<String, Double> map = new HashMap<String, Double>();
    for (int i = 0; i < array.length(); ++i) {
      map.put(array.getJSONObject(i).getJSONArray("group").getString(0),
          array.getJSONObject(i).getDouble("value"));
    }
    return map;
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
  public static void waitForCondition(@Nonnull Function<Void, Boolean> condition, long checkIntervalMs, long timeoutMs,
      @Nullable String errorMessage)
      throws Exception {
    long endTime = System.currentTimeMillis() + timeoutMs;
    try {
      while (System.currentTimeMillis() < endTime) {
        Boolean isConditionMet = condition.apply(null);
        if ((isConditionMet != null) && isConditionMet) {
          return;
        }
        Thread.sleep(checkIntervalMs);
      }
      if (errorMessage != null) {
        Assert.fail("Failed to meet condition in " + timeoutMs + "ms, error message: " + errorMessage);
      } else {
        Assert.fail("Failed to meet condition in " + timeoutMs + "ms");
      }
    } catch (Exception e) {
      if (errorMessage != null) {
        Assert.fail("Caught exception while checking the condition, error message: " + errorMessage, e);
      } else {
        Assert.fail("Caught exception while checking the condition", e);
      }
    }
  }

  public static void waitForCondition(@Nonnull Function<Void, Boolean> condition, long timeoutMs,
      @Nullable String errorMessage)
      throws Exception {
    waitForCondition(condition, 1000L, timeoutMs, errorMessage);
  }

  /**
   * Utility class for reading generic row records
   */
  public static class GenericRowRecordReader implements RecordReader {

    private final Schema _schema;
    private final List<GenericRow> _data;
    int counter = 0;

    // Constructor for the class.
    public GenericRowRecordReader(final Schema schema, final List<GenericRow> data) {
      _schema = schema;
      _data = data;
    }

    @Override
    public void rewind()
        throws Exception {
      counter = 0;
    }

    @Override
    public GenericRow next() {
      return _data.get(counter++);
    }

    @Override
    public GenericRow next(GenericRow row) {
      return next();
    }

    @Override
    public void init()
        throws Exception {

    }

    @Override
    public boolean hasNext() {
      return counter < _data.size();
    }

    @Override
    public Schema getSchema() {
      return _schema;
    }

    @Override
    public Map<String, MutableLong> getNullCountMap() {
      return null;
    }

    @Override
    public void close()
        throws Exception {

    }
  }
}
