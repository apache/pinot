/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import static org.testng.Assert.assertEquals;


/**
 * Various utilities for unit tests.
 *
 */
public class TestUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);
  private static final int testThreshold = 1000;

  public static String getFileFromResourceUrl(URL resourceUrl) {
    System.out.println(resourceUrl);
    // Check if we need to extract the resource to a temporary directory
    String resourceUrlStr = resourceUrl.toString();
    if (resourceUrlStr.contains("jar!")) {
      try {
        String extension = resourceUrlStr.substring(resourceUrlStr.lastIndexOf('.'));
        File tempFile = File.createTempFile("pinot-test-temp", extension);
        LOGGER.info("Extractng from " + resourceUrlStr + " to " + tempFile.getAbsolutePath());
        System.out.println("Extractng from " + resourceUrlStr + " to " + tempFile.getAbsolutePath());
        FileUtils.copyURLToFile(resourceUrl, tempFile);
        return tempFile.getAbsolutePath();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      System.out.println("Not extracting plain file " + resourceUrl);
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
      LOGGER.info("estimate: " + estimate + " actual: " + actual + " error (in difference): " + errorDiff);
      LOGGER.info("small value comparison ignored!");
      //Assert.assertEquals(error < 3, true);
    } else {
      double errorRate = 1;
      if (actual > 0) {
        errorRate = Math.abs((actual - estimate) / actual);
      }
      LOGGER.info("estimate: " + estimate + " actual: " + actual + " error (in rate): " + errorRate);
      Assert.assertEquals(errorRate < precision, true);
    }
  }

  /*
  public static void assertQuantileApproximation(double estimate, double actual, double precision, byte quantile) {
    assertApproximation(estimate, (actual+0.0)*(quantile+0.0)/100, precision);
  }*/

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
}
