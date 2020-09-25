/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package org.apache.pinot.thirdeye.cube.additive;

import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.cost.BalancedCostFunction;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.thirdeye.cube.entry.MultiDimensionalSummaryCLITool;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MultiDimensionalSummaryCLIToolTest {
  @Test
  public void testInitiateCostFunction()
      throws NoSuchMethodException, IOException, InstantiationException, IllegalAccessException,
             InvocationTargetException, ClassNotFoundException {
    String paramString = String.format("{\"className\":\"%s\"}", BalancedCostFunction.class.getName());

    MultiDimensionalSummaryCLITool.initiateCostFunction(paramString);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEmptyCostFunction()
      throws NoSuchMethodException, IOException, InstantiationException, IllegalAccessException,
             InvocationTargetException, ClassNotFoundException {
    String paramString = "{}";

    MultiDimensionalSummaryCLITool.initiateCostFunction(paramString);
  }

  @Test
  public void testSanitizeDimensions() {
    List<String> dims = Arrays.asList("environment", "page", "page" + MultiDimensionalSummaryCLITool.TOP_K_POSTFIX);
    Dimensions dimensions = new Dimensions(dims);
    Dimensions sanitizedDimensions = MultiDimensionalSummaryCLITool.sanitizeDimensions(dimensions);

    List<String> expectedSanitizedDims = Arrays.asList("page" + MultiDimensionalSummaryCLITool.TOP_K_POSTFIX);
    Dimensions expectedSanitizedDimensions = new Dimensions(expectedSanitizedDims);

    Assert.assertEquals(sanitizedDimensions, expectedSanitizedDimensions);
  }
}