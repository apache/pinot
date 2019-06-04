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

package org.apache.pinot.thirdeye.cube.data.cube;

import com.google.common.collect.Multimap;
import com.google.common.math.DoubleMath;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;


public class CubeUtils {
  private static double epsilon = 0.0001;

  /**
   * Removes dimensions from the given list of dimensions, which has single values in the filter set. Only dimensions
   * with one value is removed from the given dimensions because setting a filter one dimension names with one dimension
   * value (e.g., "country=US") implies that the final data cube does not contain other dimension values. Thus, the
   * summary algorithm could simply ignore that dimension (because the cube does not have any other values to compare
   * with in that dimension).
   *
   * @param dimensions the list of dimensions to be modified.
   * @param filterSets the filter to be applied on the data cube.
   *
   * @return the list of dimensions that should be used for retrieving the data for summary algorithm.
   */
  public static Dimensions shrinkDimensionsByFilterSets(Dimensions dimensions, Multimap<String, String> filterSets) {
    Set<String> dimensionsToRemove = new HashSet<>();
    for (Map.Entry<String, Collection<String>> filterSetEntry : filterSets.asMap().entrySet()) {
      if (filterSetEntry.getValue().size() == 1) {
        dimensionsToRemove.add(filterSetEntry.getKey());
      }
    }
    return removeDimensions(dimensions, dimensionsToRemove);
  }

  private static Dimensions removeDimensions(Dimensions dimensions, Collection<String> dimensionsToRemove) {
    List<String> dimensionsToRetain = new ArrayList<>();
    for (String dimensionName : dimensions.names()) {
      if(!dimensionsToRemove.contains(dimensionName)){
        dimensionsToRetain.add(dimensionName);
      }
    }
    return new Dimensions(dimensionsToRetain);
  }

  /**
   * Return the results of a minus b. If the result is very close to zero, then zero is returned.
   * This method is use to prevent the precision issue of double from inducing -0.00000000000000001, which is
   * actually zero.
   *
   * @param a a double value.
   * @param b the other double value.
   * @return the results of a minus b.
   */
  public static double doubleMinus(double a, double b) {
    double ret = a - b;
    if (DoubleMath.fuzzyEquals(ret, 0, epsilon)) {
      return 0.0;
    } else {
      return ret;
    }
  }

  /**
   * Flips parent's change ratio if the change ratios of current node and its parent are different.
   *
   * @param baselineValue the baseline value of a node.
   * @param currentValue the current value of a node.
   * @param ratio the (parent) ratio to be flipped.
   *
   * @return the ratio that has the same direction as the change direction of baseline and current value.
   */
  public static double ensureChangeRatioDirection(double baselineValue, double currentValue, double ratio) {
    // case: value goes down but parent's value goes up
    if (DoubleMath.fuzzyCompare(baselineValue, currentValue, epsilon) > 0 && DoubleMath.fuzzyCompare(ratio, 1, epsilon) > 0) {
      if (Double.compare(ratio, 2) >= 0) {
        ratio = 2d - (ratio - ((long) ratio - 1));
      } else {
        ratio = 2d - ratio;
      }
      // case: value goes up but parent's value goes down
    } else if (DoubleMath.fuzzyCompare(baselineValue, currentValue, epsilon) < 0 && DoubleMath.fuzzyCompare(ratio, 1, epsilon) < 0) {
      ratio = 2d - ratio;
    }
    // return the original ratio for other cases.
    return ratio;
  }
}
