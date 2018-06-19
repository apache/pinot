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

package com.linkedin.pinot.core.startreeV2;

import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import com.google.common.collect.BiMap;


public class OnHeapStarTreeV2BuilderHelper {

  /**
   * enumerate dimension set.
   */
  public static List<Integer> enumerateDimensions(List<String>dimensionNames, List<String>dimensionsOrder) {
    List<Integer> enumeratedDimensions = new ArrayList<>();
    if (dimensionsOrder != null) {
      for (String dimensionName : dimensionsOrder) {
        enumeratedDimensions.add(dimensionNames.indexOf(dimensionName));
      }
    }

    return enumeratedDimensions;
  }

  /**
   * compute a defualt split order.
   */
  public static List<Integer> computeDefaultSplitOrder(int dimensionsCount, List<BiMap<Object, Integer>> dimensionDictionaries) {
    List<Integer> defaultSplitOrder = new ArrayList<>();
    for (int i = 0; i < dimensionsCount; i++) {
      defaultSplitOrder.add(i);
    }

    Collections.sort(defaultSplitOrder, new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return dimensionDictionaries.get(o2).size() - dimensionDictionaries.get(o1).size();
      }
    });

    return defaultSplitOrder;
  }

  /**
   * sort the star tree data.
   */
  public static void sortStarTreeData() {

  }
}
