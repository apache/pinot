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
package org.apache.pinot.core.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.spi.utils.Pairs;
import org.apache.pinot.spi.utils.Pairs.IntPair;


/**
 * Utility to perform intersection of sorted ranges
 */
public class SortedRangeIntersection {

  public static List<IntPair> intersectSortedRangeSets(List<List<IntPair>> sortedRangeSetList) {
    if (sortedRangeSetList == null || sortedRangeSetList.size() == 0) {
      return Collections.emptyList();
    }
    if (sortedRangeSetList.size() == 1) {
      return sortedRangeSetList.get(0);
    }
    // if any list is empty return empty
    for (List<IntPair> rangeSet : sortedRangeSetList) {
      if (rangeSet.size() == 0) {
        return Collections.emptyList();
      }
    }
    int[] currentRangeSetIndex = new int[sortedRangeSetList.size()];
    Arrays.fill(currentRangeSetIndex, 0);
    int maxHead = -1;
    int maxHeadIndex = -1;
    boolean reachedEnd = false;
    List<IntPair> result = new ArrayList<IntPair>();
    while (!reachedEnd) {
      // find max Head in the current pointers
      for (int i = 0; i < sortedRangeSetList.size(); i++) {
        int head = sortedRangeSetList.get(i).get(currentRangeSetIndex[i]).getLeft();
        if (head > maxHead) {
          maxHead = head;
          maxHeadIndex = i;
        }
      }
      // move all pointers forward such that range they point to contain maxHead
      for (int i = 0; i < sortedRangeSetList.size(); i++) {
        if (i == maxHeadIndex) {
          continue;
        }
        boolean found = false;
        while (!found && currentRangeSetIndex[i] < sortedRangeSetList.get(i).size()) {
          IntPair range = sortedRangeSetList.get(i).get(currentRangeSetIndex[i]);
          if (maxHead >= range.getLeft() && maxHead <= range.getRight()) {
            found = true;
            break;
          }
          if (range.getLeft() > maxHead) {
            maxHead = range.getLeft();
            maxHeadIndex = i;
            i = -1;
            break;
          }
          currentRangeSetIndex[i] = currentRangeSetIndex[i] + 1;
        }
        // new maxHead found
        if (i == -1) {
          continue;
        }
        if (!found) {
          reachedEnd = true;
          break;
        }
      }

      if (reachedEnd) {
        break;
      }
      // there is definitely some intersection possible here
      IntPair intPair = sortedRangeSetList.get(0).get(currentRangeSetIndex[0]);
      IntPair intersection = Pairs.intPair(intPair.getLeft(), intPair.getRight());
      for (int i = 1; i < sortedRangeSetList.size(); i++) {
        IntPair pair = sortedRangeSetList.get(i).get(currentRangeSetIndex[i]);
        int start = Math.max(intersection.getLeft(), pair.getLeft());
        int end = Math.min(intersection.getRight(), pair.getRight());
        intersection.setLeft(start);
        intersection.setRight(end);
      }
      if (result.size() > 0) {
        // if new range is contiguous merge it
        IntPair prevIntersection = result.get(result.size() - 1);
        if (intersection.getLeft() == prevIntersection.getRight() + 1) {
          prevIntersection.setRight(intersection.getRight());
        } else {
          result.add(intersection);
        }
      } else {
        result.add(intersection);
      }
      // move the pointers forward for rangesets where the currenttail == intersection.tail
      for (int i = 0; i < sortedRangeSetList.size(); i++) {
        IntPair pair = sortedRangeSetList.get(i).get(currentRangeSetIndex[i]);
        if (pair.getRight() == intersection.getRight()) {
          currentRangeSetIndex[i] = currentRangeSetIndex[i] + 1;
          if (currentRangeSetIndex[i] == sortedRangeSetList.get(i).size()) {
            reachedEnd = true;
            break;
          }
        }
      }
    }
    return result;
  }
}
