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
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import com.google.common.collect.BiMap;
import it.unimi.dsi.fastutil.ints.IntComparator;


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
  public static int[] sortStarTreeData(int startDocId, int endDocId, List<Integer> sortOrder, List<List<Object>> starTreeData) {
    int docsCount = endDocId - startDocId;
    int[] sortedDocIds = new int[docsCount];
    for (int i = 0; i < docsCount; i++) {
      sortedDocIds[i] = i + startDocId;
    }

    IntComparator comparator = new IntComparator() {
      @Override
      public int compare(int i1, int i2) {
        int docId1 = sortedDocIds[i1];
        int docId2 = sortedDocIds[i2];

        int compare = 0;
        for (int index : sortOrder) {
          List<Object> column = starTreeData.get(index);
          compare = (int)column.get(docId1) - (int)column.get(docId2);
          if (compare != 0) {
            return compare;
          }
        }
        return compare;
      }

      @Override
      public int compare(Integer o1, Integer o2) {
        throw new UnsupportedOperationException();
      }
    };

    Swapper swapper = new Swapper() {
      @Override
      public void swap(int i, int j) {
        int temp = sortedDocIds[i];
        sortedDocIds[i] = sortedDocIds[j];
        sortedDocIds[j] = temp;
      }
    };
    Arrays.quickSort(0, docsCount, comparator, swapper);

    return sortedDocIds;
  }

  /**
   * function to rearrange documents according to sorted order.
   */
  public static List<List<Object>> reArrangeStarTreeData(int [] sortOrder, List<List<Object>> starTreeData) {
    List<List<Object>> newData = new ArrayList<>();
    for (List<Object> col: starTreeData) {
      List<Object> newCol = new ArrayList<>();
      for (int id: sortOrder) {
        newCol.add(col.get(id));
      }
      newData.add(newCol);
    }
    return newData;
  }

  /**
   * function to condense documents according to sorted order.
   */
  public static List<List<Object>> condenseData(List<List<Object>> starTreeData) {
    List<List<Object>> newData = new ArrayList<>();
    for ( int i = 0; i < starTreeData.size(); i++) {
      List<Object>col =  new ArrayList<>();
      col.add(starTreeData.get(i).get(0));
      newData.add(col);
    }

    for ( int i = 1; i < starTreeData.get(0).size(); i++) {
      boolean flag = false;
      for ( int j = 0; j < starTreeData.size(); j++) {
        Object prev = starTreeData.get(j).get(i-1);
        Object current = starTreeData.get(j).get(i);
        if ( !prev.equals(current)) {
          flag = true;
          break;
        }
      }
      if (flag == true) {
        for ( int k = 0; k < starTreeData.size(); k++) {
          newData.get(k).add(starTreeData.get(k).get(i));
        }
      }
    }

    return newData;
  }

  /**
   * Filter data by removing the dimension we don't need.
   */
  public static List<List<Object>> filterData(int startDocId, int endDocId, int dimensionIdToRemove, List<Integer>sortOrder, List<List<Object>>starTreeData) {

    List<List<Object>> newFilteredData = new ArrayList<>();
    for (int i = 0; i < starTreeData.size(); i++) {
      List<Object> col = starTreeData.get(i);
      List<Object> newCol = new ArrayList<>();
      for (int j = startDocId; j < endDocId; j++) {
        if (i != dimensionIdToRemove) {
          newCol.add(col.get(j));
        } else {
          newCol.add(StarTreeV2Constant.STAR_NODE);
        }
      }
      newFilteredData.add(newCol);
    }
    int[] sortedDocId = sortStarTreeData(0, endDocId - startDocId, sortOrder, newFilteredData);
    return reArrangeStarTreeData(sortedDocId, newFilteredData);
  }
}
