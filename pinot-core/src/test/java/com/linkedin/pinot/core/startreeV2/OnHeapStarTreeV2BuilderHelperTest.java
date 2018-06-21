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
import java.util.Arrays;
import org.testng.Assert;
import java.util.ArrayList;
import org.testng.annotations.Test;


public class OnHeapStarTreeV2BuilderHelperTest {


  public List<List<Object>> createData() {
    List<List<Object>> data = new ArrayList<>();
    data.add(Arrays.asList(1, 1, 1, 1, 1, 2, 2, 2));
    data.add(Arrays.asList(1, 1, 1, 2, 2, 1, 1, 2));
    data.add(Arrays.asList(0, 0, 0, 1, 1, 0, 1, 2));
    return data;
  }

  @Test
  public void testSortStarTreeData() {
    List<List<Object>> data = new ArrayList<>();
    data.add(Arrays.asList(4, 5, 1, 1, 1, 4));
    data.add(Arrays.asList(1, 1, 2, 3, 1, 4));
    data.add(Arrays.asList(0, 1, 1, 1, 2, 3));

    int[] sortedDocId = OnHeapStarTreeV2BuilderHelper.sortStarTreeData(
        2,6, Arrays.asList(0, 1, 2), data);
    int[] expected = new int[] {4, 2, 3, 5};
    for (int i: sortedDocId) {
      System.out.print(i);
    }
    assert1DListEqual(Arrays.asList(expected), Arrays.asList(sortedDocId));
  }

  @Test
  public void testCondenseData() {
   List<List<Object>> data = createData();
   List<List<Object>> filteredData = OnHeapStarTreeV2BuilderHelper.filterData(
       2, 8, 0,Arrays.asList(0, 1, 2), data);
   List<List<Object>> actual = OnHeapStarTreeV2BuilderHelper.condenseData(filteredData);
   List<List<Object>> expected = new ArrayList<>();
   expected.add(Arrays.asList(-1, -1, -1, -1));
   expected.add(Arrays.asList(1, 1, 2, 2));
   expected.add(Arrays.asList(0, 1, 1, 2));
   assert2DListEqual(expected, actual);
  }

  @Test
  public void testFilterData() {
    List<List<Object>> data = createData();
    List<List<Object>> actual = OnHeapStarTreeV2BuilderHelper.filterData(
        2, 8, 0, Arrays.asList(0, 1, 2), data);
    List<List<Object>> expected = new ArrayList<>();
    expected.add(Arrays.asList(-1, -1, -1, -1, -1, -1));
    expected.add(Arrays.asList(1, 1, 1, 2, 2, 2));
    expected.add(Arrays.asList(0, 0, 1, 1, 1, 2));
    assert2DListEqual(expected, actual);
  }

  private void print2DList(List<List<Object>> data) {
    for (List<Object> list: data) {
      for (Object obj: list) {
        System.out.print(obj);
        System.out.print(" ");
      }
      System.out.print("\n");
    }
  }

  private void assert1DListEqual(List<Object> expected, List<Object> actual) {
    Assert.assertEquals(actual.size(), expected.size());
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals(actual.get(i), expected.get(i));
    }
  }

  private void assert2DListEqual(List<List<Object>> expected, List<List<Object>> actual) {
    if (expected.size() == 0) {
      Assert.assertEquals(actual.size(), expected.size());
      return;
    }
    for (int i = 0; i < expected.size(); i++) {
      for (int j = 0; j < expected.get(0).size(); j++) {
        String message = "Test at " + Integer.toString(i) + "," + Integer.toString(j);
        Assert.assertEquals(actual.get(i).get(j), expected.get(i).get(j), message);
      }
    }
  }
}