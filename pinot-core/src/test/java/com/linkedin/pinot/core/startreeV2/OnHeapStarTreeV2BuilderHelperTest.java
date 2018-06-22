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


  public List<Record> createData() {
    List<Record> data = new ArrayList<>();

    Record r1 = new Record();
    r1.setDimensionValues(new int[]{ 4, 1, 0});
    r1.setMetricValues(Arrays.asList(1, 2, 3));
    data.add(r1);

    Record r2 = new Record();
    r2.setDimensionValues(new int[]{ 5, 1, 1});
    r2.setMetricValues(Arrays.asList(3, 2, 1));
    data.add(r2);

    Record r3 = new Record();
    r3.setDimensionValues(new int[]{ 1, 2, 1});
    r3.setMetricValues(Arrays.asList(1, 2, 3));
    data.add(r3);

    Record r4 = new Record();
    r4.setDimensionValues(new int[]{ 1, 3, 1});
    r4.setMetricValues(Arrays.asList(3, 2, 1));
    data.add(r4);

    Record r5 = new Record();
    r5.setDimensionValues(new int[]{ 1, 1, 2});
    r5.setMetricValues(Arrays.asList(1, 2, 3));
    data.add(r5);

    Record r6 = new Record();
    r6.setDimensionValues(new int[]{ 4, 4, 3});
    r6.setMetricValues(Arrays.asList(3, 2, 1));
    data.add(r6);

    return data;
  }


  public List<Record> expectedSortedData() {
    List<Record> data = new ArrayList<>();

    Record r1 = new Record();
    r1.setDimensionValues(new int[]{ 1, 1, 2});
    r1.setMetricValues(Arrays.asList(1, 2, 3));
    data.add(r1);

    Record r2 = new Record();
    r2.setDimensionValues(new int[]{ 1, 2, 1});
    r2.setMetricValues(Arrays.asList(1, 2, 3));
    data.add(r2);

    Record r3 = new Record();
    r3.setDimensionValues(new int[]{ 1, 3, 1});
    r3.setMetricValues(Arrays.asList(3, 2, 1));
    data.add(r3);

    Record r5 = new Record();
    r5.setDimensionValues(new int[]{ 4, 4, 3});
    r5.setMetricValues(Arrays.asList(3, 2, 1));
    data.add(r5);

    return data;
  }

  @Test
  public void testSortStarTreeData() {
    List<Record> data = createData();
    List<Record> actualData = OnHeapStarTreeV2BuilderHelper.sortStarTreeData(
        2,6, Arrays.asList(0, 1, 2), data);

    List<Record> expected = expectedSortedData();
    assertRecordsList(expected, actualData);
  }

//  @Test
//  public void testCondenseData() {
//    List<Record> data = createData();
//    List<List<Object>> filteredData = OnHeapStarTreeV2BuilderHelper.filterData(
//       2, 8, 0,Arrays.asList(0, 1, 2), data);
//   List<List<Object>> actual = OnHeapStarTreeV2BuilderHelper.condenseData(filteredData);
//   List<List<Object>> expected = new ArrayList<>();
//   expected.add(Arrays.asList(-1, -1, -1, -1));
//   expected.add(Arrays.asList(1, 1, 2, 2));
//   expected.add(Arrays.asList(0, 1, 1, 2));
//   assert2DListEqual(expected, actual);
//  }
//
//  @Test
//  public void testFilterData() {
//    List<List<Object>> data = createData();
//    List<List<Object>> actual = OnHeapStarTreeV2BuilderHelper.filterData(
//        2, 8, 0, Arrays.asList(0, 1, 2), data);
//    List<List<Object>> expected = new ArrayList<>();
//    expected.add(Arrays.asList(-1, -1, -1, -1, -1, -1));
//    expected.add(Arrays.asList(1, 1, 1, 2, 2, 2));
//    expected.add(Arrays.asList(0, 0, 1, 1, 1, 2));
//  }

  private void assertRecordsList(List<Record> expected, List<Record> actual) {
    if (expected.size() == 0) {
      Assert.assertEquals(actual.size(), expected.size());
      return;
    }

    for (int i = 0; i < expected.size(); i++) {
      Record expR = expected.get(i);
      Record actR = actual.get(i);
      assertRecord(expR, actR);
    }
  }

  private void assertRecord( Record a, Record b) {

    int aD [] = a.getDimensionValues();
    int bD [] = b.getDimensionValues();

    for (int i = 0; i < aD.length; i++) {
      Assert.assertEquals(aD[i], bD[i]);
    }

    List<Object> aM = a.getMetricValues();
    List<Object> bM = b.getMetricValues();

    for (int i = 0; i < aM.size(); i++) {
      Assert.assertEquals(aM.get(i), bM.get(i));
    }
  }

  private void printRecordsList( List<Record> records) {
    for (Record record: records) {
      int aD[] = record.getDimensionValues();
      for (int i = 0; i < aD.length; i++) {
       System.out.print(aD[i]);
       System.out.print(" ");
      }

      List<Object> aM = record.getMetricValues();
      for (int i = 0; i < aM.size(); i++) {
        System.out.print(aM.get(i));
        System.out.print(" ");
      }

      System.out.print("\n");
    }
  }
}