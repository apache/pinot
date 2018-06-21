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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ExperimentTest {

  public Experiment createData() {
    Experiment exp = new Experiment();
    exp.d1.addAll(Arrays.asList(4, 5, 1, 1, 1, 4));
    exp.d2.addAll(Arrays.asList(1, 1, 2, 3, 1, 4));
    exp.d3.addAll(Arrays.asList(0, 1, 1, 1, 2, 3));

    exp.data.addAll(Arrays.asList(exp.d1, exp.d2, exp.d3));
    exp.order.addAll(Arrays.asList(0, 1, 2));
    return exp;
  }

  @Test
  public void testSort() {
    Experiment exp = createData();
    int[] sortedDocId = exp.sort();
    int[] expected = new int[] {4, 2, 3, 0, 5, 1};
    for (int i=0; i<expected.length; i++) {
      Assert.assertEquals(expected[i], sortedDocId[i]);
    }

    List<List<Object>> newData = new ArrayList<>();
    for (List<Object> col: exp.data) {
      List<Object> newCol = new ArrayList<>();
      for (int id: sortedDocId) {
        newCol.add(col.get(id));
      }
      newData.add(newCol);
    }
  }
}