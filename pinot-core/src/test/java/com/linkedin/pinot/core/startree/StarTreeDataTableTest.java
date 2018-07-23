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
package com.linkedin.pinot.core.startree;

import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;


public class StarTreeDataTableTest {

  @Test
  public void testSort() throws Exception {
    Random random = new Random();
    int numRows = 10;
    final int numDimensions = 10;
    int data[][] = new int[numRows][numDimensions];
    int dimensionSize = numDimensions * Integer.BYTES;
    PinotDataBuffer dataBuffer =
        PinotDataBuffer.allocateDirect(numRows * dimensionSize, PinotDataBuffer.NATIVE_ORDER, null);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numDimensions; j++) {
        int dimensionValue = random.nextInt(10);
        data[i][j] = dimensionValue;
        dataBuffer.putInt((i * numDimensions + j) * Integer.BYTES, dimensionValue);
      }
    }

    List<Integer> sortOrderList = new ArrayList<>(numDimensions);
    for (int i = 0; i < numDimensions; i++) {
      sortOrderList.add(i);
    }
    Collections.shuffle(sortOrderList, random);
    final int[] sortOrder = new int[numDimensions];
    for (int i = 0; i < numDimensions; i++) {
      sortOrder[i] = sortOrderList.get(i);
    }

    Arrays.sort(data, (o1, o2) -> {
      for (int index : sortOrder) {
        if (o1[index] != o2[index]) {
          return o1[index] - o2[index];
        }
      }
      return 0;
    });

    try (StarTreeDataTable dataTable = new StarTreeDataTable(dataBuffer, dimensionSize, 0, 0)) {
      dataTable.sort(0, numRows, sortOrder);
      for (int i = 0; i < numRows; i++) {
        for (int j = 0; j < numDimensions; j++) {
          Assert.assertEquals(dataBuffer.getInt((i * numDimensions + j) * Integer.BYTES), data[i][j]);
        }
      }
    }
  }
}
