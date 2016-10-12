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
package com.linkedin.pinot.core.startree;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestStarTreeDataTable {
  @Test
  public void testSort() throws Exception {
    Random r = new Random();
    int ROWS = 10;
    final int COLS = 10;
    int data[][] = new int[ROWS][];
    File tempFile = new File("/tmp/test." + System.currentTimeMillis());
    FileOutputStream out = new FileOutputStream(tempFile);
    DataOutputStream dos = new DataOutputStream(out);
    for (int row = 0; row < ROWS; row++) {
      data[row] = new int[COLS];
      for (int col = 0; col < COLS; col++) {
        data[row][col] = r.nextInt(5);
        dos.writeInt(data[row][col]);
      }
    }
    out.close();
    dos.flush();
    dos.close();
//    print(data);
//    System.out.println("BEFORE SORTING: " + tempFile.length());
    int[][] input = read(tempFile, ROWS, COLS);

    int[] sortOrder = new int[COLS];
    for (int i = 0; i < COLS; i++) {
      sortOrder[i] = i;
    }

    StarTreeDataTable sorter = new StarTreeDataTable(tempFile, COLS * (Integer.SIZE / 8), 0, sortOrder);
    sorter.sort(0, ROWS);
//    System.out.println("AFTER SORTING");
    int[][] output = read(tempFile, ROWS, COLS);
//    print(output);
    Arrays.sort(input, new Comparator<int[]>() {
      @Override
      public int compare(int[] o1, int[] o2) {
        for (int i = 0; i < COLS; i++) {
          if (o1[i] != o2[i]) {
            return o1[i] - o2[i];
          }
        }
        return 0;
      }
    });

    Assert.assertTrue(compare(input, output, ROWS));
//    if (compare(input, output, ROWS)) {
//      System.out.println("PASSED");
//    } else {
//      System.out.println("FAILED");
//    }
//    System.out.println(sorter.groupByIntColumnCount(0, ROWS, 0));

  }

  private static boolean compare(int[][] expected, int[][] actual, int numRows) {
    for (int i = 0; i < numRows; i++) {
      if (!Arrays.equals(expected[i], actual[i])) {
        System.err.println("MisMatch");
        System.err.println("\t\texpected:" + Arrays.toString(expected[i]));
        System.err.println("\t\tactual  :" + Arrays.toString(actual[i]));
        return false;
      }
    }
    return true;
  }

  public static void print(int[][] output) {
//    for (int i = 0; i < output.length; i++) {
//      System.out.println(Arrays.toString(output[i]));
//    }
  }

  public static int[][] read(File tempFile, int numRows, int numCols) throws IOException {
    int[][] data = new int[numRows][];
    DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(tempFile)));
    for (int row = 0; row < numRows; row++) {
      data[row] = new int[numCols];
      for (int col = 0; col < numCols; col++) {
        data[row][col] = dis.readInt();
      }
    }
    dis.close();
    return data;
  }
}
