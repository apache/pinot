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
package org.apache.pinot.tools;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;


/**
 * Compares perf between Heap, Direct and Memory Mapped
 *
 *
 */
public class SpeedTest {
  static int SIZE = 10000000;
  static String FILE_NAME = "/tmp/big_data.dat";
  static double readPercentage = 0.9;
  static int[] heapStorage;
  static ByteBuffer directMemory;
  static MappedByteBuffer mmappedByteBuffer;
  static int[] readIndices;

  static void init()
      throws Exception {
    // write a temp file
    FileOutputStream fout = new FileOutputStream(FILE_NAME);
    DataOutputStream out = new DataOutputStream(fout);
    heapStorage = new int[SIZE];
    directMemory = ByteBuffer.allocateDirect(SIZE * 4);
    for (int i = 0; i < SIZE; i++) {
      int data = (int) (Math.random() * Integer.MAX_VALUE);
      out.writeInt(data);
      heapStorage[i] = data;
      directMemory.putInt(data);
    }
    out.close();
    RandomAccessFile raf = new RandomAccessFile(FILE_NAME, "rw");
    mmappedByteBuffer = raf.getChannel().map(MapMode.READ_WRITE, 0L, raf.length());
    mmappedByteBuffer.load();

    int toSelect = (int) (SIZE * readPercentage);
    readIndices = new int[toSelect];
    int ind = 0;
    for (int i = 0; i < SIZE && toSelect > 0; i++) {
      double probOfSelection = toSelect * 1.0 / (SIZE - i);
      double random = Math.random();
      if (random < probOfSelection) {
        readIndices[ind] = i;
        toSelect = toSelect - 1;
        ind = ind + 1;
      }
    }
    //System.out.println(Arrays.toString(heapStorage));
    //System.out.println(Arrays.toString(readIndices));
  }

  static long directMemory() {
    long start = System.currentTimeMillis();

    long sum = 0;
    for (int i = 0; i < readIndices.length; i++) {
      sum += directMemory.getInt(readIndices[i] * 4);
    }
    long end = System.currentTimeMillis();
    System.out.println("direct memory time:" + (end - start));

    return sum;
  }

  static long heap() {
    long start = System.currentTimeMillis();
    long sum = 0;
    for (int i = 0; i < readIndices.length; i++) {
      sum += heapStorage[readIndices[i]];
    }
    long end = System.currentTimeMillis();
    System.out.println("heap time:" + (end - start));
    return sum;
  }

  static long memoryMapped() {
    long start = System.currentTimeMillis();
    long sum = 0;
    for (int i = 0; i < readIndices.length; i++) {
      sum += mmappedByteBuffer.getInt(readIndices[i] * 4);
    }
    long end = System.currentTimeMillis();
    System.out.println("memory mapped time:" + (end - start));
    return sum;
  }

  public static void main(String[] args)
      throws Exception {
    init();

    System.out.println(heap());
    System.out.println(heap());
    System.out.println(directMemory());
    System.out.println(directMemory());
    System.out.println(memoryMapped());
    System.out.println(memoryMapped());
  }
}
