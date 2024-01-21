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
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;


/**
 * Compares perf between Heap, Direct and Memory Mapped
 *
 *
 */
public class SpeedTest {

  private static final int SIZE = 10000000;
  private static final String FILE_NAME = "/tmp/big_data.dat";
  private static final double READ_PERCENTAGE = 0.9;
  private int[] _heapStorage;
  private ByteBuffer _directMemory;
  private MappedByteBuffer _mmappedByteBuffer;
  private int[] _readIndices;

  private void init()
      throws Exception {
    // write a temp file
    try (FileOutputStream fout = new FileOutputStream(FILE_NAME);
          DataOutputStream out = new DataOutputStream(fout)) {
      _heapStorage = new int[SIZE];
      _directMemory = ByteBuffer.allocateDirect(SIZE * 4);
      for (int i = 0; i < SIZE; i++) {
        int data = (int) (Math.random() * Integer.MAX_VALUE);
        out.writeInt(data);
        _heapStorage[i] = data;
        _directMemory.putInt(data);
      }
    }
    try (RandomAccessFile raf = new RandomAccessFile(FILE_NAME, "rw");
          FileChannel channel = raf.getChannel()) {
      _mmappedByteBuffer = channel.map(MapMode.READ_WRITE, 0L, raf.length());
      _mmappedByteBuffer.load();

      int toSelect = (int) (SIZE * READ_PERCENTAGE);
      _readIndices = new int[toSelect];
      int ind = 0;
      for (int i = 0; i < SIZE && toSelect > 0; i++) {
        double probOfSelection = toSelect * 1.0 / (SIZE - i);
        double random = Math.random();
        if (random < probOfSelection) {
          _readIndices[ind] = i;
          toSelect = toSelect - 1;
          ind = ind + 1;
        }
      }
    }
  }

  private long directMemory() {
    long start = System.currentTimeMillis();

    long sum = 0;
    for (int i = 0; i < _readIndices.length; i++) {
      sum += _directMemory.getInt(_readIndices[i] * 4);
    }
    long end = System.currentTimeMillis();
    System.out.println("direct memory time:" + (end - start));

    return sum;
  }

  private long heap() {
    long start = System.currentTimeMillis();
    long sum = 0;
    for (int i = 0; i < _readIndices.length; i++) {
      sum += _heapStorage[_readIndices[i]];
    }
    long end = System.currentTimeMillis();
    System.out.println("heap time:" + (end - start));
    return sum;
  }

  private long memoryMapped() {
    long start = System.currentTimeMillis();
    long sum = 0;
    for (int i = 0; i < _readIndices.length; i++) {
      sum += _mmappedByteBuffer.getInt(_readIndices[i] * 4);
    }
    long end = System.currentTimeMillis();
    System.out.println("memory mapped time:" + (end - start));
    return sum;
  }

  public static void main(String[] args)
      throws Exception {
    final SpeedTest speedTest = new SpeedTest();
    speedTest.init();

    System.out.println(speedTest.heap());
    System.out.println(speedTest.heap());
    System.out.println(speedTest.directMemory());
    System.out.println(speedTest.directMemory());
    System.out.println(speedTest.memoryMapped());
    System.out.println(speedTest.memoryMapped());
  }
}
