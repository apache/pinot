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
package org.apache.pinot.perf;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.segment.local.io.writer.impl.FixedBitMVForwardIndexWriter;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class ForwardIndexWriterBenchmark {
  public static void convertRawToForwardIndex(File rawFile)
      throws Exception {
    List<String> lines = IOUtils.readLines(new FileReader(rawFile));
    int totalDocs = lines.size();
    int max = Integer.MIN_VALUE;
    int maxNumberOfMultiValues = Integer.MIN_VALUE;
    int totalNumValues = 0;
    int data[][] = new int[totalDocs][];
    for (int i = 0; i < lines.size(); i++) {
      String line = lines.get(i);
      String[] split = line.split(",");
      totalNumValues = totalNumValues + split.length;
      if (split.length > maxNumberOfMultiValues) {
        maxNumberOfMultiValues = split.length;
      }
      data[i] = new int[split.length];
      for (int j = 0; j < split.length; j++) {
        String token = split[j];
        int val = Integer.parseInt(token);
        data[i][j] = val;
        if (val > max) {
          max = val;
        }
      }
    }
    int maxBitsNeeded = (int) Math.ceil(Math.log(max) / Math.log(2));
    int size = 2048;
    int[] offsets = new int[size];
    int bitMapSize = 0;
    File outputFile = new File("output.mv.fwd");

    FixedBitMVForwardIndexWriter fixedBitSkipListSCMVWriter =
        new FixedBitMVForwardIndexWriter(outputFile, totalDocs, totalNumValues, maxBitsNeeded);

    for (int i = 0; i < totalDocs; i++) {
      fixedBitSkipListSCMVWriter.putDictIds(data[i]);
      if (i % size == size - 1) {
        MutableRoaringBitmap rr1 = MutableRoaringBitmap.bitmapOf(offsets);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        rr1.serialize(dos);
        dos.close();
        // System.out.println("Chunk " + i / size + " bitmap size:" + bos.size());
        bitMapSize += bos.size();
      } else if (i == totalDocs - 1) {
        MutableRoaringBitmap rr1 = MutableRoaringBitmap.bitmapOf(Arrays.copyOf(offsets, i % size));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        rr1.serialize(dos);
        dos.close();
        // System.out.println("Chunk " + i / size + " bitmap size:" + bos.size());
        bitMapSize += bos.size();
      }
    }
    fixedBitSkipListSCMVWriter.close();
    System.out.println("Output file size:" + outputFile.length());
    System.out.println("totalNumberOfDoc\t\t\t:" + totalDocs);
    System.out.println("totalNumberOfValues\t\t\t:" + totalNumValues);
    System.out.println("chunk size\t\t\t\t:" + size);
    System.out.println("Num chunks\t\t\t\t:" + totalDocs / size);
    int numChunks = totalDocs / size + 1;
    int totalBits = (totalNumValues * maxBitsNeeded);
    int dataSizeinBytes = (totalBits + 7) / 8;

    System.out.println("Raw data size with fixed bit encoding\t:" + dataSizeinBytes);
    System.out.println("\nPer encoding size");
    System.out.println();
    System.out.println("size (offset + length)\t\t\t:" + ((totalDocs * (4 + 4)) + dataSizeinBytes));
    System.out.println();
    System.out.println("size (offset only)\t\t\t:" + ((totalDocs * (4)) + dataSizeinBytes));
    System.out.println();
    System.out.println("bitMapSize\t\t\t\t:" + bitMapSize);
    System.out.println("size (with bitmap)\t\t\t:" + (bitMapSize + (numChunks * 4) + dataSizeinBytes));

    System.out.println();
    System.out.println("Custom Bitset\t\t\t\t:" + (totalNumValues + 7) / 8);
    System.out
        .println("size (with custom bitset)\t\t\t:" + (((totalNumValues + 7) / 8) + (numChunks * 4) + dataSizeinBytes));
  }

  public static void main(String[] args)
      throws Exception {
    convertRawToForwardIndex(new File("/tmp/output.mv.raw"));
  }
}
