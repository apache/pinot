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
package com.linkedin.pinot.index.reader;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;


@Test
public class FixedByteWidthRowColDataFileReaderTest {

  @Test
  void testSingleCol() throws Exception {
    String fileName = "test_single_col.dat";
    File f = new File(fileName);

    f.delete();

    DataOutputStream dos = new DataOutputStream(new FileOutputStream(f));
    int[] data = new int[100];
    Random r = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = r.nextInt();
      dos.writeInt(data[i]);
    }
    dos.flush();
    dos.close();
    RandomAccessFile raf = new RandomAccessFile(f, "rw");
//    System.out.println("file size: " + raf.getChannel().size());
    raf.close();
    
    PinotDataBuffer heapBuffer = PinotDataBuffer.fromFile(f, ReadMode.heap, FileChannel.MapMode.READ_ONLY, "testing");
    FixedByteSingleValueMultiColReader heapReader =
        new FixedByteSingleValueMultiColReader(heapBuffer, data.length, new int[] { 4 });
    heapReader.open();
    for (int i = 0; i < data.length; i++) {
      Assert.assertEquals(heapReader.getInt(i, 0), data[i]);
    }
    heapBuffer.close();
    heapReader.close();

    // Not strictly required. Let the tests pass first...then we can remove
    // TODO: remove me
    PinotDataBuffer mmapBuffer = PinotDataBuffer.fromFile(f, ReadMode.mmap, FileChannel.MapMode.READ_ONLY, "mmap_testing");
    FixedByteSingleValueMultiColReader mmapReader =
        new FixedByteSingleValueMultiColReader(mmapBuffer, data.length, new int[] { 4 });
    mmapReader.open();
    for (int i = 0; i < data.length; i++) {
      Assert.assertEquals(mmapReader.getInt(i, 0), data[i]);
    }
    mmapBuffer.close();
    mmapReader.close();

    f.delete();
  }

  @Test
  void testMultiCol() throws Exception {
    String fileName = "test_single_col.dat";
    File f = new File(fileName);
    f.delete();
    DataOutputStream dos = new DataOutputStream(new FileOutputStream(f));

    int numRows = 100;
    int numCols = 2;
    int[][] colData = new int[numRows][numCols];
    Random r = new Random();

    for (int i = 0; i < numRows; i++) {
      colData[i] = new int[numCols];
      for (int j = 0; j < numCols; j++) {
        colData[i][j] = r.nextInt();
        dos.writeInt(colData[i][j]);
      }
    }
    dos.flush();
    dos.close();
    RandomAccessFile raf = new RandomAccessFile(f, "rw");
//    System.out.println("file size: " + raf.getChannel().size());
    raf.close();
    PinotDataBuffer heapBuffer = PinotDataBuffer.fromFile(f, ReadMode.heap, FileChannel.MapMode.READ_ONLY, "testing-heap");
    FixedByteSingleValueMultiColReader heapReader = new FixedByteSingleValueMultiColReader(heapBuffer, numRows, new int[] { 4, 4 });
    heapReader.open();
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numCols; j++) {
        Assert.assertEquals(heapReader.getInt(i, j), colData[i][j]);
      }
    }
    heapReader.close();
    PinotDataBuffer mmapBuffer = PinotDataBuffer.fromFile(f, ReadMode.mmap, FileChannel.MapMode.READ_ONLY, "mmap_testing");
    FixedByteSingleValueMultiColReader mmapReader = new FixedByteSingleValueMultiColReader(mmapBuffer, numRows, new int[] { 4, 4 });
    mmapReader.open();
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numCols; j++) {
        Assert.assertEquals(mmapReader.getInt(i, j), colData[i][j]);
      }
    }
    mmapReader.close();

    f.delete();
  }
}
