/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.index.reader.impl.FixedByteWidthRowColDataFileReader;


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
    System.out.println("file size: " + raf.getChannel().size());
    raf.close();
    FixedByteWidthRowColDataFileReader heapReader = new FixedByteWidthRowColDataFileReader(f, data.length, 1, new int[] { 4 }, false);
    heapReader.open();
    for (int i = 0; i < data.length; i++) {
      Assert.assertEquals(heapReader.getInt(i, 0), data[i]);
    }
    // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 0);
    heapReader.close();
    // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 0);

    FixedByteWidthRowColDataFileReader mmapReader = new FixedByteWidthRowColDataFileReader(f, data.length, 1, new int[] { 4 }, true);
    mmapReader.open();
    for (int i = 0; i < data.length; i++) {
      Assert.assertEquals(mmapReader.getInt(i, 0), data[i]);
    }
    // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 2);
    mmapReader.close();
    // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 0);

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
    System.out.println("file size: " + raf.getChannel().size());
    raf.close();
    FixedByteWidthRowColDataFileReader heapReader = new FixedByteWidthRowColDataFileReader(f, numRows, numCols, new int[] { 4, 4 }, false);
    heapReader.open();
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numCols; j++) {
        Assert.assertEquals(heapReader.getInt(i, j), colData[i][j]);
      }
    }
    // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 0);
    heapReader.close();
    // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 0);

    FixedByteWidthRowColDataFileReader mmapReader = new FixedByteWidthRowColDataFileReader(f, numRows, numCols, new int[] { 4, 4 }, true);
    mmapReader.open();
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numCols; j++) {
        Assert.assertEquals(mmapReader.getInt(i, j), colData[i][j]);
      }
    }
    // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 2);
    mmapReader.close();
    // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 0);

    f.delete();
  }
}
