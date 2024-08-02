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
package org.apache.pinot.segment.local.segment.index.writer;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Random;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.local.io.reader.impl.FixedByteSingleValueMultiColReader;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteSingleValueMultiColWriter;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class FixedByteWidthRowColForwardIndexWriterTest {
  @Test
  public void testSingleColInt()
      throws Exception {

    File file = new File("test_single_col_writer.dat");
    file.delete();
    int rows = 100;
    int cols = 1;
    int[] columnSizes = new int[]{4};
    FixedByteSingleValueMultiColWriter writer = new FixedByteSingleValueMultiColWriter(file, rows, cols, columnSizes);
    int[] data = new int[rows];
    Random r = new Random();
    for (int i = 0; i < rows; i++) {
      data[i] = r.nextInt();
      writer.setInt(i, 0, data[i]);
    }
    writer.close();

    File rfile = new File("test_single_col_writer.dat");
    try (FixedByteSingleValueMultiColReader reader = new FixedByteSingleValueMultiColReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(rfile), rows, columnSizes)) {
      for (int i = 0; i < rows; i++) {
        Assert.assertEquals(reader.getInt(i, 0), data[i]);
      }
    }
    rfile.delete();
  }

  @Test
  public void testSingleColFloat()
      throws Exception {

    File wfile = new File("test_single_col_writer.dat");
    wfile.delete();
    final int rows = 100;
    final int cols = 1;
    final int[] columnSizes = new int[]{4};
    FixedByteSingleValueMultiColWriter writer = new FixedByteSingleValueMultiColWriter(wfile, rows, cols, columnSizes);
    final float[] data = new float[rows];
    Random r = new Random();
    for (int i = 0; i < rows; i++) {
      data[i] = r.nextFloat();
      writer.setFloat(i, 0, data[i]);
    }
    writer.close();

    File rfile = new File("test_single_col_writer.dat");
    try (FixedByteSingleValueMultiColReader reader = new FixedByteSingleValueMultiColReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(rfile), rows, columnSizes)) {
      for (int i = 0; i < rows; i++) {
        Assert.assertEquals(reader.getFloat(i, 0), data[i]);
      }
    }
    rfile.delete();
  }

  @Test
  public void testSingleColDouble()
      throws Exception {

    File wfile = new File("test_single_col_writer.dat");
    wfile.delete();
    final int rows = 100;
    final int cols = 1;
    final int[] columnSizes = new int[]{8};
    FixedByteSingleValueMultiColWriter writer = new FixedByteSingleValueMultiColWriter(wfile, rows, cols, columnSizes);
    final double[] data = new double[rows];
    Random r = new Random();
    for (int i = 0; i < rows; i++) {
      data[i] = r.nextDouble();
      writer.setDouble(i, 0, data[i]);
    }
    writer.close();

    File rfile = new File("test_single_col_writer.dat");
    try (FixedByteSingleValueMultiColReader reader = new FixedByteSingleValueMultiColReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(rfile), rows, columnSizes)) {
      for (int i = 0; i < rows; i++) {
        Assert.assertEquals(reader.getDouble(i, 0), data[i]);
      }
    }
    rfile.delete();
  }

  @Test
  public void testSingleColLong()
      throws Exception {

    File wfile = new File("test_single_col_writer.dat");
    wfile.delete();
    final int rows = 100;
    final int cols = 1;
    final int[] columnSizes = new int[]{8};
    FixedByteSingleValueMultiColWriter writer = new FixedByteSingleValueMultiColWriter(wfile, rows, cols, columnSizes);
    final long[] data = new long[rows];
    Random r = new Random();
    for (int i = 0; i < rows; i++) {
      data[i] = r.nextLong();
      writer.setLong(i, 0, data[i]);
    }
    writer.close();

    File rfile = new File("test_single_col_writer.dat");
    try (FixedByteSingleValueMultiColReader reader = new FixedByteSingleValueMultiColReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(rfile), rows, columnSizes)) {
      for (int i = 0; i < rows; i++) {
        Assert.assertEquals(reader.getLong(i, 0), data[i]);
      }
    }
    rfile.delete();
  }

  @Test
  public void testMultiCol()
      throws Exception {

    File file = new File("test_single_col_writer.dat");
    file.delete();
    int rows = 100;
    int cols = 2;
    int[] columnSizes = new int[]{4, 4};
    FixedByteSingleValueMultiColWriter writer = new FixedByteSingleValueMultiColWriter(file, rows, cols, columnSizes);
    int[][] data = new int[rows][cols];
    Random r = new Random();
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        data[i][j] = r.nextInt();
        writer.setInt(i, j, data[i][j]);
      }
    }
    writer.close();
    DataInputStream dis = new DataInputStream(new FileInputStream(file));
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        Assert.assertEquals(dis.readInt(), data[i][j]);
      }
    }
    dis.close();
    file.delete();
  }

  @Test
  public void testSpecialCharsForStringReaderWriter()
      throws Exception {
    final byte[] bytes1 = new byte[]{-17, -65, -67, -17, -65, -67, 32, 69, 120, 101, 99, 117, 116, 105, 118, 101};
    final byte[] bytes2 = new byte[]{
        -17, -65, -68, 32, 99, 97, 108, 103, 97, 114, 121, 32, 106, 117, 110, 107, 32, 114, 101, 109, 111, 118, 97, 108
    };
    File file = new File("test_single_col_writer.dat");
    file.delete();
    int rows = 100;
    int cols = 1;
    String testString1 = new String(bytes1);
    String testString2 = new String(bytes2);
    int stringColumnMaxLength = Math.max(testString1.getBytes().length, testString2.getBytes().length);
    int[] columnSizes = new int[]{stringColumnMaxLength};
    FixedByteSingleValueMultiColWriter writer = new FixedByteSingleValueMultiColWriter(file, rows, cols, columnSizes);
    String[] data = new String[rows];
    for (int i = 0; i < rows; i++) {
      String toPut = (i % 2 == 0) ? testString1 : testString2;
      final int padding = stringColumnMaxLength - toPut.getBytes().length;

      final StringBuilder bld = new StringBuilder();
      bld.append(toPut);
      for (int j = 0; j < padding; j++) {
        bld.append(V1Constants.Str.DEFAULT_STRING_PAD_CHAR);
      }
      data[i] = bld.toString();
      writer.setString(i, 0, data[i]);
    }
    writer.close();
    try (FixedByteSingleValueMultiColReader dataFileReader = new FixedByteSingleValueMultiColReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(file), rows, new int[]{stringColumnMaxLength})) {
      for (int i = 0; i < rows; i++) {
        String stringInFile = dataFileReader.getString(i, 0);
        Assert.assertEquals(stringInFile, data[i]);
        Assert.assertEquals(StringUtils.remove(stringInFile, String.valueOf(V1Constants.Str.DEFAULT_STRING_PAD_CHAR)),
            StringUtils.remove(data[i], String.valueOf(V1Constants.Str.DEFAULT_STRING_PAD_CHAR)));
      }
    }
    file.delete();
  }

  @Test
  public void testSpecialPaddingCharsForStringReaderWriter()
      throws Exception {
    for (int iter = 0; iter < 2; iter++) {
      char paddingChar = (iter == 0) ? '%' : '\0';
      final byte[] bytes1 = new byte[]{-17, -65, -67, -17, -65, -67, 32, 69, 120, 101, 99, 117, 116, 105, 118, 101};
      final byte[] bytes2 = new byte[]{
          -17, -65, -68, 32, 99, 97, 108, 103, 97, 114, 121, 32, 106, 117, 110, 107, 32, 114, 101, 109, 111, 118, 97,
          108
      };
      File file = new File("test_single_col_writer.dat");
      file.delete();
      int rows = 100;
      int cols = 1;
      String testString1 = new String(bytes1);
      String testString2 = new String(bytes2);
      int stringColumnMaxLength = Math.max(testString1.getBytes().length, testString2.getBytes().length);
      int[] columnSizes = new int[]{stringColumnMaxLength};
      FixedByteSingleValueMultiColWriter writer = new FixedByteSingleValueMultiColWriter(file, rows, cols, columnSizes);
      String[] data = new String[rows];
      for (int i = 0; i < rows; i++) {
        String toPut = (i % 2 == 0) ? testString1 : testString2;
        final int padding = stringColumnMaxLength - toPut.getBytes().length;

        final StringBuilder bld = new StringBuilder();
        bld.append(toPut);
        for (int j = 0; j < padding; j++) {
          bld.append(paddingChar);
        }
        data[i] = bld.toString();
        writer.setString(i, 0, data[i]);
      }
      writer.close();
      try (FixedByteSingleValueMultiColReader dataFileReader = new FixedByteSingleValueMultiColReader(
          PinotDataBuffer.mapReadOnlyBigEndianFile(file), rows, new int[]{stringColumnMaxLength})) {
        for (int i = 0; i < rows; i++) {
          String stringInFile = dataFileReader.getString(i, 0);
          Assert.assertEquals(stringInFile, data[i]);
          Assert.assertEquals(StringUtils.remove(stringInFile, String.valueOf(paddingChar)),
              StringUtils.remove(data[i], String.valueOf(paddingChar)));
        }
      }
      file.delete();
    }
  }
}
